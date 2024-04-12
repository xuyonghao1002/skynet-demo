#include "skynet.h"
#include "skynet_server.h"
#include "skynet_imp.h"
#include "skynet_mq.h"
#include "skynet_handle.h"
#include "skynet_module.h"
#include "skynet_timer.h"
#include "skynet_monitor.h"
#include "skynet_socket.h"
#include "skynet_daemon.h"
#include "skynet_harbor.h"

#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

// 监视
// 全局唯一的实例
struct monitor {
	int count;  // 工作线程的数量
	struct skynet_monitor ** m;  // 全部 worker 线程的 monitor 指针
	pthread_cond_t cond;  // 给 worker 线程挂起用的全局条件
	pthread_mutex_t mutex;  // 互斥锁
	int sleep;  // 休眠的工作线程数量
	int quit;  // 退出标记
};


// 工作线程的参数
struct worker_parm {
	struct monitor *m;  // 指向全局唯一监视结构体的指针
	int id;  // 工作线程的id
	int weight;  // 工作线程的权重
};

static volatile int SIG = 0;

static void
handle_hup(int signal) {
	if (signal == SIGHUP) {
		SIG = 1;
	}
}

#define CHECK_ABORT if (skynet_context_total()==0) break;

static void
create_thread(pthread_t *thread, void *(*start_routine) (void *), void *arg) {
	if (pthread_create(thread,NULL, start_routine, arg)) {
		fprintf(stderr, "Create thread failed");
		exit(1);
	}
}

static void
wakeup(struct monitor *m, int busy) {
	if (m->sleep >= m->count - busy) {
		// signal sleep worker, "spurious wakeup" is harmless
		pthread_cond_signal(&m->cond);
	}
}

static void *
thread_socket(void *p) {
	struct monitor * m = p;
	// 设置线程类型
	skynet_initthread(THREAD_SOCKET);
	for (;;) {
		int r = skynet_socket_poll();
		if (r==0)
			// 退出网络轮询
			break;
		if (r<0) {
			CHECK_ABORT
			// 一般是还有消息没处理完，直接继续循环
			continue;
		}
		// 要所有 work 线程全都睡眠，才会唤醒一个 work 线程
		wakeup(m,0);
	}
	return NULL;
}

static void
free_monitor(struct monitor *m) {
	int i;
	int n = m->count;
	for (i=0;i<n;i++) {
		skynet_monitor_delete(m->m[i]);
	}
	pthread_mutex_destroy(&m->mutex);
	pthread_cond_destroy(&m->cond);
	skynet_free(m->m);
	skynet_free(m);
}

// monitor 顾名思义就是监控，它监控的就是所有 worker 线程的工作状态，如果 worker 线程在处理一条消息的时候用时太久了，monitor 线程会打印出一条错误日志，告诉开发者一条从 A 服务到 B 服务的消息的处理逻辑中可能有死循环存在。
static void *
thread_monitor(void *p) {  // 检查过载服务
	struct monitor * m = p;
	int i;
	// 拿到 worker 线程的数量
	int n = m->count;
	// 设置线程属性
	skynet_initthread(THREAD_MONITOR);
	for (;;) {
		CHECK_ABORT
		// 遍历检查所有工作线程的 monitor
		for (i=0;i<n;i++) {
			skynet_monitor_check(m->m[i]);
		}
		// 睡眠 5s
        // 使用循环分开调用是为了更快的触发 abort
		for (i=0;i<5;i++) {
			CHECK_ABORT
			sleep(1);
		}
	}

	return NULL;
}

static void
signal_hup() {
	// make log file reopen

	struct skynet_message smsg;
	smsg.source = 0;
	smsg.session = 0;
	smsg.data = NULL;
	smsg.sz = (size_t)PTYPE_SYSTEM << MESSAGE_TYPE_SHIFT;
	uint32_t logger = skynet_handle_findname("logger");
	if (logger) {
		skynet_context_push(logger, &smsg);
	}
}

static void *
thread_timer(void *p) {
	struct monitor * m = p;
	skynet_initthread(THREAD_TIMER);
	for (;;) {
		skynet_updatetime();
		skynet_socket_updatetime();
		CHECK_ABORT
		wakeup(m,m->count-1);
		usleep(2500);
		if (SIG) {
			signal_hup();
			SIG = 0;
		}
	}
	// wakeup socket thread
	skynet_socket_exit();
	// wakeup all worker thread
	pthread_mutex_lock(&m->mutex);
	m->quit = 1;
	pthread_cond_broadcast(&m->cond);
	pthread_mutex_unlock(&m->mutex);
	return NULL;
}

static void *
thread_worker(void *p) {
	struct worker_parm *wp = p;
	int id = wp->id;
	int weight = wp->weight;
	struct monitor *m = wp->m;
	struct skynet_monitor *sm = m->m[id];
	skynet_initthread(THREAD_WORKER);
	struct message_queue * q = NULL;
	while (!m->quit) {
		q = skynet_context_message_dispatch(sm, q, weight);
		// 如果 q 是 NULL 的话，说明没有 pop 到要处理的消息队列，要把它投入到睡眠中去
		if (q == NULL) {
			// 获取全局 monitor 的锁
			if (pthread_mutex_lock(&m->mutex) == 0) {
				// 累加当前睡眠的线程数量
				++ m->sleep;
				// "spurious wakeup" is harmless,
				// because skynet_context_message_dispatch() can be call at any time.
				if (!m->quit)
					// 等待在条件上，释放 mutex, 等待 socket 线程或是 timer 线程唤醒
					pthread_cond_wait(&m->cond, &m->mutex);
				// 被唤醒后减少睡眠线程数
				-- m->sleep;
				// 释放锁
				if (pthread_mutex_unlock(&m->mutex)) {
					fprintf(stderr, "unlock mutex error");
					exit(1);
				}
			}
		}
	}
	return NULL;
}

static void
start(int thread) {
	pthread_t pid[thread+3];

	struct monitor *m = skynet_malloc(sizeof(*m));
	memset(m, 0, sizeof(*m));
	m->count = thread;
	m->sleep = 0;

	m->m = skynet_malloc(thread * sizeof(struct skynet_monitor *));
	int i;
	for (i=0;i<thread;i++) {
		m->m[i] = skynet_monitor_new();
	}
	if (pthread_mutex_init(&m->mutex, NULL)) {
		fprintf(stderr, "Init mutex error");
		exit(1);
	}
	if (pthread_cond_init(&m->cond, NULL)) {
		fprintf(stderr, "Init cond error");
		exit(1);
	}

	create_thread(&pid[0], thread_monitor, m);
	create_thread(&pid[1], thread_timer, m);
	create_thread(&pid[2], thread_socket, m);

	static int weight[] = { 
		-1, -1, -1, -1, 0, 0, 0, 0,
		1, 1, 1, 1, 1, 1, 1, 1, 
		2, 2, 2, 2, 2, 2, 2, 2, 
		3, 3, 3, 3, 3, 3, 3, 3, };
	struct worker_parm wp[thread];
	for (i=0;i<thread;i++) {
		wp[i].m = m;
		wp[i].id = i;
		if (i < sizeof(weight)/sizeof(weight[0])) {
			wp[i].weight= weight[i];
		} else {
			wp[i].weight = 0;
		}
		create_thread(&pid[i+3], thread_worker, &wp[i]);
	}

	for (i=0;i<thread+3;i++) {
		pthread_join(pid[i], NULL); 
	}

	free_monitor(m);
}

static void
bootstrap(struct skynet_context * logger, const char * cmdline) {
	int sz = strlen(cmdline);
	char name[sz+1];
	char args[sz+1];
	int arg_pos;
	sscanf(cmdline, "%s", name);  
	arg_pos = strlen(name);
	if (arg_pos < sz) {
		while(cmdline[arg_pos] == ' ') {
			arg_pos++;
		}
		strncpy(args, cmdline + arg_pos, sz);
	} else {
		args[0] = '\0';
	}
	struct skynet_context *ctx = skynet_context_new(name, args);
	if (ctx == NULL) {
		skynet_error(NULL, "Bootstrap error : %s\n", cmdline);
		skynet_context_dispatchall(logger);
		exit(1);
	}
}

void 
skynet_start(struct skynet_config * config) {
	// register SIGHUP for log file reopen
	struct sigaction sa;
	sa.sa_handler = &handle_hup;
	sa.sa_flags = SA_RESTART;
	sigfillset(&sa.sa_mask);
	sigaction(SIGHUP, &sa, NULL);

	if (config->daemon) {
		if (daemon_init(config->daemon)) {
			exit(1);
		}
	}

	// 初始化节点数量
	skynet_harbor_init(config->harbor);
	// 初始化 handle 存储器
	skynet_handle_init(config->harbor);
	// 初始化全局消息队列
	skynet_mq_init();
	// 初始化 C 模块管理器，设置查找路径
	skynet_module_init(config->module_path);
	// 初始化全局时间，时间轮算法在此实现
	skynet_timer_init();
	// 初始化 socket 管理器
	skynet_socket_init();
	// 标记是否开了性能测试
	skynet_profile_enable(config->profile);

	// 创建 logger C服务
	struct skynet_context *ctx = skynet_context_new(config->logservice, config->logger);
	if (ctx == NULL) {
		fprintf(stderr, "Can't launch %s service\n", config->logservice);
		exit(1);
	}
	// 注册 logger 服务的名字
	skynet_handle_namehandle(skynet_context_handle(ctx), "logger");

	// 启动，执行 config 文件中的 bootstrap 命令
	bootstrap(ctx, config->bootstrap);

	// 启动全部线程
	start(config->thread);

	// harbor_exit may call socket send, so it should exit before socket_free
	skynet_harbor_exit();
	skynet_socket_free();
	if (config->daemon) {
		daemon_exit(config->daemon);
	}
}
