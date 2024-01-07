#include "skynet.h"

#include "skynet_monitor.h"
#include "skynet_server.h"
#include "skynet.h"
#include "atomic.h"

#include <stdlib.h>
#include <string.h>

// work 线程的 monitor 结构
struct skynet_monitor {
	ATOM_INT version;  // 原子 version
	int check_version;  // 上次检查时的 version
	uint32_t source;  // 当前处理的消息的源 handle
	uint32_t destination;  // 当前处理的消息的目标 handle
};

struct skynet_monitor * 
skynet_monitor_new() {
	struct skynet_monitor * ret = skynet_malloc(sizeof(*ret));
	memset(ret, 0, sizeof(*ret));
	return ret;
}

void 
skynet_monitor_delete(struct skynet_monitor *sm) {
	skynet_free(sm);
}

// 　　skynet_monitor_check 中的操作也很简单，就是检查 skynet_monitor 中的 vesion 和 check_version 是否一致。如果 version 和 check_version 不相等，则把 check_version 设为 version，如果相等，则说明从上次检查到这次检查也就是 5s 之内，worker 线程都在处理同一条消息。这时候就认为这个 worker 线程可能已经陷入了死循环中。把目标服务的 endless 属性设为 true，然后输出一条错误日志警告开发者。
//　　通过阅读实现可以发现，monitor 线程只能起到非常微弱的辅助作用，那就是如果一条消息的执行时间超过 5s，就发出一次警告。 
void 
skynet_monitor_trigger(struct skynet_monitor *sm, uint32_t source, uint32_t destination) {
	sm->source = source;
	sm->destination = destination;
	ATOM_FINC(&sm->version);
}

void 
skynet_monitor_check(struct skynet_monitor *sm) {
	if (sm->version == sm->check_version) {
		if (sm->destination) {
			skynet_context_endless(sm->destination);
			skynet_error(NULL, "A message from [ :%08x ] to [ :%08x ] maybe in an endless loop (version = %d)", sm->source , sm->destination, sm->version);
		}
	} else {
		sm->check_version = sm->version;
	}
}
