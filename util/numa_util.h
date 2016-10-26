#ifndef _NUMA_UTIL_H_
#define _NUMA_UTIL_H_

#include <numa.h>
#include <numaif.h>
#include <utmpx.h>
#include <time.h>
#include <sched.h>
//#include "port/atomic-template.h"

#define BILLION 1000000000L

inline void x_add64(uint64_t* addr, uint64_t val) {
	asm volatile(
		"lock;addq %1, %0"
		: "+m"(*addr)
		: "a"(val)
		: "cc");
}

static int Numa_get_node(void* ptr) {
	int numa_node = -1;
	get_mempolicy(&numa_node, NULL, 0, ptr, MPOL_F_NODE | MPOL_F_ADDR);
	return numa_node;
}

static int Numa_current_node() {
	int cpu = sched_getcpu();
	int node = numa_node_of_cpu(cpu);
	return node;
}

static void* Numa_alloc_onnode(size_t size, int node){
	void * ptr = numa_alloc_onnode(size, node);
	if(ptr == NULL){
		fprintf(stderr, "numa_alloc_error\n");
	}
	return ptr;
}
static void Numa_free(void * start, size_t size){
	numa_free(start, size);
}
static uint64_t get_nanoseconds(const timespec& begin, const timespec& end) {
	return (end.tv_sec - begin.tv_sec) * BILLION + (end.tv_nsec - begin.tv_nsec);
}

class time_measure{
	struct timespec begin, end;
	uint64_t* ts;
public:
	time_measure(uint64_t* _ts):ts(_ts){
		clock_gettime(CLOCK_MONOTONIC, &begin);
	}
	~time_measure(){
		clock_gettime(CLOCK_MONOTONIC, &end);
		uint64_t span = get_nanoseconds(begin, end);
		x_add64(ts,span);
	}
};

#endif
