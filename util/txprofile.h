// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_TXPROFILE_H_
#define STORAGE_LEVELDB_UTIL_TXPROFILE_H_
#include <immintrin.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>

#define XBEGIN_STARTED_INDEX 0
#define XABORT_EXPLICIT_INDEX 1
#define XABORT_RETRY_INDEX 2
#define XABORT_CONFLICT_INDEX 3
#define XABORT_CAPACITY_INDEX 4
#define XABORT_DEBUG_INDEX 5
#define XABORT_NESTED_INDEX 6

class RTMPara{
public:
	uint32_t abortCounts;
	uint32_t capacityCounts;
	uint32_t conflictCounts;
	uint32_t succCounts;
	uint32_t zeroCounts;
	uint64_t winner_interval;
	uint64_t winners;
	uint64_t total_interval;
	RTMPara():abortCounts(0),
		capacityCounts(0),
		conflictCounts(0),
		succCounts(0),
		zeroCounts(0),
		winners(0),
		winner_interval(0),
		total_interval(0){}
};

class RTMProfile {

public:
	pthread_spinlock_t slock;
	uint32_t status[7];
	uint32_t abortCounts;
	uint32_t capacityCounts;
	uint32_t conflictCounts;
	uint32_t totalCounts;
	uint32_t zeroCounts;
	uint64_t winner_interval;
	uint64_t winners;
	uint64_t total_interval;
	uint32_t fallbacks;

	static __inline__ void atomic_inc32(uint32_t *p) {
		__asm__ __volatile__("lock; incl %0"
							 : "+m"(*p)
							 :
							 : "cc");
	}

	static __inline__ void atomic_add32(uint32_t *p, int num) {
		__asm__ __volatile__("lock; addl %1, %0"
							 : "+m"(*p)
							 : "q"(num)
							 : "cc");
	}

	RTMProfile() {
		pthread_spin_init(&slock, PTHREAD_PROCESS_PRIVATE);
		abortCounts	= 0;
		totalCounts = 0;
		capacityCounts = 0;
		conflictCounts = 0;
		zeroCounts = 0;
		for(int i = 0; i < 7; i++){
			status[i] = 0;
		}
		winners = 0;
		winner_interval = 0;
		fallbacks = 0;
	}

	void recordRetryNum(int num) {
		atomic_add32(&abortCounts, num);
		atomic_inc32(&totalCounts);
	}

	void transfer_para(RTMPara& para){
		para.abortCounts += abortCounts;
		para.capacityCounts += capacityCounts;
		para.conflictCounts += conflictCounts;
		para.succCounts += totalCounts;
		para.zeroCounts += zeroCounts;
		para.winners+=winners;
		para.winner_interval+=winner_interval;
		para.total_interval += total_interval;
	}

	void recordAbortStatus(int stat) {
		atomic_inc32(&abortCounts);

		if((stat & _XABORT_EXPLICIT) != 0)
			atomic_inc32(&status[XABORT_EXPLICIT_INDEX]);
		if((stat &  _XABORT_RETRY) != 0)
			atomic_inc32(&status[XABORT_RETRY_INDEX]);
		if((stat & _XABORT_CONFLICT) != 0)
			atomic_inc32(&status[XABORT_CONFLICT_INDEX]);
		if((stat & _XABORT_CAPACITY) != 0)
			atomic_inc32(&status[XABORT_CAPACITY_INDEX]);
		if((stat & _XABORT_DEBUG) != 0)
			atomic_inc32(&status[XABORT_DEBUG_INDEX]);
		if((stat &  _XABORT_NESTED) != 0)
			atomic_inc32(&status[XABORT_NESTED_INDEX]);
	}

	void MergeLocalStatus(RTMProfile& stat) {
		atomic_inc32(&totalCounts);
		atomic_add32(&abortCounts, stat.abortCounts);
		atomic_add32(&status[XABORT_EXPLICIT_INDEX], stat.status[XABORT_EXPLICIT_INDEX]);
		atomic_add32(&status[XABORT_RETRY_INDEX], stat.status[XABORT_RETRY_INDEX]);
		atomic_add32(&status[XABORT_CONFLICT_INDEX], stat.status[XABORT_CONFLICT_INDEX]);
		atomic_add32(&status[XABORT_CAPACITY_INDEX], stat.status[XABORT_CAPACITY_INDEX]);
		atomic_add32(&status[XABORT_DEBUG_INDEX], stat.status[XABORT_DEBUG_INDEX]);
		atomic_add32(&status[XABORT_NESTED_INDEX], stat.status[XABORT_NESTED_INDEX]);
	}

	void localRecordAbortStatus(int stat) {
		abortCounts++;

		if((stat & _XABORT_EXPLICIT) != 0)
			status[XABORT_EXPLICIT_INDEX]++;
		if((stat &  _XABORT_RETRY) != 0)
			status[XABORT_RETRY_INDEX]++;
		if((stat & _XABORT_CONFLICT) != 0)
			status[XABORT_CONFLICT_INDEX]++;
		if((stat & _XABORT_CAPACITY) != 0)
			status[XABORT_CAPACITY_INDEX]++;
		if((stat & _XABORT_DEBUG) != 0)
			status[XABORT_DEBUG_INDEX]++;
		if((stat &  _XABORT_NESTED) != 0)
			status[XABORT_NESTED_INDEX]++;
	}

	void reportAbortStatus() {
		//printf("[Alex]reportAbortStatus\n");
		if(status[XBEGIN_STARTED_INDEX] != 0)
			printf("XBEGIN_STARTED %d\n", status[XBEGIN_STARTED_INDEX]);
		if(status[XABORT_EXPLICIT_INDEX] != 0)
			printf("XABORT_EXPLICIT %d\n", status[XABORT_EXPLICIT_INDEX]);
		if(status[XABORT_RETRY_INDEX] != 0)
			printf("XABORT_RETRY %d\n", status[XABORT_RETRY_INDEX]);
		if(status[XABORT_CONFLICT_INDEX] != 0)
			printf("XABORT_CONFLICT %d\n", status[XABORT_CONFLICT_INDEX]);
		if(status[XABORT_CAPACITY_INDEX] != 0)
			printf("XABORT_CAPACITY %d\n", status[XABORT_CAPACITY_INDEX]);
		if(status[XABORT_DEBUG_INDEX] != 0)
			printf("XABORT_ZERO %d\n", status[XABORT_DEBUG_INDEX]);
		if(status[XABORT_NESTED_INDEX] != 0)
			printf("XABORT_LOCK %d\n", status[XABORT_NESTED_INDEX]);

		if(totalCounts != 0) {
			printf("============================\n");
			printf("Commit Counts %d\n", totalCounts);
			printf("Abort Counts %d\n", abortCounts);
			
			printf("Capacity Counts %d\n", capacityCounts);
			printf("Zero Counts %d\n", zeroCounts);
			printf("Conflict Counts %d\n", conflictCounts);
			printf("Fallback Counts %d\n", fallbacks);
		}
	}

	~RTMProfile() {
		pthread_spin_destroy(&slock);
	}

private:
	RTMProfile(const RTMProfile&);
	void operator=(const RTMProfile&);
};

#endif  // STORAGE_LEVELDB_UTIL_TXPROFILE_H_
