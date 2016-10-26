// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RTM_H_
#define STORAGE_LEVELDB_UTIL_RTM_H_
#include <immintrin.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include "util/spinlock.h"
#include "txprofile.h"


#define BILLION 1000000000L

#define MAXNEST 0
#define MAXZERO 3
#define MAXCAPACITY 16
#define MAXCONFLICT 128

#define RTMPROFILE 0

#define MAXWRITE 64
#define MAXREAD 128

#define SIMPLERETY 0

#define AVOIDNESTTX

enum OP_TYPE {GET_TYPE, ADD_TYPE, UPDATE_TYPE , DEL_TYPE, UNKNOWN_TYPE};
class RTMScope {
	RTMProfile* globalprof;
	int retry;
	int conflict;
	int capacity;
	int nested;
	int zero;
	SpinLock* slock;
	OP_TYPE type;
	bool profiled;
#ifdef AVOIDNESTTX
	int isnest;
#endif
	//struct LeafNode;
public:
	static SpinLock fblock;

	inline RTMScope(RTMProfile* prof, int read = 1, int write = 1,
					SpinLock* sl = NULL, OP_TYPE _type = UNKNOWN_TYPE, bool profiled_ = false, int* conflict_num = NULL) {
		type = _type;
		//clock_gettime(CLOCK_MONOTONIC, &begin);
		globalprof = prof;
		profiled = profiled_;
		retry = 0;
		conflict = 0;
		capacity = 0;
		zero = 0;
		nested = 0;
#ifdef AVOIDNESTTX
		isnest = _xtest();
#endif

		if(sl == NULL) {
			//If the user doesn't provide a lock, we give him a default locking
			slock = &fblock;
		} else {
			slock = sl;
		}

#ifdef AVOIDNESTTX
		if(isnest != 0) {
			if(slock->IsLocked())
				_xabort(0xff);
			return;
		}
#endif

#if !SIMPLERETY
		if(read > MAXREAD || write > MAXWRITE) {
			slock->Lock();
			return;
		}
#endif

		while(true) {
			unsigned stat;
			stat = _xbegin();
			if(stat == _XBEGIN_STARTED) {
				//Put the global lock into read set
				if(slock->IsLocked()) {
					_xabort(0xff);
				}
				return;
			} else {
				retry++;
				if(stat == 0) //_XABORT_ZERO
					zero++;
				else if((stat & _XABORT_CONFLICT) != 0)
					conflict++;
				else if((stat & _XABORT_CAPACITY) != 0)
					capacity++;
				else if((stat & _XABORT_NESTED) != 0)
					nested++;

				if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
					while(slock->IsLocked())
						_mm_pause();
				}

#if SIMPLERETY
				if(retry > 100)
					break;
#else

				int step = (read + write) / 64;

				if(step == 0) {
					step = 1;
				}
				if(nested > MAXNEST) {
					break;
				}

				if(zero > MAXZERO) {
					break;
				}

				if(capacity > MAXCAPACITY / step) {
					break;
				}

				if(conflict > MAXCONFLICT / step) {
					break;
				}
#endif

			}
		}
		slock->Lock();
#if RTMPROFILE
if(profiled){
		globalprof->fallbacks++;
}
#endif
	}

	void Abort() {
		_xabort(0x1);
	}

	inline  ~RTMScope() {
#ifdef AVOIDNESTTX
		if(isnest != 0) {
			return;
		}
#endif
		if(slock->IsLocked())
			slock->Unlock();
		else
			_xend();
		//access the global profile info outside the transaction scope
		//clock_gettime(CLOCK_MONOTONIC, &end);
#if RTMPROFILE
		if(profiled && globalprof != NULL) {
			globalprof->totalCounts++;
			globalprof->abortCounts += retry;
			globalprof->capacityCounts += capacity;
			globalprof->conflictCounts += conflict;
			globalprof->zeroCounts += zero;
			/*if(winner) {
				globalprof->winners++;
				uint64_t winner_interval = (end.tv_sec - begin.tv_sec) * BILLION + (end.tv_nsec - begin.tv_nsec);
				globalprof->winner_interval += winner_interval;
				//printf("Winner interval = %ld\n", winner_interval);
			}
			uint64_t total_interval = (end.tv_sec - begin.tv_sec) * BILLION + (end.tv_nsec - begin.tv_nsec);
			globalprof->total_interval += total_interval;
			*/
		}
		
#endif
	}
	inline int getRetry() {
		//printf("retry:%d\n", retry);
		return retry;	
	}

private:
	RTMScope(const RTMScope&);
	void operator=(const RTMScope&);
};
#endif  // STORAGE_LEVELDB_UTIL_RTM_H_
