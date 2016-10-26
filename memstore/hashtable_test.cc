// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include <deque>
#include <set>
#include "port/port.h"
#include "port/atomic.h"
#include <iostream>
#include "util/mutexlock.h"
#include "leveldb/comparator.h"

#include <vector>
#include "memstore/memstore_hash.h"
#include "lockfreememstore/lockfree_hash.h"


static const char* FLAGS_benchmarks ="putget";

static int FLAGS_num = 10000000;
static int FLAGS_threads = 1;

#define CHECK 0

namespace leveldb {
	
typedef uint64_t Key;

__inline__ int64_t XADD64(int64_t* addr, int64_t val) {
    asm volatile(
        "lock;xaddq %0, %1"
        : "+a"(val), "+m"(*addr)
        :
        : "cc");

    return val;
}

class kvrandom_lcg_nr_simple { public:
    enum { min_value = 0, max_value = 0xFFFFFFFFU };
    typedef uint32_t value_type;
    typedef uint32_t seed_type;
    kvrandom_lcg_nr_simple()
	: seed_(default_seed) {
    }
    explicit kvrandom_lcg_nr_simple(seed_type seed)
	: seed_(seed) {
    }
    void reset(seed_type seed) {
	seed_ = seed;
    }
    value_type next() {
	return (seed_ = seed_ * a + c);
    }
  private:
    uint32_t seed_;
    enum { default_seed = 819234718U, a = 1664525U, c = 1013904223U };
};

class kvrandom_lcg_nr : public kvrandom_lcg_nr_simple { public:
    enum { min_value = 0, max_value = 0x7FFFFFFF };
    typedef int32_t value_type;
    value_type next() {
	uint32_t x0 = kvrandom_lcg_nr_simple::next(),
	    x1 = kvrandom_lcg_nr_simple::next();
	return (x0 >> 15) | ((x1 & 0x7FFE) << 16);
    }
};


class Benchmark {


private:
	

   int64_t total_count;  

   Memstore *table;

   port::SpinLock slock;

   Random ramdon;
   
private:
	
		
	struct SharedState {
	  port::Mutex mu;
	  port::CondVar cv;
	  int total;

	  volatile double start_time;
	  volatile double end_time;
	  	
	  int num_initialized;
	  int num_done;
	  bool start;
	  volatile int num_half_done;
	  std::vector<Arena*>* collector;
	
	  SharedState() : cv(&mu) { }
	};
	
	// Per-thread state for concurrent executions of the same benchmark.
	struct ThreadState {
	  int tid;			   // 0..n-1 when running in n threads
	  SharedState* shared;
	  int count;
	  double time1;
	  double time2;
	  kvrandom_lcg_nr rnd;      

	  ThreadState(int index)
	      : tid(index) {
	  }
	  
	};

	  struct ThreadArg {
		Benchmark* bm;
		SharedState* shared;
		ThreadState* thread;
		void (Benchmark::*method)(ThreadState*);
	  };
	
	  static void ThreadBody(void* v) {

		//printf("ThreadBody\n");
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		SharedState* shared = arg->shared;
		ThreadState* thread = arg->thread;
		{
		  MutexLock l(&shared->mu);
 
		  
		  shared->num_initialized++;
		  if (shared->num_initialized >= shared->total) {
			shared->cv.SignalAll();
		  }
		  while (!shared->start) {
			shared->cv.Wait();
		  }
		}

		double start = leveldb::Env::Default()->NowMicros();
		if(shared->start_time == 0)
			shared->start_time = start;

		(arg->bm->*(arg->method))(thread);

		double end = leveldb::Env::Default()->NowMicros();
		shared->end_time = end;
		//thread->time = end - start;
		
		{
		  MutexLock l(&shared->mu);

		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	  }


	  struct Str {	  	
		const char *s;
		int len;
	  	Str(const char *first, const char *last)
		: s(first), len(last - first) {
    	}
	  };
	  void convertToString(uint64_t x) {
	  	
		char buf_[32];
		char *bbuf_;
		int minlen = 0;
		bbuf_ = buf_ + sizeof(buf_) - 1;
		do {
			*--bbuf_ = (x % 10) + '0';
			x /= 10;
		} while (--minlen > 0 || x != 0);
		Benchmark::Str(bbuf_, buf_ + sizeof(buf_) - 1);
	  }


	  void Mix(ThreadState* thread) {
	  	int tid = thread->tid;
		int num = thread->count;
		int seed = 31949 + tid % 48;
		double getfrac = 0.5;
		thread->rnd.reset(seed);
		const unsigned c = 2654435761U;
    	const unsigned offset = thread->rnd.next();
		
		double start = leveldb::Env::Default()->NowMicros();
		uint64_t puts = 0, gets = 0;
    	int getfrac65536 = (int) (getfrac * 65536 + 0.5);
    	while ((puts + gets) <= num) {
			if (puts == 0 || (thread->rnd.next() % 65536) >= getfrac65536) {
	  	  		// insert
	  	 		unsigned x = (offset + puts) * c;
	  			x %= 100000000;
				convertToString(x);
	   			Memstore::MemNode* mn = table->GetWithInsert(x);
				uint64_t v = x + 1;
				convertToString(v);
				mn->value = (uint64_t *)v;
	   			++puts;
			} else {
	   			// get
			   	unsigned x = (offset + (thread->rnd.next() % puts)) * c;
	   			x %= 100000000;
				convertToString(x);
			   	Memstore::MemNode* mn = table->Get(x);
				if (mn == NULL) {
					printf("Not found\n");
					continue;
				}
				uint64_t *v = mn->value;
				convertToString(x + 1);
				if ((uint64_t)v != (x + 1)) {
					printf("Wrong value\n");
					continue;
				}
	   			++gets;
			}
    	}
	    double end = leveldb::Env::Default()->NowMicros();
		printf("Exe time: %f\n", (end-start)/1000/1000);
		printf("Thread[%d] Op Throughput %lf ops/s\n", tid, num/((end - start)/1000/1000));
	  }
	  
	  void PutGet(ThreadState* thread) {
	  	int tid = thread->tid;
		int seed = 31949 + tid % 48;
		thread->rnd.reset(seed);
		int num = thread->count;
		SharedState *shared = thread->shared;
		double pstart = leveldb::Env::Default()->NowMicros();
		unsigned i;
		for (i=0; i<num; i++) {
			int32_t x = (int32_t) (thread->rnd.next());
        	x %= 100000000;
			convertToString(x);
        	Memstore::MemNode* mn = table->GetWithInsert(x);
			mn->value = new uint64_t(x+1);
			uint64_t v = x + 1;
			convertToString(v);
			mn->value = (uint64_t *)v;
		}
		double pend = leveldb::Env::Default()->NowMicros();
		thread->time1 = pend - pstart;
		printf("Exe time: %f\n", ((double)pend-(double)pstart)/1000/1000);

		printf("Thread[%d] Put Throughput %lf ops/s\n", tid, num/(thread->time1/1000/1000));
		

#if 0
		{
		  MutexLock l(&shared->mu);
	//	  table->prof.reportAbortStatus();
		  shared->num_half_done++;
		  
		}
		while (shared->num_half_done < shared->total);
		

#endif
		
#if 1

		
		int32_t *a = (int32_t *) malloc(sizeof(int32_t) * num);
    	assert(a);
		thread->rnd.reset(seed);
    	for (unsigned i = 0; i < num; ++i) {
			a[i] = (int32_t) (thread->rnd.next());
			a[i] %= 100000000;
    	}
	    for (unsigned i = 0; i < num; ++i)
			std::swap(a[i], a[thread->rnd.next() % num]);


		double gstart = leveldb::Env::Default()->NowMicros();
		for (i=0; i<num; i++) {
			convertToString(a[i]);
			Memstore::MemNode* mn = table->Get(a[i]);
			if (mn == NULL) {
				printf("Not found\n");
				continue;
			}
			uint64_t *v = mn->value;
			convertToString(a[i]+1);
/*
			if (*v ==  1) 
				printf("wrong\n");*/

			if ((uint64_t)v != (a[i]+1)) {
				printf("Wrong value\n");
				continue;
			}
		}
		double gend = leveldb::Env::Default()->NowMicros();
		thread->time2 = gend - gstart;
		printf("Exe time: %f\n", ((double)gend-(double)gstart)/1000/1000);
		printf("Thread[%d] Get Throughput %lf ops/s\n", tid, num/(thread->time2/1000/1000));		
#endif		
	  }



	public:

	  Benchmark(): total_count(FLAGS_num), ramdon(1000){}
	  ~Benchmark() {
	  	
	  	

	  }
	  
	  void RunBenchmark(int n, int num,
						void (Benchmark::*method)(ThreadState*)) {
		SharedState shared;
		shared.total = n;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		shared.num_half_done = 0;
	
//		double start = leveldb::Env::Default()->NowMicros();
		
		ThreadArg* arg = new ThreadArg[n];
		for (int i = 0; i < n; i++) {
		  arg[i].bm = this;
		  arg[i].method = method;
		  arg[i].shared = &shared;
		  arg[i].thread = new ThreadState(i);
		  arg[i].thread->shared = &shared;
		  arg[i].thread->count = num;
		  Env::Default()->StartThread(ThreadBody, &arg[i]);
		}
	
		shared.mu.Lock();
		while (shared.num_initialized < n) {
		  shared.cv.Wait();
		}
	
		shared.start = true;
		printf("Send Start Signal\n");
		shared.cv.SignalAll();
//		std::cout << "Startup Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;
		
		while (shared.num_done < n) {
		  shared.cv.Wait();
		}
		shared.mu.Unlock();

		
		printf("Total Run Time : %lf ms\n", (shared.end_time - shared.start_time)/1000);

	/*	
		for (int i = 0; i < n; i++) {
		  printf("Thread[%d] Put Throughput %lf ops/s\n", i, num/(arg[i].thread->time1/1000/1000));
		  printf("Thread[%d] Get Throughput %lf ops/s\n", i, num/(arg[i].thread->time2/1000/1000));
		}*/
		
		for (int i = 0; i < n; i++) {
		  delete arg[i].thread;
		}
		delete[] arg;
	  }


	void Run(){

	  table = new leveldb::MemstoreHashTable();
      //table = new leveldb::LockfreeHashTable();
 
      int num_threads = FLAGS_threads;  
      int num_ = FLAGS_num;
    

	  Slice name = FLAGS_benchmarks;
	  
	  void (Benchmark::*method)(ThreadState*) = NULL;
	  if (name == "putget")
	  	method = &Benchmark::PutGet;
	  else if (name == "mix")
	  	method = &Benchmark::Mix;
      RunBenchmark(num_threads, num_, method);
	  table->PrintStore();
	  delete table;
	  
    }
  
};




}  // namespace leveldb



int main(int argc, char** argv) {

 for (int i = 1; i < argc; i++) {

	 int n;
	 char junk;
	 
	 if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
	   FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
	 } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
	   FLAGS_num = n;
	 } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
	   FLAGS_threads = n;
	 } 
 }

  leveldb::Benchmark benchmark;
  benchmark.Run();
//  while (1);
  return 1;
}
