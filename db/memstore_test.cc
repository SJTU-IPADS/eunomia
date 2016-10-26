// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/hashtable_template.h"
#include "db/dbtransaction_template.h"
#include "db/txmemstore_template.h"

#include "util/mutexlock.h"
#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include <deque>
#include <set>
#include "port/port.h"
#include <iostream>
#include "leveldb/comparator.h"
#include "dbformat.h"
#include <vector>
#include "db/memstore_skiplist.h"

static const char* FLAGS_benchmarks ="lockfree";

static int FLAGS_num = 1000000;
static int FLAGS_threads = 1;


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

unsigned long Read_tsc(void)
{
      unsigned a, d;
      __asm __volatile("rdtsc":"=a"(a), "=d"(d));
      return ((unsigned long)a) | (((unsigned long) d) << 32);
}


class Benchmark {


private:
	//leveldb::ScalaSkipList ssl;

   int64_t total_count;
   int64_t read_count;
   int64_t write_count;
   int *array;
   char *tmparray;
   SpinLock slock;
   RTMProfile gprof;

   MemStoreSkipList memstore;
   
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

	
	  SharedState() : cv(&mu) { }
	};
	
	// Per-thread state for concurrent executions of the same benchmark.
	struct ThreadState {
	  int tid;			   // 0..n-1 when running in n threads
	  SharedState* shared;
	  int count;
	  int falseConflict;
	  int conflict;
	  uint64_t addT;
	  uint64_t getT;
	  uint64_t valT;
	  uint64_t comT;
	  uint64_t time;
	  Random rnd;         // Has different seeds for different threads

	  ThreadState(int index)
	      : tid(index),
	        rnd(1000 + index) {
	 		falseConflict = 0;
			conflict = 0;
			addT = 0;
			getT = 0;
			valT = 0;
			comT = 0;
	  }
	  
	};

	  struct ThreadArg {
		Benchmark* bm;
		SharedState* shared;
		ThreadState* thread;
		void (Benchmark::*method)(ThreadState*);
	  };
	
	  static void ThreadBody(void* v) {
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
        
		
		if(shared->start_time == 0)
			shared->start_time =  leveldb::Env::Default()->NowMicros();

		uint64_t start = Read_tsc();
		
		(arg->bm->*(arg->method))(thread);
		//std::cout << thread->tid << std::endl;
		uint64_t end = Read_tsc();
		
		shared->end_time = leveldb::Env::Default()->NowMicros();
		
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	  }
	   	 
	   void LockFreeInsert(ThreadState* thread) {

		  memstore.ThreadLocalInit();
		  
		  uint64_t tid = thread->tid;
		  int index = 0;
		  int retry = 0;
		  
		   while(total_count > 0) {

			
			int64_t oldv = XADD64(&total_count, -1000);
			if(oldv <= 0)
				   break;
				
			for (int i =0; i < 1000; i++) {
				index++;
				uint64_t key = (tid <<32)|index;
//				printf("%d insert %lx\n", tid, key);
				MemStoreSkipList::Node* x = memstore.GetLatestNodeWithInsert(key);
				if(x == NULL)
				{
					printf("Error\n");
					exit(1);
				}
			}

		  }
	   }


       

	public:

	  Benchmark(): 
	  	total_count(FLAGS_num)
	  {

	  }
	  
	  ~Benchmark() {}
	  
	  void RunBenchmark(int n,
						void (Benchmark::*method)(ThreadState*)) {

		total_count = FLAGS_num;
		
		SharedState shared;
		shared.total = n;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
	
//		double start = leveldb::Env::Default()->NowMicros();
		
		ThreadArg* arg = new ThreadArg[n];
		for (int i = 0; i < n; i++) {
		  arg[i].bm = this;
		  arg[i].method = method;
		  arg[i].shared = &shared;
		  arg[i].thread = new ThreadState(i);
		  arg[i].thread->shared = &shared;
		  arg[i].thread->time = 0;
		  Env::Default()->StartThread(ThreadBody, &arg[i]);
		}
	
		shared.mu.Lock();
		while (shared.num_initialized < n) {
		  shared.cv.Wait();
		}
	
		shared.start = true;
		printf("Send Start Signal\n");
		shared.cv.SignalAll();

		double start = leveldb::Env::Default()->NowMicros();

		while (shared.num_done < n) {
		  shared.cv.Wait();
		}
		shared.mu.Unlock();

		double end = leveldb::Env::Default()->NowMicros();


		printf("Total Run Time : %lf ms\n", (end - start)/1000);

		
		for (int i = 0; i < n; i++) {
		  printf("Thread[%d] Run Time %lf ms\n", i, arg[i].thread->time/1000);
		}

		for (int i = 0; i < n; i++) {
		  delete arg[i].thread;
		}
		delete[] arg;
	  }


	void Run(){

	  total_count = FLAGS_num;
	  int num_threads = FLAGS_threads;  
	  
	  const char* benchmarks = FLAGS_benchmarks;
	  while (benchmarks != NULL) {
		const char* sep = strchr(benchmarks, ',');
		Slice name;
		if (sep == NULL) {
		  name = benchmarks;
		  benchmarks = NULL;
		} else {
		  name = Slice(benchmarks, sep - benchmarks);
		  benchmarks = sep + 1;
		}

	  
	      void (Benchmark::*method)(ThreadState*) = NULL;

		  if (name == Slice("lockfree")) {
	        method = &Benchmark::LockFreeInsert;
	      } else {
	        printf("Wrong benchmark name %s\n", name.ToString().c_str());
			return;
		  }

		  double start = leveldb::Env::Default()->NowMicros();
		  total_count = FLAGS_num;

	      RunBenchmark(num_threads, method);

		  memstore.PrintList();

//	      std::cout << name.ToString() << " total Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;

	  	}
	  
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

  
  return 1;
}
