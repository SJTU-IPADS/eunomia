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

#define CPUFREQ 3400000000
#define ARRAYSIZE 256*1024*1024
#define L1SIZE 32*1024*1024
#define L2SIZE 256*1024*1024

static const char* FLAGS_benchmarks ="rtm,"
	"lock,"
	"native,"
	"bar";

static int FLAGS_num = 1000000;
static int FLAGS_threads = 1;
static int FLAGS_rdnum = 2;
static int FLAGS_wtnum = 1;



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
		thread->time = (end - start) * 1000 / CPUFREQ;

		
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	  }

	   void Warmup(ThreadState* thread) {

		//warm up
		 for(int index = thread->tid*ARRAYSIZE/4; index < write_count ; index++) {
			array[index] = index;
		 }
	   }
	   	 
	   void RTMWrite(ThreadState* thread) {

		  Warmup(thread);
		  int tid = thread->tid;
		  int seqNum = 0;
		  int rnum = read_count;
		  int wnum = write_count;
		  unsigned stat;
		  int index = 0;
		  int retry = 0;

		  uint64_t befabort = 0;
		  uint64_t aftabort = 0;

		  uint64_t start = 0;
		  uint64_t end = 0;
		  uint64_t totalTime = 0;
		  int count = 0;
		  uint64_t comTime = 0;
		  uint64_t execTime = 0;
		  uint64_t begTime = 0;
		  uint64_t befBegin = 0;
		  uint64_t aftBegin = 0;

		   start = Read_tsc();

		   while(total_count > 0) {
				
				int64_t oldv = XADD64(&total_count, -1000);
				if(oldv <= 0)
					   break;
				
			for (int i =0; i < 1000; i++) {
tag:			
	          
			  stat = _xbegin ();
			  
			  if (stat == _XBEGIN_STARTED) {
			  	
			    for(int index = tid*ARRAYSIZE/4; index < wnum ; index++) {
				  array[index] = index;
			    }
			
			   
			  _xend();

				count++;
			  } else {

				retry++;
				goto tag;
			  }
			  
			}

		  }
		  end = Read_tsc();
		  totalTime += end - start;
			  
		  printf("RTM Average Cycle %ld\n", totalTime / count);
		  printf("RTM Retry Count %d\n", retry);
		  printf("RTM Succ Count %d\n", count);
		 // printf("Commit Cycle %ld\n", (comTime / count));
		 //printf("Start Cycle %ld\n", (begTime / count));
		 // printf("Compute Cycle %ld\n", (execTime / count));

	   }


       
       void NativeWrite(ThreadState* thread){

		  Warmup(thread);
		  int tid = thread->tid;
		  int seqNum = 0;
		  int rnum = read_count;
		  int wnum = write_count;
		  unsigned stat;
		  int index = 0;
		  int retry = 0;

		  uint64_t befabort = 0;
		  uint64_t aftabort = 0;

		  uint64_t start = 0;
		  uint64_t end = 0;
		  uint64_t totalTime = 0;
		  int count = 0;
		  uint64_t comTime = 0;

		 start = Read_tsc();

		  while(total_count > 0) {
				
				int64_t oldv = XADD64(&total_count, -1000);
				if(oldv <= 0)
					   break;
		
		  for (int i =0; i < 1000; i++) {
			
		  	  for(int index = tid*ARRAYSIZE/4; index < wnum ; index++) {
				  array[index] = index;
			    }
		
			  count++;
			 
			  
			}

		  }
		  end = Read_tsc();
		  totalTime += end - start;
		  printf("Native Average Cycle %ld\n", totalTime / count);
		  printf("Native Succ Count %d\n", count);		  

	   }
	   
	   void LockWrite(ThreadState* thread) {
		   Warmup(thread);

		  
		  int tid = thread->tid;
		  int seqNum = 0;
		  int rnum = read_count;
		  int wnum = write_count;
		  unsigned stat;
		  int index = 0;
		  int retry = 0;

		  uint64_t befabort = 0;
		  uint64_t aftabort = 0;

		  uint64_t start = 0;
		  uint64_t end = 0;
		  uint64_t totalTime = 0;
		  int count = 0;
		  uint64_t comTime = 0;
		  
		  uint64_t total_cycle = 10000;
		  
		 start = Read_tsc();

		  while(total_count > 0) {
				
				int64_t oldv = XADD64(&total_count, -1000);
				if(oldv <= 0)
					   break;
		
		  for (int i =0; i < 1000; i++) {

			  uint64_t beg = 0;
			  slock.Lock();
		  	  //for(int index = tid*ARRAYSIZE/4; index < wnum ; index++) {
				//  array[index] = index;
			    //}
			  beg = Read_tsc();
			  while((Read_tsc() - beg) < (total_cycle/4*3));
	
			  slock.Unlock();

			  beg = Read_tsc();
			  while((Read_tsc() - beg) < (total_cycle/4));

			  count++;
			  
			}

		  }
		  end = Read_tsc();
		  totalTime += end - start;
		  printf("Lock Average Cycle %ld\n", totalTime / count);
		  printf("Lock Succ Count %d\n", count);		  

	   }


	    void BarWrite(ThreadState* thread) {

		  Warmup(thread);
		  int tid = thread->tid;
		  int seqNum = 0;
		  int rnum = read_count;
		  int wnum = write_count;
		  unsigned stat;
		  int index = 0;
		  int retry = 0;

		  uint64_t befabort = 0;
		  uint64_t aftabort = 0;

		  uint64_t start = 0;
		  uint64_t end = 0;
		  uint64_t totalTime = 0;
		  int count = 0;
		  uint64_t comTime = 0;

		 start = Read_tsc();

		   while(total_count > 0) {

//				printf("total_count %d\n", total_count);
				int64_t oldv = XADD64(&total_count, -1000);
				if(oldv <= 0)
					   break;
				
		  for (int i =0; i < 1000; i++) {

			  
		  	  for(int index = tid*ARRAYSIZE/4; index < wnum ; index++) {
				  array[index] = index;
			    }
		
			  count++;
			  mb();
			  
			}
		  }
		  end = Read_tsc();
		  totalTime += end - start;
		  printf("Bar Average Cycle %ld\n", totalTime / count);
		  printf("Bar Succ Count %d\n", count);		  

	   }


	public:

	  Benchmark(): 
	  	total_count(FLAGS_num), read_count(FLAGS_rdnum), 
		write_count(FLAGS_wtnum)
	  {

	    tmparray = new char[L1SIZE];
	
	  	array = new int[ARRAYSIZE];
		
		for( int i = 0; i < ARRAYSIZE; i++)
			array[i] = 0;
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
/*
		printf(" ...... Iterate  MemStore ......\n");
//		memstore.DumpTXMemStore();

		printf("Throughput %lf txs/s\n", totaltxs * 1000000 / (end - start));

		printf("Total Run Time : %lf ms\n", (end - start)/1000);

		
		for (int i = 0; i < n; i++) {
		  printf("Thread[%d] Run Time %lf ms\n", i, arg[i].thread->time/1000);
		}

		int conflict = 0;
		int falseConflict = 0;
		uint64_t addT = 0;
		uint64_t getT = 0;
		uint64_t valT = 0;
		uint64_t comT = 0;
		
		for (int i = 0; i < n; i++) {
		 	conflict += arg[i].thread->conflict;
			falseConflict += arg[i].thread->falseConflict;
			addT += arg[i].thread->addT;
			getT += arg[i].thread->getT;
			valT += arg[i].thread->valT;
			comT += arg[i].thread->comT;
		}

		printf("Conflict %d FalseConflict %d\n", conflict, falseConflict);
		printf("Get %ld ms  Add %ld ms Validate %ld ms  Commit %ld ms\n", 
			getT * 1000/CPUFREQ, addT * 1000/CPUFREQ, valT * 1000/CPUFREQ, comT * 1000/CPUFREQ);
	*/	
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

		  if (name == Slice("rtm")) {
	        method = &Benchmark::RTMWrite;
	      } else if (name == Slice("lock")) {
	        method = &Benchmark::LockWrite;
	      } else if (name == Slice("native")) {
	        method = &Benchmark::NativeWrite;
		  } else if (name == Slice("bar")) {
	        method = &Benchmark::BarWrite;
		  } else {
	        printf("Wrong benchmark name %s\n", name.ToString().c_str());
			return;
		  }

		  double start = leveldb::Env::Default()->NowMicros();
		  total_count = FLAGS_num;

	      RunBenchmark(num_threads, method);

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
	 } else if (sscanf(argv[i], "--read=%d%c", &n, &junk) == 1) {
	   FLAGS_rdnum= n;
	 } else if (sscanf(argv[i], "--write=%d%c", &n, &junk) == 1) {
	   FLAGS_wtnum = n;
	 }
 }

	
  leveldb::Benchmark benchmark;
  benchmark.Run();

  
  return 1;
}
