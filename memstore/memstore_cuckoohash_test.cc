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
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_cuckoohash.h"


static const char* FLAGS_benchmarks ="random";

static int FLAGS_num = 10000000;
static int FLAGS_threads = 1;

#define CHECK 0
#define SEQ 0


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


class Benchmark {


private:
	

   int64_t total_count;  

   Memstore *btree;

   port::SpinLock slock;

   Random ramdon;
   
private:
	
	static uint64_t getkey(Key key) { return (key >> 40); }
	static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
	static uint64_t hash(Key key) { return key & 0xff; }

	static uint64_t HashNumbers(uint64_t k, uint64_t g) {
		uint64_t data[2] = { k, g };
		return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
	}

	Key MakeKey(uint64_t k, uint64_t g) {
		assert(sizeof(Key) == sizeof(uint64_t));
		assert(g <= 0xffffffffu);
		//return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
	//	return ((k << 40) | (g));
		
		return (g << 16 | k);
	/*	MutexSpinLock lock(&slock);
		Key key = ramdon.Next();
		return key;*/
	}
		
	struct SharedState {
	  port::Mutex mu;
	  port::CondVar cv;
	  int total;

	  volatile double start_time;
	  volatile double end_time;
	  	
	  int num_initialized;
	  int num_done;
	  bool start;

	  std::vector<Arena*>* collector;
	
	  SharedState() : cv(&mu) { }
	};
	
	// Per-thread state for concurrent executions of the same benchmark.
	struct ThreadState {
	  int tid;			   // 0..n-1 when running in n threads
	  SharedState* shared;
	  int count;
	  double time;
	  Random rnd;         // Has different seeds for different threads

	  ThreadState(int index)
	      : tid(index),
	        rnd(1000 + index) {
	  }
	  
	};

	  struct ThreadArg {
		Benchmark* bm;
		SharedState* shared;
		ThreadState* thread;
		void (Benchmark::*method)(ThreadState*);
	  };
	
	  static void ThreadBody(void* v) {

		printf("ThreadBody\n");
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
		thread->time = end - start;
		
		{
		  MutexLock l(&shared->mu);

		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	  }




	  void Insert(ThreadState* thread) {
			  
				  int tid = thread->tid;
				  int seqNum = 0;
				  
		#if CHECK
				 for (int i=0; i<total_count; i++) {
				 	Key k = i*10 + tid;
				//	printf("Insert %d %ld\n", tid, k);
					btree->GetWithInsert(k);
				 }
		#elif SEQ 
		 uint64_t seq = 0;
		
		 while(total_count > 0) {
	
			  int64_t oldv = XADD64(&total_count, -1000);
			  if(oldv <= 0)
				  break;
			  
			  for (int i =0; i < 1000; i++) {
				  
				  uint64_t k;
	
					k = (((uint64_t)tid) << 40 | seq);
					seq++;
					btree->GetWithInsert(k);
	
			  }
		  }
			
		#else
		  while(total_count > 0) {
		
				  int64_t oldv = XADD64(&total_count, -1000);
				  if(oldv <= 0)
					  break;
				  
				  for (int i =0; i < 1000; i++) {
					  
					  Key k;
		
						k = MakeKey(tid, thread->rnd.Next());
						//printf("Insert %d %lx\n", tid, k);
						//k = MakeKey(tid,seqNum++);
						btree->GetWithInsert(k);
		
				  }
			  }
		  
		#endif		
	   }



	   void ReadRandom(ThreadState* thread) {
 	   	  DoRead(thread, false);
	   }

	   void ReadSeq(ThreadState* thread) {
 	   	  DoRead(thread, true);
	   }

	   
	   void DoRead(ThreadState* thread, bool seq) {
	   
		   int tid = thread->tid;
		   int seqNum = 0;
		   
		   while(total_count > 0) {

			   uint64_t oldv = XADD64(&total_count, -100);
			   
			   if(oldv <= 0)
			   	   break;
			   
			   for (int i =0; i < 100; i++) {
	    		   
				   Key k;
				   if(seq)
				   	 k = MakeKey(tid,seqNum++);
				   else
				   	 k = MakeKey(tid, thread->rnd.Next());


				 if(btree->Get(k)==NULL) {
					printf(" %ld Not Found\n", k);
				 }
			   }
		   }
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

	
//		double start = leveldb::Env::Default()->NowMicros();
		
		ThreadArg* arg = new ThreadArg[n];
		for (int i = 0; i < n; i++) {
		  arg[i].bm = this;
		  arg[i].method = method;
		  arg[i].shared = &shared;
		  arg[i].thread = new ThreadState(i);
		  arg[i].thread->shared = &shared;
		  arg[i].thread->count = num;
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
//		std::cout << "Startup Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;
		
		while (shared.num_done < n) {
		  shared.cv.Wait();
		}
		shared.mu.Unlock();


		printf("Total Run Time : %lf ms\n", (shared.end_time - shared.start_time)/1000);
		
		for (int i = 0; i < n; i++) {
		  printf("Thread[%d] Run Time %lf ms\n", i, arg[i].thread->time/1000);
		}
		
		for (int i = 0; i < n; i++) {
		  delete arg[i].thread;
		}
		delete[] arg;
	  }


	void Run(){

	  int num_threads = FLAGS_threads;  
      int num_ = FLAGS_num/num_threads;
    
      void (Benchmark::*wmethod)(ThreadState*) = NULL;
	  void (Benchmark::*rmethod)(ThreadState*) = NULL;

	  Slice name = FLAGS_benchmarks;
	  

//	  double start = leveldb::Env::Default()->NowMicros();
	  total_count = FLAGS_num;
//	   btree = new MemstoreCuckooHashTable();

	  btree = new MemstoreBPlusTree();

      RunBenchmark(num_threads, num_, &Benchmark::Insert);

	  
#if CHECK
	 for (int i=0; i<FLAGS_num; i++) {
	 	for (int j=0; j<num_threads; j++) {
			Key k = i*10 + j;
			if (btree->Get(k) == NULL) printf("Not found %lx\n",k);
	 	}
	 }
#endif	 
//	  total_count = FLAGS_num;
 //     std::cout << "Total Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;
	  delete (MemstoreBPlusTree *)btree;
//	delete (MemstoreCuckooHashTable *)btree;

	 
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
