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
#include "lockfreememstore/lockfree_hash.h"
#include "memstore/memstore_hash.h"
#include "memstore/memstore_skiplist.h"
#include "memstore/memstore_cuckoohash.h"




#include "db/dbtx.h"
#include "db/dbtables.h"
static const char* FLAGS_benchmarks = "mix";

static int FLAGS_num = 10000000;
static int FLAGS_threads = 1;
static uint64_t nkeys = 80000000;
#define CHECK 0
#define YCSBRecordSize 100
#define GETCOPY 0
namespace leveldb {

typedef uint64_t Key;
typedef uint64_t* (*Key_Generator)(int32_t);

class fast_random {
public:
	fast_random(unsigned long seed)
		: seed(0) {
		set_seed0(seed);
	}

	inline unsigned long
	next() {
		return ((unsigned long) next(32) << 32) + next(32);
	}

	inline uint32_t
	next_u32() {
		return next(32);
	}

	inline uint16_t
	next_u16() {
		return next(16);
	}

	/** [0.0, 1.0) */
	inline double
	next_uniform() {
		return (((unsigned long) next(26) << 27) + next(27)) / (double)(1L << 53);
	}

	inline char
	next_char() {
		return next(8) % 256;
	}

	inline std::string
	next_string(size_t len) {
		std::string s(len, 0);
		for(size_t i = 0; i < len; i++)
			s[i] = next_char();
		return s;
	}

	inline unsigned long
	get_seed() {
		return seed;
	}

	inline void
	set_seed(unsigned long seed) {
		this->seed = seed;
	}

private:
	inline void
	set_seed0(unsigned long seed) {
		this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
	}

	inline unsigned long
	next(unsigned int bits) {
		seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
		return (unsigned long)(seed >> (48 - bits));
	}

	unsigned long seed;
};



class Benchmark {


private:


	int64_t total_count;

	MemstoreBPlusTree* table;

	port::SpinLock slock;

	Random ramdon;

	Key_Generator key_generator;

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
		fast_random rnd;
		uint64_t seed;

		ThreadState(int index, uint64_t seed)
			: tid(index), rnd(seed) {
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
			if(shared->num_initialized >= shared->total) {
				shared->cv.SignalAll();
			}
			while(!shared->start) {
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
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}

	void Mix(ThreadState* thread) {
		int tid = thread->tid;
		int num = thread->count;
		int finish = 0 ;
		fast_random r = thread->rnd;
		double start = leveldb::Env::Default()->NowMicros();
		std::string v;
		char nv[YCSBRecordSize];
		while(finish < num) {
			double d = r.next_uniform();
			//Read
			if(d < 0.8) {
				uint64_t key = r.next() % nkeys;
				Memstore::MemNode * mn = table->Get(key);
				char *s = (char *)(mn);
#if GETCOPY
				std::string *p = &v;
				p->assign(s, YCSBRecordSize);
#else
				if(s == NULL)
					printf("N");
#endif
				finish++;
			}
			//RMW
			if(d < 0.2) {
				uint64_t key = r.next() % nkeys;
				Memstore::MemNode * mn = table->Get(key);
				char *s = (char *)(mn);
#if GETCOPY
				std::string *p = &v;
				p->assign(s,  YCSBRecordSize);
#else
				if(s == NULL)
					printf("N");
#endif
				std::string c(YCSBRecordSize, 'c');
				memcpy(nv, c.data(), YCSBRecordSize);
				table->TPut(key, (uint64_t *)(nv));

				finish++;
			}
		}



		double end = leveldb::Env::Default()->NowMicros();
		printf("Exe time: %f\n", (end - start) / 1000 / 1000);
		printf("Thread[%d] Op Throughput %lf ops/s\n", tid, finish / ((end - start) / 1000 / 1000));
	}



public:

	Benchmark(): total_count(FLAGS_num), ramdon(1000) {}
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
		fast_random r(8544290);
		ThreadArg* arg = new ThreadArg[n];
		for(int i = 0; i < n; i++) {
			arg[i].bm = this;
			arg[i].method = method;
			arg[i].shared = &shared;
			arg[i].thread = new ThreadState(i, r.next());
			arg[i].thread->shared = &shared;
			arg[i].thread->count = num;
			Env::Default()->StartThread(ThreadBody, &arg[i]);
		}

		shared.mu.Lock();
		while(shared.num_initialized < n) {
			shared.cv.Wait();
		}

		shared.start = true;
		printf("Send Start Signal\n");
		shared.cv.SignalAll();
//		std::cout << "Startup Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;

		while(shared.num_done < n) {
			shared.cv.Wait();
		}
		shared.mu.Unlock();


		printf("Total Run Time : %lf ms\n", (shared.end_time - shared.start_time) / 1000);
		/*
			for (int i = 0; i < n; i++) {
			  printf("Thread[%d] Put Throughput %lf ops/s\n", i, num/(arg[i].thread->time1/1000/1000));
			  printf("Thread[%d] Get Throughput %lf ops/s\n", i, num/(arg[i].thread->time2/1000/1000));
			}*/

		for(int i = 0; i < n; i++) {
			delete arg[i].thread;
		}
		delete[] arg;
	}


	void Run() {

		table = new leveldb::MemstoreBPlusTree();
		//table = new leveldb::LockfreeHashTable();
		//table = new leveldb::MemstoreHashTable();
		//table = new leveldb::MemStoreSkipList();
		//table = new MemstoreCuckooHashTable();


		int num_threads = FLAGS_threads;
		int num_ = FLAGS_num;
		Slice name = FLAGS_benchmarks;




		if(true) {
			for(uint64_t i = 0; i < nkeys; i++) {
				std::string *s = new std::string(YCSBRecordSize, 'a');
				table->TPut(i, (uint64_t *)s->data());
			}
		}

		//  printf("depth %d\n",((leveldb::MemstoreBPlusTree *)table)->depth);
		//  table->PrintStore();
		//  exit(0);

		void (Benchmark::*method)(ThreadState*) = NULL;
		method = &Benchmark::Mix;
		RunBenchmark(num_threads, num_, method);
		delete table;
	}

};




}  // namespace leveldb



int main(int argc, char** argv) {

	for(int i = 1; i < argc; i++) {

		int n;
		char junk;

		if(leveldb::Slice(argv[i]).starts_with("--benchmark=")) {
			FLAGS_benchmarks = argv[i] + strlen("--benchmark=");
		} else if(sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
			FLAGS_num = n;
		} else if(sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
			FLAGS_threads = n;
		}
	}

	leveldb::Benchmark benchmark;
	benchmark.Run();
//  while (1);
	return 1;
}

