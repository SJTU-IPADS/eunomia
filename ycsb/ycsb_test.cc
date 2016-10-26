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
#include <random>
#include "sys/types.h"
#include "sys/sysinfo.h"

static const int LENS = 100000;
static const char* FLAGS_benchmarks = "mix";

static int FLAGS_num = 10000000/10;
static int FLAGS_threads = 1;
static uint64_t nkeys = 10000000;
static uint64_t pre_keys = 100000;
static double READ_RATIO = 0.5;
static bool EUNO_USED = false;
static int CONT_SIZE = 20;
static double THETA = 0.01;
static int ENTITIES = 20;
static int LAMBDA = 1;

static double H_VALUE = 0.2;

static double DELTA = 1.0;
uint64_t next_o_id = 0;

static const double EE  = 2.71828;

#define CHECK 0
#define YCSBRecordSize 100
#define GETCOPY 0

enum dist_func {SEQT = 0, UNIF, NORM, CAUCHY, ZIPF, SELF, POISSON};

static dist_func FUNC_TYPE = SEQT;

namespace leveldb {

typedef uint64_t Key;

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

static inline ALWAYS_INLINE uint64_t* SequentialKeys(uint64_t* dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t start_id = dist_last_ids[0]++;

	fast_random r(start_id);
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	uint32_t upper_id = 1 * 10 + 1;

	uint64_t oid = static_cast<uint64_t>(upper_id) * 1000000 + static_cast<uint64_t>(start_id);
	for(int i = 0; i < CONT_SIZE; i++) {
		uint64_t id = oid * CONT_SIZE + i ;
		//printf("[%d]id = %lu\n",i, id);
		cont_window[i] = id;
	}
	return cont_window;
}

static inline ALWAYS_INLINE uint64_t* UniformKeys(uint64_t* dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t start_id = dist_last_ids[0]++;
	//std::random_device rd;
	//std::mt19937 gen(rd());

	fast_random r(start_id);
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	uint32_t upper_id = 1 * 10 + 1;

	uint64_t oid = static_cast<uint64_t>(upper_id) * 1000000 + static_cast<uint64_t>(start_id);
	for(int i = 0; i < CONT_SIZE; i++) {
		uint64_t id = oid * CONT_SIZE + static_cast<int>(r.next()) % CONT_SIZE ;
		//printf("[%d]id = %lu\n",i, id);
		cont_window[i] = id;
	}
	return cont_window;
}

static inline ALWAYS_INLINE uint64_t* NormalKeys(uint64_t* dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	std::random_device rd;
	std::mt19937 gen(rd());

	//std::default_random_engine generator;
	std::normal_distribution<double> distribution(CONT_SIZE / 2, DELTA);

	uint32_t upper_id =  1;
	uint64_t start_id = dist_last_ids[0]++;
	uint64_t oid = static_cast<uint64_t>(upper_id) * 10000000 + static_cast<uint64_t>(start_id);

	for(int i = 0; i < CONT_SIZE; i++) {
		int no = static_cast<int>(distribution(gen)) % CONT_SIZE;
		if(statistics != NULL) {
			statistics[no ] += 1;
		}
		uint64_t id = oid * CONT_SIZE + no;
		//printf("[%2d][%d]id = %lu, no = %d\n",sched_getcpu(), i, id, no);
		cont_window[i] = id;
		//printf("[%d]id = %lu\n", i, id);
		//uint64_t id = oid * CONT_SIZE + i;
		//cont_window[i] = id;
	}

	return cont_window;
}

static inline ALWAYS_INLINE uint64_t* CauchyKeys(uint64_t* dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));

	std::random_device rd;
	std::mt19937 gen(rd());
	//std::default_random_engine gen;
	std::cauchy_distribution<double> distribution(CONT_SIZE / 2, DELTA);

	uint32_t upper_id =  1;
	uint64_t start_id = dist_last_ids[0]++;
	uint64_t oid = static_cast<uint64_t>(upper_id) * 10000000 + static_cast<uint64_t>(start_id);

	for(int i = 0; i < CONT_SIZE; i++) {
		int ck = static_cast<int>(distribution(gen)); //% CONT_SIZE;
		uint64_t id = oid * CONT_SIZE + ck;
		//printf("[%2d][%d]id = %lu, ck = %d\n",sched_getcpu(), i, id, ck);

		cont_window[i] = id;
	}
	sort(cont_window, cont_window + CONT_SIZE);
	return cont_window;
}

static inline ALWAYS_INLINE double randf() {
	return static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
}

double zeta(long n, double theta) {
	int i;
	double ans = 0;
	for(i = 1; i <= n; i++) {
		ans += pow(1.0 / static_cast<double>(n), theta);
	}
	return ans;
}

long zipf(long n, double theta) {
	double tt = theta*1.2; 
	double alpha = 1 / (1 - tt);
	double zetan = zeta(n, tt);
	double eta = (1 - pow(2.0 / n, 1.0 - tt)) / (1.0 - zeta(2, tt) / zetan);
	double u = randf();
	double uz = u * zetan;
	if(uz < 1) return 1;
	if(uz < 1 + pow(0.5, tt)) return 2;
	return 1 + (long)(n * pow(eta * u - eta + 1, alpha));
}

struct probvals {
	float prob;                  /* the access probability */
	float cum_prob;              /* the cumulative access probability */
};

struct probvals* zdist = NULL;

struct probvals* get_zipf(float theta, int NUM) {
	double tt = theta*1.0;
	float sum = 0.0;
	float c = 0.0;
	float expo;
	float sumc = 0.0;
	int i;

	expo = tt;
	/*
	* zipfian - p(i) = c / i ^^ (theta) At x
	* = 1, uniform * at x = 0, pure zipfian
	*/
	struct probvals* dist = (struct probvals*)calloc(NUM, sizeof(struct probvals));
	for(i = 1; i <= NUM; i++) {
		sum += 1.0 / (float) pow((double) i, (double)(expo));
	}
	c = 1.0 / sum;

	for(i = 0; i < NUM; i++) {
		dist[i].prob = c /
					   (float) pow((double)(i + 1), (double)(expo));
		sumc +=  dist[i].prob;
		dist[i].cum_prob = sumc;
	}
	return dist;
}

static inline ALWAYS_INLINE uint64_t* ZipfKeys(uint64_t* dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	//printf("zdist = %p\n", zdist);
	double rnd = randf();
	//printf("rnd = %lf\n", rnd);
	int dist_num = 0;
	for(int j = 0; j < ENTITIES ; j++) {
		if(rnd < zdist[j].cum_prob) {
			dist_num = j;
			//printf("dist_num = %d\n", dist_num);
			break;
		}
	}
	uint32_t upper_id = 10 + dist_num;
	uint64_t last_o_id = dist_last_ids[dist_num]++; // __sync_fetch_and_add(&dist_o_id[dist_num], 1);
	uint64_t oid = static_cast<uint64_t>(upper_id) * 1000000 + static_cast<uint64_t>(last_o_id);

	for(int i = 0; i < CONT_SIZE; i++) {
		cont_window[i] = oid * CONT_SIZE + i;
		//printf("cont_window[%d] = %lu\n",i,cont_window[i]);
	}
	return cont_window;
}

static inline ALWAYS_INLINE int Selfsimilar(long n, double h) {
	return (static_cast<int>(n * pow(randf(), log(h) / log(1.0 - h))));
}
/*
static inline ALWAYS_INLINE uint64_t* ZipfianKeys(uint64_t * dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	int dist_num = zipf(ENTITIES, THETA);
	uint32_t upper_id = 10 + dist_num;
	uint64_t last_o_id = dist_last_ids[dist_num]++; // __sync_fetch_and_add(&dist_o_id[dist_num], 1);
	uint64_t oid = static_cast<uint64_t>(upper_id) * 1000000 + static_cast<uint64_t>(last_o_id);

	for(int i = 0; i < CONT_SIZE; i++) {
		cont_window[i] = oid * CONT_SIZE + i;
		//printf("cont_window[%d] = %lu\n",i,cont_window[i]);
	}
	return cont_window;
}
*/
static inline ALWAYS_INLINE uint64_t* SelfSimilarKeys(uint64_t * dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	int dist_num = Selfsimilar(ENTITIES, H_VALUE);
	uint32_t upper_id = 10 + dist_num;
	uint64_t last_o_id = dist_last_ids[dist_num]++; // __sync_fetch_and_add(&dist_o_id[dist_num], 1);
	uint64_t oid = static_cast<uint64_t>(upper_id) * 1000000 + static_cast<uint64_t>(last_o_id);
	if(statistics != NULL) {
		statistics[dist_num] += 1;
	}

	for(int i = 0; i < CONT_SIZE; i++) {
		cont_window[i] = oid * CONT_SIZE + i;
		//printf("cont_window[%d] = %lu\n",i,cont_window[i]);
	}
	return cont_window;
}

int Poisson(long lambda) {
	int n = 0;
	double c = pow(EE, -lambda);
	double p = 1.0;
	while(p >= c) {
		p = p * randf();
		n++;
	}
	return n - 1;
}

static inline ALWAYS_INLINE uint64_t* PoissonKeys(uint64_t * dist_last_ids, uint64_t * statistics = NULL) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	int dist_num = Poisson(LAMBDA);
	if(statistics != NULL) {
		statistics[dist_num] += 1;
	}
	if(dist_num >= ENTITIES) {
		dist_num = LAMBDA;
	}

	uint32_t upper_id = 10 + dist_num;
	uint64_t last_o_id = dist_last_ids[dist_num]++; // __sync_fetch_and_add(&dist_o_id[dist_num], 1);
	uint64_t oid = static_cast<uint64_t>(upper_id) * 1000000 + static_cast<uint64_t>(last_o_id);

	for(int i = 0; i < CONT_SIZE; i++) {
		cont_window[i] = oid * CONT_SIZE + i;
		//printf("cont_window[%d] = %lu\n",i,cont_window[i]);
	}
	return cont_window;
}

/*
static inline ALWAYS_INLINE uint64_t* ZipfKeys(int32_t start_id) {
	uint64_t* cont_window = (uint64_t*)calloc(CONT_SIZE, sizeof(uint64_t));
	uint32_t upper_id =  1;
	uint64_t oid = static_cast<uint64_t>(upper_id) * 10000000 + static_cast<uint64_t>(start_id);
	for(int i = 0; i < CONT_SIZE; i++) {
		long zf = zipf(CONT_SIZE, THETA)%CONT_SIZE;
		uint64_t id = oid * CONT_SIZE + zf;
		//printf("[%2d][%d]id = %lu zf = %ld\n",sched_getcpu(), i, id,zf);
		cont_window[i] = id;
	}
	sort(cont_window,cont_window+CONT_SIZE);
	return cont_window;
}
*/
class Benchmark {
	typedef uint64_t* (*Key_Generator)(uint64_t*, uint64_t*);

private:
	int64_t total_count;

	Memstore *table;

	uint64_t* statistics;

	port::SpinLock slock;

	Random ramdon;

	DBTables *store;
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

	void bind_cores(int tid) {
		cpu_set_t mask;
		CPU_ZERO(&mask);
		CPU_SET(tid , &mask);
		pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
	}
	//Main workload of YCSB
	void TxMix(ThreadState* thread) {
		int tid = thread->tid;

		bind_cores(tid);

		store->ThreadLocalInit(tid);
		int num = thread->count;

		int finish = 0 ;
		fast_random r = thread->rnd;
		std::string v;

		char nv[YCSBRecordSize];
		DBTX tx(store);
		tx.ThreadLocalInit();

		double start = leveldb::Env::Default()->NowMicros();
		int next_id = 3000;

		while(finish < num) {
			double d = r.next_uniform();
			if(d < READ_RATIO) {
				//printf("read begins\n");
				uint64_t key = r.next() % pre_keys;
				bool b = false;
				while(!b) {
					tx.Begin();
					uint64_t *s;
					tx.Get(0, key, &s);
#if GETCOPY
					std::string *p = &v;
					p->assign((char *)s, YCSBRecordSize);
#endif
					b = tx.End();
				}
				//printf("read ends\n");
				finish++;
			} else {
				//printf("write begins\n");
				uint64_t key = r.next() % nkeys;
				bool b = false;

				while(!b) {
					/*
					tx.Begin();
					uint64_t *s;

					tx.Get(0, key, &s);
					std::string c(YCSBRecordSize, 'c');
					//printf("[%d] write key = %lu\n", tid, key);
					tx.Add(0, key, (uint64_t *)c.data(), YCSBRecordSize);

					uint64_t * cont_keys = UniformKeys(next_id);
					next_id++;
					for(int idx = 0; idx < CONT_SIZE; idx++) {
						uint64_t key = cont_keys[idx];
						printf("[%d] write key = %lu\n", tid, key);
						std::string c(YCSBRecordSize, 'c');
						tx.Add(0, key, (uint64_t *)c.data(), YCSBRecordSize);
					}
					b = tx.End();
					*/
				}
				finish ++;
				//printf("write ends\n");
			}
		}

		double end = leveldb::Env::Default()->NowMicros();
		printf("Exe time: %f\n", (end - start) / 1000 / 1000);
		printf("Thread[%d] Op Throughput %lf ops/s\n", tid, num / ((end - start) / 1000 / 1000));
	}


	void Mix(ThreadState* thread) {
		int tid = thread->tid;

		bind_cores(tid);

		int num = thread->count;
		//printf("tid = %d, num = %ld\n",tid,num);
		int finish = 0 ;
		uint64_t* dist_o_id = (uint64_t*)calloc(ENTITIES, sizeof(uint64_t));//thread-local variable
		//int next_id = 3000;
		fast_random r = thread->rnd;
		double start = leveldb::Env::Default()->NowMicros();
		std::string v;
		char nv[YCSBRecordSize];
		
		//printf("READ_RATIO = %lf\n", READ_RATIO);
		while(finish < num) {
			double d = r.next_uniform();
			//Read
			if(d < READ_RATIO) {
				//printf("flag1\n");
				uint64_t key = r.next() % pre_keys;
				table->Get(key);
				//printf("flag2\n");
				finish += 1;
			}
			//Write
			else {
				uint64_t* cont_keys = key_generator(dist_o_id, statistics);
				for(int idx = 0; idx < CONT_SIZE; idx++) {
					
					uint64_t key = cont_keys[idx];
					//printf("[%2d] key = %lu\n", sched_getcpu(), key);
					Memstore::MemNode * mn = table->GetWithInsert(key).node;
					char *s = (char *)(mn->value);
					std::string c(YCSBRecordSize, 'c');
					memcpy(nv, c.data(), YCSBRecordSize);
					//table->Put(key, (uint64_t *)nv);
					mn = table->GetWithInsert(key).node;
					mn->value = (uint64_t *)(nv);
				}
				finish += CONT_SIZE;
				free(cont_keys);
			}
		}

		double end = leveldb::Env::Default()->NowMicros();
		//printf("Exe time: %f, total_num = %d\n", (end - start) / 1000 / 1000, finish);
		//printf("Thread[%d] Throughput %lf ops/s\n", tid, finish / ((end - start) / 1000 / 1000));
	}

	void ConsCheck(ThreadState* thread) {
		int tid = thread->tid;
		bind_cores(tid);
		int num = thread->count;
		int finish = 0 ;
		fast_random r = thread->rnd;
		r.set_seed(time(NULL));
		uint64_t* array = (uint64_t*)calloc(LENS, sizeof(uint64_t));

		for(int i = 0; i < LENS; i++){
			array[i] = r.next() ;
		}
		//table->Put(0,NULL);
		for(int i = 0; i < LENS; i++){
			table->Put(array[i], NULL); 	
		}
		printf("Put all done\n");
		for(int i = 0; i < LENS; i++){
			Memstore::MemNode* res = table->Get(array[i] );
			if(res == NULL){
				printf("[%d] key = %lu nonexist\n", i, array[i]);
				return;
			}
		}
		printf("Check Pass\n");
	}


public:

	Benchmark(): total_count(FLAGS_num), ramdon(1000) {}
	~Benchmark() {

	}
	void RunBenchmark(int thread_num, int num,
					  void (Benchmark::*method)(ThreadState*)) {
		SharedState shared;
		shared.total = thread_num;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		shared.num_half_done = 0;

//		double start = leveldb::Env::Default()->NowMicros();
		fast_random r(8544290);
		ThreadArg* arg = new ThreadArg[thread_num];
		for(int i = 0; i < thread_num; i++) {
			arg[i].bm = this;
			arg[i].method = method;
			arg[i].shared = &shared;
			arg[i].thread = new ThreadState(i, r.next());
			arg[i].thread->shared = &shared;
			arg[i].thread->count = num;
			Env::Default()->StartThread(ThreadBody, &arg[i]);
		}

		shared.mu.Lock();
		while(shared.num_initialized < thread_num) {
			shared.cv.Wait();
		}

		shared.start = true;
		printf("Send Start Signal\n");
		shared.cv.SignalAll();

		while(shared.num_done < thread_num) {
			shared.cv.Wait();
		}
		shared.mu.Unlock();

		printf("Total Run Time : %lf ms\n", (shared.end_time - shared.start_time) / 1000);
		printf("Total Throughput = %lf op/s\n", (double)(num * thread_num) / ((shared.end_time - shared.start_time) / 1000000));

		for(int i = 0; i < thread_num; i++) {
			delete arg[i].thread;
		}
		delete[] arg;
	}

	void Run() {
		//statistics = (uint64_t*)calloc(ENTITIES, sizeof(uint64_t));
		statistics = NULL;
		if(EUNO_USED) {
			printf("EunomiaTree\n");
			table = new leveldb::MemstoreEunoTree();
			printf("thread num:%d\n", FLAGS_threads);
			table -> set_thread_num(FLAGS_threads);
		} else {
			printf("B+Tree\n");
			table = new leveldb::MemstoreBPlusTree();
		}
		switch(FUNC_TYPE) {
		case SEQT:
			key_generator = SequentialKeys;
			printf("Sequential Distribution\n");
			break;
		case UNIF:
			key_generator = UniformKeys;
			printf("Uniform Distribution\n");
			break;
		case NORM:
			key_generator = NormalKeys;
			printf("Normal Distribution\n");
			break;
		case CAUCHY:
			key_generator = CauchyKeys;
			printf("Cauchy Distribution\n");
			break;
		case ZIPF:
			key_generator = ZipfKeys;
			if(zdist == NULL) {
				zdist = get_zipf(THETA, ENTITIES);
			}
			printf("Zipfian Distribution. Theta = %lf\n", THETA);
			break;
		case SELF:
			key_generator = SelfSimilarKeys;
			printf("SelfSimilar Distribution. H_VALUE = %lf\n", H_VALUE);
			break;
		case POISSON:
			key_generator = PoissonKeys;
			printf("Poisson Distribution. LAMBDA = %d\n", LAMBDA);
			break;
		}
		store = new DBTables();

		int num_threads = FLAGS_threads;
		int num_ = FLAGS_num;
		store->RCUInit(num_threads);
		Slice name = FLAGS_benchmarks;

		if(false) {
			for(uint64_t i = 1; i < pre_keys; i++) {
				std::string *s = new std::string(YCSBRecordSize, 'a');
				if(name == "txmix") {
					store->tables[0]->Put(i, (uint64_t *)s->data());
				} else {
					table->Put(i, (uint64_t *)s->data());
				}
			}
		}

		//  printf("depth %d\n",((leveldb::MemstoreBPlusTree *)table)->depth);
		//  table->PrintStore();
		//  exit(0);

		void (Benchmark::*method)(ThreadState*) = NULL;
		if(name == "mix") {
			method = &Benchmark::Mix;
		} else if(name == "txmix") {
			method = &Benchmark::TxMix;
		}else if (name=="check"){
			method = &Benchmark::ConsCheck;
		}
		printf("RunBenchmark Starts\n");
		RunBenchmark(num_threads, num_, method);
		//delete dynamic_cast<leveldb::MemstoreEunoTree*>(table);
		struct sysinfo memInfo;
		sysinfo (&memInfo);
	long long totalVirtualMem = memInfo.totalram;
	//Add other values in next statement to avoid int overflow on right hand side...
	totalVirtualMem += memInfo.totalswap;
	totalVirtualMem *= memInfo.mem_unit;
	long long virtualMemUsed = memInfo.totalram - memInfo.freeram;
	//Add other values in next statement to avoid int overflow on right hand side...
	virtualMemUsed += memInfo.totalswap - memInfo.freeswap;
	virtualMemUsed *= memInfo.mem_unit;
	printf("total:%lld, memory used:%lld\n", totalVirtualMem, virtualMemUsed);
		delete table;
		//delete store;
		printf("RunBenchmark Ends\n");
		uint64_t total = 0, topten = 0;
		/*
		for(int i = 0 ; i < ENTITIES; i++) {
			printf("statistics[%d] = %lu\n", i, statistics[i]);
			total += statistics[i];
		}
		
		for(int i = 0 ; i < ENTITIES / 10; i++) {
			//printf("statistics[%d] = %lu\n", i, statistics[i]);
			topten += statistics[i];
		}
		printf("total = %lu, topten = %lu\n", total, topten);
		*/

	}

};

}  // namespace leveldb

int main(int argc, char** argv) {
	srand(10000);
	//srand(time(NULL));
	for(int i = 1; i < argc; i++) {
		int n;
		char junk;
		double read_rate;
		int func_type;
		int cont_size;
		double theta;
		double h_value;
		if(leveldb::Slice(argv[i]).starts_with("--benchmark=")) {
			FLAGS_benchmarks = argv[i] + strlen("--benchmark=");
		} else if(sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
			FLAGS_num = n;
		} else if(sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
			FLAGS_threads = n;
		} else if(sscanf(argv[i], "--euno=%d%c", &n, &junk) == 1) {
			EUNO_USED = (n == 1);
		} else if(sscanf(argv[i], "--read-rate=%lf%c", &read_rate, &junk) == 1) {
			READ_RATIO = read_rate;
		} else if(sscanf(argv[i], "--func=%d%c", &func_type, &junk) == 1) {
			FUNC_TYPE = static_cast<dist_func>(func_type);
		} else if(sscanf(argv[i], "--cont-size=%d%c", &cont_size, &junk) == 1) {
			CONT_SIZE = cont_size;
		} else if(sscanf(argv[i], "--theta=%lf%c", &theta, &junk) == 1) {
			THETA = theta;
		} else if(sscanf(argv[i], "--hvalue=%lf%c", &h_value, &junk) == 1) {
			H_VALUE = h_value;
		}

	}
	printf("total_key_num = %d, threads = %d, read_rate = %lf, cont_size = %d\n", FLAGS_num*FLAGS_threads, FLAGS_threads, READ_RATIO, CONT_SIZE);
	leveldb::Benchmark benchmark;
	benchmark.Run();
	return 1;
}
