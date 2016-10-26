#define __STDC_FORMAT_MACROS
#include <climits>
#include <cstdio>
#include <inttypes.h>
#include <string>
#include <sched.h>

#include "leveldb/env.h"
#include "port/port_posix.h"
#include "leveldb/slice.h"

#include "tpcc/clock.h"
#include "tpcc/randomgenerator.h"
#include "tpcc/tpccclient.h"
#include "tpcc/tpccgenerator.h"
#include "tpcc/tpcctables.h"
//#include "tpcc/tpccleveldb.h"
//#include "tpcc/tpcctxmemstore.h"
#include "tpcc/tpccskiplist.h"

static int NUM_TRANSACTIONS = 100000;
static int NUM_WAREHOUSE = 1;

#define LOCALRANDOM 0
#define SHAREWAREHOUSE 0
#define SETAFFINITY	1

namespace leveldb {



class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = leveldb::Env::Default()->NowMicros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = leveldb::Env::Default()->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp() {
    
    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      //fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      //fflush(stderr);
    }
  }

  void Report(const leveldb::Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    fprintf(stdout, "%-12s : %11.3f micros/op;\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_);
    
    fflush(stdout);
  }
};

struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
  
  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index) {
  }
};

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
  TPCCDB* tables;
  SystemClock* clock;
  tpcc::NURandC cLoad;
  int64_t total_count;
 
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

 public:

  Benchmark(TPCCDB* t, SystemClock* c, tpcc::NURandC cl){
  	 tables = t;
	 clock = c;
	 cLoad = cl;
  }


  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
#if SETAFFINITY	
	int x = thread->tid;
//	if (x == 0) x = 1;
//	else if (x == 1) x = 6;
	cpu_set_t  mask;
    CPU_ZERO(&mask);
    CPU_SET(x, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
#endif	
	
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
  
    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();
    
    {
  	  MutexLock l(&shared->mu);
  	  shared->num_done++;
  	  if (shared->num_done >= shared->total) {
  	    shared->cv.SignalAll();
  	  }
  	}
  }
  

  void RunBenchmark(int n, Slice name, void (Benchmark::*method)(ThreadState*)) {
  	
	printf("Running... \n");
	total_count = NUM_TRANSACTIONS;
	
	SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

	double start_ = leveldb::Env::Default()->NowMicros();
    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();
	double end_ = leveldb::Env::Default()->NowMicros() - start_;

	
  	((leveldb::TPCCSkiplist*)tables)->store->Sync();

	double syncend_ = leveldb::Env::Default()->NowMicros() - start_;
	
    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    //arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

	printf("Throughput %g tx/sec\n", NUM_TRANSACTIONS / end_  * 1e6);
	printf("Sync Throughput %g tx/sec\n", NUM_TRANSACTIONS / syncend_  * 1e6);
	//printf("Write records %d . Read records %d .", tables->wcount, tables->rcount );
  }

  void doOne(ThreadState* thread) {
  	((leveldb::TPCCSkiplist*)tables)->ThreadLocalInit();
	((leveldb::TPCCSkiplist*)tables)->store->ThreadLocalInit(thread->tid);
	printf("thread %d\n",thread->tid);
  	// Change the constants for run
#if TIMEPROFILE
	double start_time = leveldb::Env::Default()->NowMicros();
#endif

#if LOCALRANDOM
	tpcc::TXDBRandomGenerator* random = new tpcc::TXDBRandomGenerator();
#else
    tpcc::RealRandomGenerator* random = new tpcc::RealRandomGenerator();
#endif
    random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));
	random->seed(0xdeadbeef + thread->tid << 10);
    // Client owns all the parameters
    TPCCClient client(clock, random, tables, Item::NUM_ITEMS, static_cast<int>(NUM_WAREHOUSE),
            District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT);
#if SHAREWAREHOUSE
	client.bindWarehouseDistrict(thread->tid/2 + 1, 0);
#else
	client.bindWarehouseDistrict(thread->tid + 1, 0);
#endif	
    //for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
    while (total_count > 0) {
	  int64_t oldv = XADD64(&total_count, -1000);
  	  if(oldv <= 0) break;
	  
	  for (int i =0; i < 1000; i++) {	
        client.doOne();
		thread->stats.FinishedSingleOp();
	  }
    }
	
#if TIMEPROFILE
	double stop_time = leveldb::Env::Default()->NowMicros();
	printf("Mix run time : %lf\n", (stop_time - start_time)/1000);
	client.reportStat();
#endif

  }

  void doNewOrder(ThreadState* thread) {
  	((leveldb::TPCCSkiplist*)tables)->ThreadLocalInit();
  	((leveldb::TPCCSkiplist*)tables)->store->ThreadLocalInit(thread->tid);
  	// Change the constants for run
#if LOCALRANDOM
		tpcc::TXDBRandomGenerator* random = new tpcc::TXDBRandomGenerator();
#else
		tpcc::RealRandomGenerator* random = new tpcc::RealRandomGenerator();
#endif
    random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));
	random->seed(0xdeadbeef + thread->tid << 10);
    // Client owns all the parameters
    TPCCClient client(clock, random, tables, Item::NUM_ITEMS, static_cast<int>(NUM_WAREHOUSE),
            District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT);
#if SHAREWAREHOUSE
	client.bindWarehouseDistrict(thread->tid/2 + 1, 0);
#else
	client.bindWarehouseDistrict(thread->tid + 1, 0);
#endif	
	
    //for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
    while (total_count > 0) {
	  int64_t oldv = XADD64(&total_count, -1000);
  	  if(oldv <= 0) break;
	  
	  for (int i =0; i < 1000; i++) {	
        client.doNewOrder();
        //client.doPayment();
		thread->stats.FinishedSingleOp();
	  }
    }
	//printf("rdtsc %ld\n", ((leveldb::TPCCSkiplist*)tables)->secs);
  }

  
  void doReadOnly(ThreadState* thread) {
  	  ((leveldb::TPCCSkiplist*)tables)->ThreadLocalInit();
  	  ((leveldb::TPCCSkiplist*)tables)->store->ThreadLocalInit(thread->tid);
	  // Change the constants for run
#if LOCALRANDOM
		  tpcc::TXDBRandomGenerator* random = new tpcc::TXDBRandomGenerator();
#else
		  tpcc::RealRandomGenerator* random = new tpcc::RealRandomGenerator();
#endif
	  random->setC(tpcc::NURandC::makeRandomForRun(random, cLoad));
  	  random->seed(0xdeadbeef + thread->tid << 10);
	  // Client owns all the parameters
	  TPCCClient client(clock, random, tables, Item::NUM_ITEMS, static_cast<int>(NUM_WAREHOUSE),
			  District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT);
#if SHAREWAREHOUSE
	  client.bindWarehouseDistrict(thread->tid/2 + 1, 0);
#else
	  client.bindWarehouseDistrict(thread->tid + 1, 0);
#endif	
	  
	  //for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
	  while (total_count > 0) {
		int64_t oldv = XADD64(&total_count, -1000);
		if(oldv <= 0) break;
		
		for (int i =0; i < 1000; i++) {   
		  client.doReadOnly();
		  
		  //client.doStockLevel();
		  thread->stats.FinishedSingleOp();
		}
	  }
	//  printf("%ld\n", ((leveldb::TPCCSkiplist*)tables)->secs);
	}
};
}
int main(int argc, const char* argv[]) {
/*	cpu_set_t  mask;
    CPU_ZERO(&mask);
    CPU_SET(0, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
	*/
	long num_warehouses = 1;
	const char* benchmark = "mix";
	for (int i = 1; i < argc; i++) {
		int n;
		char junk;
		if (sscanf(argv[i], "--numwarehouse=%d%c", &n, &junk) == 1) {
		   	num_warehouses = n;
	 	}
		else if (leveldb::Slice(argv[i]).starts_with("--benchmark=")) {
		   	benchmark = argv[i] + strlen("--benchmark=");
		}
		else if (sscanf(argv[i], "--numtx=%d%c", &n, &junk) == 1) {
		   	NUM_TRANSACTIONS = n;
	 	}
		else if (leveldb::Slice(argv[i]).starts_with("--help")){
			fprintf(stderr, "./neworder [--numwarehouse=...] [--benchmark=...] [--numtx=...]\n");
        	exit(1);
		}
	}
	
    
    if (num_warehouses == LONG_MIN || num_warehouses == LONG_MAX) {
        fprintf(stderr, "Bad warehouse number (%ld)\n", num_warehouses);
        exit(1);
    }
    if (num_warehouses <= 0) {
        fprintf(stderr, "Number of warehouses must be > 0 (was %ld)\n", num_warehouses);
        exit(1);
    }
    if (num_warehouses > Warehouse::MAX_WAREHOUSE_ID) {
        fprintf(stderr, "Number of warehouses must be <= %d (was %ld)\n", Warehouse::MAX_WAREHOUSE_ID, num_warehouses);
        exit(1);
    }

    NUM_WAREHOUSE = num_warehouses;
	
    //TPCCTables* tables = new TPCCTables();
    //leveldb::TPCCLevelDB* tables = new leveldb::TPCCLevelDB();
    //TPCCDB* tables = new leveldb::TPCCSkiplist();
    leveldb::TPCCSkiplist* tables = new leveldb::TPCCSkiplist();
	tables->store->RCUInit(NUM_WAREHOUSE);
	tables->store->PBufInit(NUM_WAREHOUSE);
    SystemClock* clock = new SystemClock();

    // Create a generator for filling the database.
#if LOCALRANDOM
		tpcc::TXDBRandomGenerator* random = new tpcc::TXDBRandomGenerator();
#else
		tpcc::RealRandomGenerator* random = new tpcc::RealRandomGenerator();
#endif
    tpcc::NURandC cLoad = tpcc::NURandC::makeRandom(random);
    random->setC(cLoad);

    // Generate the data
    printf("Loading %ld warehouses... ", num_warehouses);
    fflush(stdout);
    char now[Clock::DATETIME_SIZE+1];
    clock->getDateTimestamp(now);
    TPCCGenerator generator(random, now, Item::NUM_ITEMS, District::NUM_PER_WAREHOUSE,
            Customer::NUM_PER_DISTRICT, NewOrder::INITIAL_NUM_PER_DISTRICT);
    int64_t begin = clock->getMicroseconds();
    generator.makeItemsTable(tables);
    for (int i = 0; i < num_warehouses; ++i) {
        generator.makeWarehouse(tables, i+1);
    }
    int64_t end = clock->getMicroseconds();
    printf("%" PRId64" ms\n", (end - begin + 500)/1000);

	//tables->printSkiplist();
	
    leveldb::Slice name(benchmark);
    leveldb::Benchmark b(tables, clock, cLoad);

	if (name == leveldb::Slice("mix")) 
		b.RunBenchmark(num_warehouses, name, &leveldb::Benchmark::doOne);
	else if (name == leveldb::Slice("neworder"))
		b.RunBenchmark(num_warehouses, name, &leveldb::Benchmark::doNewOrder);
	else if (name == leveldb::Slice("readonly"))
		b.RunBenchmark(num_warehouses, name, &leveldb::Benchmark::doReadOnly);

	//tables->printSkiplist();
	
	delete tables;
	printf("Hello World\n");
    return 0;
}

