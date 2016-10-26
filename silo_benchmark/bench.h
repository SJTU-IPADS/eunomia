#ifndef _NDB_BENCH_H_
#define _NDB_BENCH_H_

#include <stdint.h>

#include <map>
#include <vector>
#include <utility>
#include <string>

#include "abstract_db.h"
#include "macros.h"
#include "thread.h"
#include "util.h"
#include "spinbarrier.h"
//#include "rcu.h"
#define MILLION 1000000L
#define TOTAL_CPUS_ONLINE 8

extern void ycsb_do_test(abstract_db *db, int argc, char **argv);
extern void tpcc_do_test(int argc, char **argv);
extern void articles_do_test(int argc, char **argv);

extern void queue_do_test(abstract_db *db, int argc, char **argv);
extern void encstress_do_test(abstract_db *db, int argc, char **argv);

enum {
  RUNMODE_TIME = 0,
  RUNMODE_OPS  = 1
};

// benchmark global variables
extern size_t nthreads;
extern volatile bool running;
extern int verbose;
extern uint64_t txn_flags;
extern double scale_factor;
extern uint64_t runtime;
extern uint64_t ops_per_worker;
extern int run_mode;
extern int enable_parallel_loading;
extern int pin_cpus;
extern int slow_exit;
extern int retry_aborted_transaction;
extern int no_reset_counters;
extern int backoff_aborted_transaction;

class scoped_db_thread_ctx {
	public:

  scoped_db_thread_ctx(const scoped_db_thread_ctx &) = delete;
  scoped_db_thread_ctx(scoped_db_thread_ctx &&) = delete;
  scoped_db_thread_ctx &operator=(const scoped_db_thread_ctx &) = delete;

  scoped_db_thread_ctx(abstract_db *db, bool loader)
    : db(db)
  {
    //db->thread_init(loader);
  }
  ~scoped_db_thread_ctx()
  {
    //db->thread_end();
  }
private:
  abstract_db *const db;
};

class bench_loader : public ndb_thread {
public:
  bench_loader(unsigned long seed, abstract_db *db)
    : r(seed), db(db),  b(0)
  {
    txn_obj_buf.reserve(2 * CACHELINE_SIZE);
  //  txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }
  inline void
  set_barrier(spin_barrier &b)
  {
    ALWAYS_ASSERT(!this->b);
    this->b = &b;
  }
  virtual void
  run()
  {
    { // XXX(stephentu): this is a hack
 //     scoped_rcu_region r; // register this thread in rcu region
    }
    ALWAYS_ASSERT(b);
    b->count_down();
    b->wait_for();
    scoped_db_thread_ctx ctx(db, true);
    load();
  }
protected:
  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  virtual void load() = 0;

  util::fast_random r;
  abstract_db *const db;
  spin_barrier *b;
  std::string txn_obj_buf;
//  str_arena arena;
};

class bench_worker : public ndb_thread {
public:
	uint64_t secs;
	static uint64_t total_ops;
	uint64_t txn_times[5];
  bench_worker(unsigned int worker_id,
               bool set_core_id,
               unsigned long seed, abstract_db *db,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : worker_id(worker_id), set_core_id(set_core_id),
      r(seed), db(db), 
      barrier_a(barrier_a), barrier_b(barrier_b),
      // the ntxn_* numbers are per worker
      latency_numer_us(0),
      backoff_shifts(0), // spin between [0, 2^backoff_shifts) times before retry
      size_delta(0)
  {
    txn_obj_buf.reserve(2 * CACHELINE_SIZE);
	for (int i = 0 ;i < 5; i++) {
		ntxn_commits[i] = 0;
		ntxn_aborts[i] = 0;
		txn_times[i] = 0;
	}
 //   txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }
  void set_cpu_id(int cpuid){cpu_id = cpuid;} 
  int get_cpu_id(){return cpu_id;} 

  virtual ~bench_worker() {
	
  	//printf("[Alex] ~bench_worker\n");
  }
  // returns [did_commit?, size_increase_bytes]
  typedef std::pair<bool, ssize_t> txn_result;
  typedef txn_result (*txn_fn_t)(bench_worker *, bool first_run);

  struct workload_desc {
    workload_desc() {}
    workload_desc(const std::string &name, double frequency, txn_fn_t fn)
      : name(name), frequency(frequency), fn(fn)
    {
      ALWAYS_ASSERT(frequency > 0.0);
      ALWAYS_ASSERT(frequency <= 1.0);
    }
    std::string name;
    double frequency; //frequency
    txn_fn_t fn; //handler
  };
  typedef std::vector<workload_desc> workload_desc_vec;
  virtual workload_desc_vec get_workload() const = 0;

  virtual void run();

  inline size_t get_mixed_commits() const { 
	size_t all_commits = 0;
	for(int i = 0; i < 5; i++){
		all_commits += ntxn_commits[i];
	}
	return all_commits; 
  }
  inline size_t get_new_order_commits() const{
	return ntxn_commits[0];
  }
  inline size_t get_ntxn_aborts(int i) const { return ntxn_aborts[i]; }

  inline uint64_t get_latency_numer_us() const { return latency_numer_us; }

  inline double
  get_avg_latency_us() const
  {
    return double(latency_numer_us) / double(get_mixed_commits());
  }

  std::map<std::string, size_t> get_txn_counts() const;

  typedef abstract_db::counter_map counter_map;
  typedef abstract_db::txn_counter_map txn_counter_map;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  inline txn_counter_map
  get_local_txn_counters() const
  {
    return local_txn_counters;
  }
#endif

  inline ssize_t get_size_delta() const { return size_delta; }

protected:

  virtual void on_run_setup() {}

  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  unsigned int worker_id;
  bool set_core_id;
  util::fast_random r;
  abstract_db *const db;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;

private:
  size_t ntxn_commits[5];
  size_t ntxn_aborts[5];
  uint64_t latency_numer_us;
  unsigned backoff_shifts;

protected:

#ifdef ENABLE_BENCH_TXN_COUNTERS
  txn_counter_map local_txn_counters;
  void measure_txn_counters(void *txn, const char *txn_name);
#else
  inline ALWAYS_INLINE void measure_txn_counters(void *txn, const char *txn_name) {}
#endif

  std::vector<size_t> txn_counts; // breakdown of txns
  ssize_t size_delta; // how many logical bytes (of values) did the worker add to the DB

  std::string txn_obj_buf;
  int cpu_id;
//  str_arena arena;
};

class bench_runner {
public:
  bench_runner(const bench_runner &) = delete;
  bench_runner(bench_runner &&) = delete;
  bench_runner &operator=(const bench_runner &) = delete;
  
  bench_runner(abstract_db *db)
    : db(db), barrier_a(nthreads), barrier_b(1) {}
  virtual ~bench_runner() {}
  void run();
  
protected:
  // only called once
  virtual std::vector<bench_loader*> make_loaders() = 0;

  // only called once
  virtual std::vector<bench_worker*> make_workers() = 0;
  virtual void sync_log() = 0;
  virtual void initPut() = 0;
  virtual void  print_persisted_info() = 0;
  abstract_db *const db;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};
#endif /* _NDB_BENCH_H_ */
