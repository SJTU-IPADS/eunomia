#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/sysinfo.h>
#include "util/numa_util.h"
#include "bench.h"
//#include "base_txn_btree.h"
//#include "counter.h"
#include "scopedperf.hh"
//#include "allocator.h"
#include "persistent/pbuf.h"
#define SET_AFFINITY 1

#define blue_cerr cerr << "\e[44m"
#define blue_endl "\e[0m"<<endl


#ifdef USE_JEMALLOC
//cannot include this header b/c conflicts with malloc.h
//#include <jemalloc/jemalloc.h>
extern "C" void malloc_stats_print(void (*write_cb)(void *, const char *), void *cbopaque, const char *opts);
extern "C" int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);
#endif
#ifdef USE_TCMALLOC
#include <google/heap-profiler.h>
#endif

using namespace std;
using namespace util;

size_t nthreads = 1;
volatile bool running = true;
int verbose = 0;
uint64_t txn_flags = 0;
double scale_factor = 1.0;
uint64_t runtime = 30;
uint64_t ops_per_worker = 0;
int run_mode = RUNMODE_TIME;
int enable_parallel_loading = false;
int pin_cpus = 0;
int slow_exit = 0;
int retry_aborted_transaction = 0;
int no_reset_counters = 0;
int backoff_aborted_transaction = 0;

template <typename T>
static void
delete_pointers(const vector<T *> &pts) {
	for(size_t i = 0; i < pts.size(); i++)
		delete pts[i];
}

template <typename T>
static vector<T>
elemwise_sum(const vector<T> &a, const vector<T> &b) {
	INVARIANT(a.size() == b.size());
	vector<T> ret(a.size());
	for(size_t i = 0; i < a.size(); i++)
		ret[i] = a[i] + b[i];
	return ret;
}

template <typename K, typename V>
static void
map_agg(map<K, V> &agg, const map<K, V> &m) {
	for(typename map<K, V>::const_iterator it = m.begin();
			it != m.end(); ++it)
		agg[it->first] += it->second;
}

// returns <free_bytes, total_bytes>
static pair<uint64_t, uint64_t>
get_system_memory_info() {
	struct sysinfo inf;
	sysinfo(&inf);
	return make_pair(inf.mem_unit * inf.freeram, inf.mem_unit * inf.totalram);
}

static bool
clear_file(const char *name) {
	ofstream ofs(name);
	ofs.close();
	return true;
}

static void
write_cb(void *p, const char *s) UNUSED;
static void
write_cb(void *p, const char *s) {
	const char *f = "jemalloc.stats";
	static bool s_clear_file UNUSED = clear_file(f);
	ofstream ofs(f, ofstream::app);
	ofs << s;
	ofs.flush();
	ofs.close();
}


//static event_avg_counter evt_avg_abort_spins("avg_abort_spins");
__inline__ int64_t XADD64(uint64_t* addr, int64_t val) {
	asm volatile(
		"lock;xaddq %0, %1"
		: "+a"(val), "+m"(*addr)
		:
		: "cc");

	return val;
}


uint64_t bench_worker::total_ops = 0;
void
bench_worker::run() {
	// XXX(stephentu): so many nasty hacks here. should actually
	// fix some of this stuff one day
	//printf("worker id %d\n", worker_id);

#if SET_AFFINITY
	int x = worker_id;
	int y = x - TOTAL_CPUS_ONLINE;
	if(y >= 20) {
		fprintf(stderr, "[Alex]Number of workers should be < %d\n", 20);
	}
	/*
	if (nthreads == 8 || nthreads == 7) {
	 	if (x == 8) y = 0;
		else if (x == 9) y = 2;
		else if (x == 10) y = 4;
		else if (x == 11) y = 6;
		else if (x == 12) y = 1;
		else if (x == 13) y = 3;
	else if (x == 14) y = 5;
	else if (x == 15) y = 7;
	 }
	else if (nthreads == 6) {
		if (x == 8) y = 0;
		else if (x == 9) y = 2;
		else if (x == 10) y = 4;
	else if (x == 11) y = 1;
		else if (x == 12) y = 3;
	else if (x == 13) y = 5;
	}
	*/
	int socket_0[] =      {0, 2, 4, 6, 8, 10, 12, 14, 16, 18};
	int shared_cores[] =  {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18};
	int mixed_sockets[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
	int cross_sockets[] = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
	//int core_id = socket_0[y];
	//int core_id = cross_sockets[y];
	int core_id = mixed_sockets[y];
	//int core_id = shared_cores[y];
	
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(core_id , &mask);
	this->set_cpu_id(core_id);
	fprintf(stderr, "[Alex]worker_id = %2d core_id = %2d\n", worker_id, core_id);
	pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
#endif
#if 0
	if(set_core_id)
		coreid::set_core_id(worker_id); // cringe
	{
//    scoped_rcu_region r; // register this thread in rcu region
	}
#endif
	on_run_setup();
	scoped_db_thread_ctx ctx(db, false);
	const workload_desc_vec workload = get_workload();
	txn_counts.resize(workload.size());
	//printf("workload.size() = %d\n", workload.size());
	//txn_times = (uint64_t*)calloc(workload.size(), sizeof(uint64_t));
	for(int i = 0; i < 5; i++){
		txn_times[i]=0;
	}
	barrier_a->count_down();
	barrier_b->wait_for();
	//struct timespec begin, end;
	//clock_gettime(CLOCK_MONOTONIC, &begin);
	//uint64_t txn_span = 0;
	//printf("[Alex]running begins\n");
	while(running && (run_mode != RUNMODE_OPS || total_ops > 0)) {
		int64_t oldv = XADD64(&total_ops, -1000);
		if(oldv <= 0) break;
		for(int s = 0; s < 1000; s++) {
			//a new transaction begins here
			double d = r.next_uniform(); //generate a new random seed
			for(size_t i = 0; i < workload.size(); i++) {
				if((i + 1) == workload.size() || d < workload[i].frequency) {
					unsigned workload_index = i;
					// TODO: temporarily skip the last two txn types
					if(i > 2){ workload_index = 0;} 
					bool first_run = true;
retry:
					//reload the stored seed => the retry transaction is consistent with the aborted one
					const unsigned long old_seed = r.get_seed(); 
					//timer t;
					//clock_gettime(CLOCK_MONOTONIC, &begin);
					const txn_result ret = workload[workload_index].fn(this, first_run); //execute the transaction
					//atomic_add64(&txn_times[i], t.lap());
					//clock_gettime(CLOCK_MONOTONIC, &end);
					//txn_span += get_nanoseconds(begin, end);
					if(likely(ret.first)) { //whether this txn commits successfully
						++ntxn_commits[workload_index]; 
						//latency_numer_us += t.lap();
						backoff_shifts >>= 1;
					} else { //this txn aborts
						++ntxn_aborts[workload_index];
						if(retry_aborted_transaction && running) {
							if(backoff_aborted_transaction) {
								if(backoff_shifts < 63)
									backoff_shifts++;
								uint64_t spins = 1UL << backoff_shifts;
								spins *= 100; // XXX: tuned pretty arbitrarily
								//evt_avg_abort_spins.offer(spins);
								while(spins) {
									nop_pause();
									spins--;
								}
							}
							r.set_seed(old_seed); //store the old seed for retry
							first_run = false;
							goto retry;
						}
					}
					size_delta += ret.second; // should be zero on abort
					txn_counts[workload_index]++; // txn_counts aren't used to compute throughput (is
					// just an informative number to print to the console
					// in verbose mode)
					break;
				}
				d -= workload[i].frequency;
			}
		}
	}
	//printf("------cascading------\n");
	//clock_gettime(CLOCK_MONOTONIC, &end);
	//uint64_t span = get_nanoseconds(begin, end);
	//printf("workload span = %ld\n", txn_span);
	/*
	for(int i = 0; i < 5; i++){
		printf("txn[%d] time = %lf\n", i, (double)txn_times[i]/MILLION);
	}
	*/
}


void
bench_runner::run() {
	// load data
	const vector<bench_loader *> loaders = make_loaders();
	{
		spin_barrier b(loaders.size());
		const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
		{
			scoped_timer t("dataloading", verbose);
			for(vector<bench_loader *>::const_iterator it = loaders.begin();
					it != loaders.end(); ++it) {
				(*it)->set_barrier(b);
				//cerr << "Thread started here" << endl;
				(*it)->start();
			}
			for(vector<bench_loader *>::const_iterator it = loaders.begin();
					it != loaders.end(); ++it)
				(*it)->join();
		}
		const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
		const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
		const double delta_mb = double(delta) / 1048576.0;
		if(verbose)
			cerr << "DB size: " << delta_mb << " MB" << endl;
	}

	initPut();
#if 0
	db->do_txn_epoch_sync(); // also waits for worker threads to be persisted
	{
		const auto persisted_info = db->get_ntxn_persisted();
		if(get<0>(persisted_info) != get<1>(persisted_info))
			cerr << "ERROR: " << persisted_info << endl;
		//ALWAYS_ASSERT(get<0>(persisted_info) == get<1>(persisted_info));
		if(verbose)
			cerr << persisted_info << " txns persisted in loading phase" << endl;
	}
	db->reset_ntxn_persisted();

	if(!no_reset_counters) {
		event_counter::reset_all_counters(); // XXX: for now - we really should have a before/after loading
		PERF_EXPR(scopedperf::perfsum_base::resetall());
	}

	{
		const auto persisted_info = db->get_ntxn_persisted();
		if(get<0>(persisted_info) != 0 ||
				get<1>(persisted_info) != 0 ||
				get<2>(persisted_info) != 0.0) {
			cerr << persisted_info << endl;
			ALWAYS_ASSERT(false);
		}
	}
#endif
	map<string, size_t> table_sizes_before;
	if(verbose) {
#if 0
		for(map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
				it != open_tables.end(); ++it) {
			scoped_rcu_region guard;
			const size_t s = it->second->size();
			cerr << "table " << it->first << " size " << s << endl;
			table_sizes_before[it->first] = s;
		}
#endif
		cerr << "starting benchmark..." << endl;
	}

	bench_worker::total_ops = ops_per_worker * nthreads;
	const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();

	const vector<bench_worker *> workers = make_workers();
	ALWAYS_ASSERT(!workers.empty());
	for(vector<bench_worker *>::const_iterator it = workers.begin();
			it != workers.end(); ++it)
		(*it)->start();

	barrier_a.wait_for(); // wait for all threads to start up
	timer t, t_nosync;
	barrier_b.count_down(); // bombs away!
	if(run_mode == RUNMODE_TIME) {
		sleep(runtime);
		running = false;
	}
	__sync_synchronize();
	
	for(size_t i = 0; i < nthreads; i++)
		workers[i]->join();
	const unsigned long elapsed_nosync = t_nosync.lap();

	//db->do_txn_finish(); // waits for all worker txns to persist
	sync_log();

	const unsigned long elapsed = t.lap(); // lap() must come after do_txn_finish(),
	// because do_txn_finish() potentially
	// waits a bit
	size_t n_commits = 0;
	size_t n_new_order_commits = 0;
	size_t n_aborts[5];
	for(size_t i = 0; i < 5; i++){
		n_aborts[i] = 0;
	}
	uint64_t latency_numer_us = 0;
	for(size_t i = 0; i < nthreads; i++) {
		n_new_order_commits += workers[i]->get_new_order_commits();
		n_commits += workers[i]->get_mixed_commits(); //the throughput of all txns 
		for(size_t j = 0; j < 5; j++)
			n_aborts[j] += workers[i]->get_ntxn_aborts(j);
		latency_numer_us += workers[i]->get_latency_numer_us();
	}
//  const auto persisted_info = db->get_ntxn_persisted();
	print_persisted_info();

	// various sanity checks
//  ALWAYS_ASSERT(get<0>(persisted_info) == get<1>(persisted_info));
	// not == b/c persisted_info does not count read-only txns
//  ALWAYS_ASSERT(n_commits >= get<1>(persisted_info));

	const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
	const double agg_nosync_throughput = double(n_commits) / elapsed_nosync_sec;
	const double agg_new_order_throughput = double(n_new_order_commits)/elapsed_nosync_sec;
	const double avg_nosync_per_core_throughput = agg_nosync_throughput / double(workers.size());

	const double elapsed_sec = double(elapsed) / 1000000.0;
	const double agg_throughput = double(n_commits) / elapsed_sec;
	const double avg_per_core_throughput = agg_throughput / double(workers.size());

	//const double agg_abort_rate = double(n_aborts) / elapsed_sec;
	//const double avg_per_core_abort_rate = agg_abort_rate / double(workers.size());

	// we can use n_commits here, because we explicitly wait for all txns
	// run to be durable
	const double agg_persist_throughput = double(n_commits) / elapsed_sec;
	const double avg_per_core_persist_throughput =
		agg_persist_throughput / double(workers.size());

	// XXX(stephentu): latency currently doesn't account for read-only txns
	const double avg_latency_us =
		double(latency_numer_us) / double(n_commits);
	const double avg_latency_ms = avg_latency_us / 1000.0;
	const double avg_persist_latency_ms = 0;
//    get<2>(persisted_info) / 1000.0;

	if(verbose) {
		const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
		const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
		const double delta_mb = double(delta) / 1048576.0;
		map<string, size_t> agg_txn_counts = workers[0]->get_txn_counts();
		ssize_t size_delta = workers[0]->get_size_delta();
		for(size_t i = 1; i < workers.size(); i++) {
			map_agg(agg_txn_counts, workers[i]->get_txn_counts());
			size_delta += workers[i]->get_size_delta();
		}
		const double size_delta_mb = double(size_delta) / 1048576.0;
//    map<string, counter_data> ctrs = event_counter::get_all_counters();

		cerr << "--- table statistics ---" << endl;
#if 0
		for(map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
				it != open_tables.end(); ++it) {
			scoped_rcu_region guard;
			const size_t s = it->second->size();
			const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
			cerr << "table " << it->first << " size " << it->second->size();
			if(delta < 0)
				cerr << " (" << delta << " records)" << endl;
			else
				cerr << " (+" << delta << " records)" << endl;
		}
#endif
#ifdef ENABLE_BENCH_TXN_COUNTERS
		cerr << "--- txn counter statistics ---" << endl;
		{
			// take from thread 0 for now
			abstract_db::txn_counter_map agg = workers[0]->get_local_txn_counters();
			for(auto &p : agg) {
				cerr << p.first << ":" << endl;
				for(auto &q : p.second)
					cerr << "  " << q.first << " : " << q.second << endl;
			}
		}
#endif
		cerr << "--- benchmark statistics ---" << endl;
		blue_cerr << "runtime: " << elapsed_sec << " sec" << blue_endl;
		cerr << "memory delta: " << delta_mb  << " MB" << endl;
		cerr << "memory delta rate: " << (delta_mb / elapsed_sec)  << " MB/sec" << endl;
		cerr << "logical memory delta: " << size_delta_mb << " MB" << endl;
		cerr << "logical memory delta rate: " << (size_delta_mb / elapsed_sec) << " MB/sec" << endl;
		blue_cerr << "agg_nosync_mixed_throughput: " << agg_nosync_throughput << " ops/sec" << blue_endl;
		cerr << "agg_nosync_new_order_throughput: " << agg_new_order_throughput << " ops/sec" << endl;
		cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput << " ops/sec/core" << endl;
//    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
//    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
		cerr << "agg_persist_throughput: " << agg_persist_throughput << " ops/sec" << endl;
		cerr << "avg_per_core_persist_throughput: " << avg_per_core_persist_throughput << " ops/sec/core" << endl;
		blue_cerr << "avg_latency: " << avg_latency_ms << " ms" << blue_endl;
//    cerr << "avg_persist_latency: " << avg_persist_latency_ms << " ms" << endl;

		int totalabort = 0;

		for(int i = 0; i < 5; i++) {
			totalabort += n_aborts[i];
			cerr << "workload[" << i << "] agg_abort_num: " << n_aborts[i] << endl;
		}

		blue_cerr << "total_abort_num: " << totalabort << blue_endl;
		blue_cerr << "total_commit_num: " << n_commits << blue_endl;
		const double agg_abort_rate = double(totalabort) / elapsed_sec;
		cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
//    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
		cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(), agg_txn_counts.end()) << endl;
#if 0
		cerr << "--- system counters (for benchmark) ---" << endl;
		for(map<string, counter_data>::iterator it = ctrs.begin();
				it != ctrs.end(); ++it)
			cerr << it->first << ": " << it->second << endl;
#endif
		cerr << "--- perf counters (if enabled, for benchmark) ---" << endl;
		PERF_EXPR(scopedperf::perfsum_base::printall());
		//  cerr << "--- allocator stats ---" << endl;
		// ::allocator::DumpStats();
		cerr << "---------------------------------------" << endl;

#ifdef USE_JEMALLOC
		cerr << "dumping heap profile..." << endl;
		mallctl("prof.dump", NULL, NULL, NULL, 0);
		cerr << "printing jemalloc stats..." << endl;
		malloc_stats_print(write_cb, NULL, "");
#endif
#ifdef USE_TCMALLOC
		HeapProfilerDump("before-exit");
#endif
	}

	// output for plotting script
	/*cout << agg_nosync_throughput << " "
		 << agg_persist_throughput << " "
		 << elapsed_sec <<  endl;
	*/
	cout.flush();

	//if(!slow_exit)
	//return;

	map<string, uint64_t> agg_stats;
#if 0
	for(map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
			it != open_tables.end(); ++it) {
		map_agg(agg_stats, it->second->clear());
		delete it->second;
	}
#endif
	if(verbose) {
		for(auto &p : agg_stats)
			cerr << p.first << " : " << p.second << endl;

	}
//  open_tables.clear();

	delete_pointers(loaders);
	delete_pointers(workers);
}

template <typename K, typename V>
struct map_maxer {
	typedef map<K, V> map_type;
	void
	operator()(map_type &agg, const map_type &m) const {
		for(typename map_type::const_iterator it = m.begin();
				it != m.end(); ++it)
			agg[it->first] = std::max(agg[it->first], it->second);
	}
};

//template <typename KOuter, typename KInner, typename VInner>
//struct map_maxer<KOuter, map<KInner, VInner>> {
//  typedef map<KInner, VInner> inner_map_type;
//  typedef map<KOuter, inner_map_type> map_type;
//};

#ifdef ENABLE_BENCH_TXN_COUNTERS
void
bench_worker::measure_txn_counters(void *txn, const char *txn_name) {
	auto ret = db->get_txn_counters(txn);
	map_maxer<string, uint64_t>()(local_txn_counters[txn_name], ret);
}
#endif

map<string, size_t>
bench_worker::get_txn_counts() const {
	map<string, size_t> m;
	const workload_desc_vec workload = get_workload();
	for(size_t i = 0; i < txn_counts.size(); i++)
		m[workload[i].name] = txn_counts[i];
	return m;
}
