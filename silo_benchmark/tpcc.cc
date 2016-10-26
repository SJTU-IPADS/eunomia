/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */
#include <iostream>
#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>

//#include "txn.h"
//#include "macros.h"
//#include "scopedperf.hh"
//#include "spinlock.h"

#include "bench.h"
#include "tpcc.h"


#include "db/dbtx.h"
#include "db/dbrotx.h"
#include "db/dbtables.h"


using namespace std;
using namespace util;
#define SEPERATE 0

#define SLDBTX	0
#define CHECKTPCC 0
#define SEC_INDEX 1
#define BINDWAREHOUSE 1

#define TPCC_DUMP 0
#define DBTX_TIME 0
#define DBTX_PROF 0

//multiple tables in the database
#define WARE 0
#define DIST 1
#define CUST 2
#define HIST 3
#define NEWO 4
#define ORDE 5
#define ORLI 6
#define ITEM 7
#define STOC 8

#if USESECONDINDEX
#define CUST_INDEX 0
#define ORDER_INDEX 1
#else
#define CUST_INDEX 9
#define ORDER_INDEX 10
#endif

enum TPCC_TYPE{NEW_ORDER = 0, PAYMENT, DELIVERY, ORDER_STATUS, STOCK_LEVEL};

#if 0
#define TPCC_TABLE_LIST(x) \
  x(customer) \
  x(customer_name_idx) \
  x(district) \
  x(history) \
  x(item) \
  x(new_order) \
  x(oorder) \
  x(oorder_c_id_idx) \
  x(order_line) \
  x(stock) \
  x(stock_data) \
  x(warehouse)
#endif

#if SHORTKEY

static inline ALWAYS_INLINE size_t
NumWarehouses() {
	return (size_t) scale_factor;
}

// config constants

static constexpr inline ALWAYS_INLINE size_t
NumItems() {
	return 100000;
}

static constexpr inline ALWAYS_INLINE size_t
NumDistrictsPerWarehouse() {
	return 5;
}

static constexpr inline ALWAYS_INLINE size_t
NumCustomersPerDistrict() {
	return 3000;
}


static inline ALWAYS_INLINE int64_t makeDistrictKey(int32_t w_id, int32_t d_id) {
	int32_t did = d_id + (w_id * NumDistrictsPerWarehouse());
	int64_t id = static_cast<int64_t>(did);
	return id;
}

static inline ALWAYS_INLINE int64_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
	int32_t upper_id = w_id * NumDistrictsPerWarehouse()+ d_id;
	int64_t id =  static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
	return id;
}

static inline ALWAYS_INLINE int64_t makeHistoryKey(int32_t h_c_id, int32_t h_c_d_id, int32_t h_c_w_id, int32_t h_d_id, int32_t h_w_id) {
	int32_t cid = (h_c_w_id * NumDistrictsPerWarehouse() + h_c_d_id) * NumCustomersPerDistrict() + h_c_id;
	int32_t did = h_d_id + (h_w_id * NumDistrictsPerWarehouse());
	int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
	return id;
}

static inline ALWAYS_INLINE int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
	int32_t upper_id = w_id * NumDistrictsPerWarehouse() + d_id;
	int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	return id;
}

static inline ALWAYS_INLINE int64_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
	int32_t upper_id = w_id * NumDistrictsPerWarehouse() + d_id;
	int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	return id;
}

static inline ALWAYS_INLINE int64_t makeOrderIndex(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
	int32_t upper_id = (w_id * NumDistrictsPerWarehouse() + d_id) * NumCustomersPerDistrict() + c_id;
	int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	return id;
}

static inline ALWAYS_INLINE int64_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
	int32_t upper_id = w_id * NumDistrictsPerWarehouse() + d_id;
	int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
	int64_t olid = oid * 15 + number;
	int64_t id = static_cast<int64_t>(olid);
	return id;
}

static inline ALWAYS_INLINE int64_t makeStockKey(int32_t w_id, int32_t s_id) {
	int32_t sid = s_id + (w_id * NumItems());
	int64_t id = static_cast<int64_t>(sid);
	return id;
}

static void convertString(char *newstring, const char *oldstring, int size) {
	for(int i = 0; i < 8; i++)
		if(i < size)
			newstring[7 - i] = oldstring[i];
		else newstring[7 - i] = '\0';

	for(int i = 8; i < 16; i++)
		if(i < size)
			newstring[23 - i] = oldstring[i];
		else newstring[23 - i] = '\0';
#if 0
	for(int i = 0; i < 16; i++)
		printf("%lx ", oldstring[i]);
	printf("\n");
	for(int i = 0; i < 16; i++)
		printf("%lx ", newstring[i]);
	printf("\n");
#endif
}
static bool compareCustomerIndex(uint64_t key, uint64_t bound) {
	uint64_t *k = (uint64_t *)key;
	uint64_t *b = (uint64_t *)bound;
	for(int i = 0; i < 5; i++) {
		if(k[i] > b[i]) return false;
		if(k[i] < b[i]) return true;
	}
	return true;
}

static uint64_t makeCustomerIndex(int32_t w_id, int32_t d_id, string s_last, string s_first) {
	uint64_t *seckey = new uint64_t[5];
	int32_t did = d_id + (w_id * 10);
	seckey[0] = did;
	convertString((char *)(&seckey[1]), s_last.data(), s_last.size());
	convertString((char *)(&seckey[3]), s_first.data(), s_last.size());
#if 0
	printf("%d %d %s %s \n", w_id, d_id, c_last, c_first);
	for(int i = 0; i < 5; i++)
		printf("%lx ", seckey[i]);
	printf("\n");
#endif
	return (uint64_t)seckey;
}

#endif


// T must implement lock()/unlock(). Both must *not* throw exceptions
template <typename T>
class scoped_multilock {
public:
	inline scoped_multilock()
		: did_lock(false) {
	}

	inline ~scoped_multilock() {
		if(did_lock)
			for(auto &t : locks)
				t->unlock();
	}

	inline void
	enq(T &t) {
		ALWAYS_ASSERT(!did_lock);
		locks.emplace_back(&t);
	}

	inline void
	multilock() {
		ALWAYS_ASSERT(!did_lock);
		if(locks.size() > 1)
			sort(locks.begin(), locks.end());
#ifdef CHECK_INVARIANTS
		if(set<T *>(locks.begin(), locks.end()).size() != locks.size()) {
			for(auto &t : locks)
				cerr << "lock: " << hexify(t) << endl;
			INVARIANT(false && "duplicate locks found");
		}
#endif
		for(auto &t : locks)
			t->lock();
		did_lock = true;
	}

private:
	bool did_lock;
	typename util::vec<T *, 64>::type locks;
};

// like a lock_guard, but has the option of not acquiring
template <typename T>
class scoped_lock_guard {
public:
	inline scoped_lock_guard(T &l)
		: l(&l) {
		this->l->lock();
	}

	inline scoped_lock_guard(T *l)
		: l(l) {
		if(this->l)
			this->l->lock();
	}

	inline ~scoped_lock_guard() {
		if(l)
			l->unlock();
	}

private:
	T *l;
};

// configuration flags
static int g_disable_xpartition_txn = 0;
static int g_disable_read_only_scans = 0;
static int g_enable_partition_locks = 0;
static int g_enable_separate_tree_per_partition = 0;
static int g_new_order_remote_item_pct = 1;
static int g_new_order_fast_id_gen = 0;
static int g_uniform_item_dist = 0;
//static unsigned g_txn_workload_mix[] = { 45,43,4,4,4 }; // default TPC-C workload mix
static unsigned g_txn_workload_mix[] = {45,43,4,4,4};

//static aligned_padded_elem<spinlock> *g_partition_locks = nullptr;
static aligned_padded_elem<atomic<uint64_t>> *g_district_ids = nullptr;

// maps a wid => partition id
static inline ALWAYS_INLINE unsigned int
PartitionId(unsigned int wid) {
	INVARIANT(wid >= 1 && wid <= NumWarehouses());
	wid -= 1; // 0-idx
	if(NumWarehouses() <= nthreads)
		// more workers than partitions, so its easy
		return wid;
	const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
	const unsigned partid = wid / nwhse_per_partition;
	if(partid >= nthreads)
		return nthreads - 1;
	return partid;
}
#if 0
static inline ALWAYS_INLINE spinlock &
LockForPartition(unsigned int wid) {
	INVARIANT(g_enable_partition_locks);
	return g_partition_locks[PartitionId(wid)].elem;
}
#endif
static inline atomic<uint64_t> &
NewOrderIdHolder(unsigned warehouse, unsigned district) {
	INVARIANT(warehouse >= 1 && warehouse <= NumWarehouses());
	INVARIANT(district >= 1 && district <= NumDistrictsPerWarehouse());
	const unsigned idx =
		(warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
	return g_district_ids[idx].elem;
}

static inline uint64_t
FastNewOrderIdGen(unsigned warehouse, unsigned district) {
	return NewOrderIdHolder(warehouse, district).fetch_add(1, memory_order_acq_rel);
}

struct checker {
	// these sanity checks are just a few simple checks to make sure
	// the data is not entirely corrupted

	static inline ALWAYS_INLINE void
	SanityCheckCustomer(const customer::key *k, const customer::value *v) {

#if !SHORTKEY
		INVARIANT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
		INVARIANT(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
		INVARIANT(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
#endif
		INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
		INVARIANT(v->c_middle == "OE");
	}

	static inline ALWAYS_INLINE void
	SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v) {
#if !SHORTKEY
		INVARIANT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
#endif
		INVARIANT(v->w_state.size() == 2);
		INVARIANT(v->w_zip == "123456789");
	}

	static inline ALWAYS_INLINE void
	SanityCheckDistrict(const district::key *k, const district::value *v) {
#if !SHORTKEY
		INVARIANT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
		INVARIANT(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
#endif
		//printf("v->d_next_o_id = %d\n",v->d_next_o_id );

		INVARIANT(v->d_next_o_id >= 3001);
		INVARIANT(v->d_state.size() == 2);
		INVARIANT(v->d_zip == "123456789");
	}

	static inline ALWAYS_INLINE void
	SanityCheckItem(const item::key *k, const item::value *v) {
#if !SHORTKEY
		INVARIANT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
#endif
		INVARIANT(v->i_price >= 1.0 && v->i_price <= 100.0);
	}

	static inline ALWAYS_INLINE void
	SanityCheckStock(const stock::key *k, const stock::value *v) {
#if !SHORTKEY
		INVARIANT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
		INVARIANT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
#endif
	}

	static inline ALWAYS_INLINE void
	SanityCheckNewOrder(const new_order::key *k, const new_order::value *v) {
#if !SHORTKEY
		INVARIANT(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= NumWarehouses());
		INVARIANT(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
#endif
	}

	static inline ALWAYS_INLINE void
	SanityCheckOOrder(const oorder::key *k, const oorder::value *v) {
#if !SHORTKEY
		INVARIANT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
		INVARIANT(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
#endif
		INVARIANT(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
		INVARIANT(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
		INVARIANT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
	}

	static inline ALWAYS_INLINE void
	SanityCheckOrderLine(const order_line::key *k, const order_line::value *v) {
#if !SHORTKEY
		INVARIANT(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
		INVARIANT(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
		INVARIANT(k->ol_number >= 1 && k->ol_number <= 15);
#endif
		INVARIANT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
	}

};


struct _dummy {}; // exists so we can inherit from it, so we can use a macro in
// an init list...

class tpcc_worker_mixin : private _dummy {

#if 0
#define DEFN_TBL_INIT_X(name) \
  , tbl_ ## name ## _vec(partitions.at(#name))
#endif

public:
	DBTables *store;
	tpcc_worker_mixin(DBTables *s) :
		_dummy() // so hacky...
#if 0
		TPCC_TABLE_LIST(DEFN_TBL_INIT_X)
#endif
	{
		ALWAYS_ASSERT(NumWarehouses() >= 1);
		store = s;
	}
#if 0
#undef DEFN_TBL_INIT_X

protected:

#define DEFN_TBL_ACCESSOR_X(name) \
private:  \
  vector<abstract_ordered_index *> tbl_ ## name ## _vec; \
protected: \
  inline ALWAYS_INLINE abstract_ordered_index * \
  tbl_ ## name (unsigned int wid) \
  { \
    INVARIANT(wid >= 1 && wid <= NumWarehouses()); \
    INVARIANT(tbl_ ## name ## _vec.size() == NumWarehouses()); \
    return tbl_ ## name ## _vec[wid - 1]; \
  }

	TPCC_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X
#endif

	// only TPCC loaders need to call this- workers are automatically
	// pinned by their worker id (which corresponds to warehouse id
	// in TPCC)
	//
	// pins the *calling* thread
	static void
	PinToWarehouseId(unsigned int wid) {
		const unsigned int partid = PartitionId(wid);
		ALWAYS_ASSERT(partid < nthreads);
		const unsigned int pinid  = partid;
#if 0
		if(verbose)
			cerr << "PinToWarehouseId(): coreid=" << coreid::core_id()
				 << " pinned to whse=" << wid << " (partid=" << partid << ")"
				 << endl;
//    rcu::s_instance.pin_current_thread(pinid);
//    rcu::s_instance.fault_region();
#endif
	}

public:

	static inline uint32_t
	GetCurrentTimeMillis() {
		//struct timeval tv;
		//ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
		//return tv.tv_sec * 1000;

		// XXX(stephentu): implement a scalable GetCurrentTimeMillis()
		// for now, we just give each core an increasing number

		static __thread uint32_t tl_hack = 0;
		return tl_hack++;
	}

	// utils for generating random #s and strings

	static inline ALWAYS_INLINE int
	CheckBetweenInclusive(int v, int lower, int upper) {
		INVARIANT(v >= lower);
		INVARIANT(v <= upper);
		return v;
	}

	static inline ALWAYS_INLINE int
	RandomNumber(fast_random &r, int min, int max) {
		return CheckBetweenInclusive((int)(r.next_uniform() * (max - min + 1) + min), min, max);
	}

	static inline ALWAYS_INLINE int
	NonUniformRandom(fast_random &r, int A, int C, int min, int max) {
		return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
	}

	static inline ALWAYS_INLINE int
	GetItemId(fast_random &r) {
		return CheckBetweenInclusive(
				   g_uniform_item_dist ?
				   RandomNumber(r, 1, NumItems()) :
				   NonUniformRandom(r, 8191, 7911, 1, NumItems()),
				   1, NumItems());
	}

	static inline ALWAYS_INLINE int
	GetCustomerId(fast_random &r) {
		return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1, NumCustomersPerDistrict());
	}

	// pick a number between [start, end)
	static inline ALWAYS_INLINE unsigned
	PickWarehouseId(fast_random &r, unsigned start, unsigned end) {
		INVARIANT(start < end);
		const unsigned diff = end - start;
		if(diff == 1)
			return start;
		return (r.next() % diff) + start;
	}

	static string NameTokens[];

	// all tokens are at most 5 chars long
	static const size_t CustomerLastNameMaxSize = 5 * 3;

	static inline size_t
	GetCustomerLastName(uint8_t *buf, fast_random &r, int num) {
		const string &s0 = NameTokens[num / 100];
		const string &s1 = NameTokens[(num / 10) % 10];
		const string &s2 = NameTokens[num % 10];
		uint8_t *const begin = buf;
		const size_t s0_sz = s0.size();
		const size_t s1_sz = s1.size();
		const size_t s2_sz = s2.size();
		NDB_MEMCPY(buf, s0.data(), s0_sz);
		buf += s0_sz;
		NDB_MEMCPY(buf, s1.data(), s1_sz);
		buf += s1_sz;
		NDB_MEMCPY(buf, s2.data(), s2_sz);
		buf += s2_sz;
		return buf - begin;
	}

	static inline ALWAYS_INLINE size_t
	GetCustomerLastName(char *buf, fast_random &r, int num) {
		return GetCustomerLastName((uint8_t *) buf, r, num);
	}

	static inline string
	GetCustomerLastName(fast_random &r, int num) {
		string ret;
		ret.resize(CustomerLastNameMaxSize);
		ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
		return ret;
	}

	static inline ALWAYS_INLINE string
	GetNonUniformCustomerLastNameLoad(fast_random &r) {
		return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
	}

	static inline ALWAYS_INLINE size_t
	GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r) {
		return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
	}

	static inline ALWAYS_INLINE size_t
	GetNonUniformCustomerLastNameRun(char *buf, fast_random &r) {
		return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
	}

	static inline ALWAYS_INLINE string
	GetNonUniformCustomerLastNameRun(fast_random &r) {
		return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
	}

	// following oltpbench, we really generate strings of len - 1...
	static inline string
	RandomStr(fast_random &r, uint len) {
		// this is a property of the oltpbench implementation...
		if(!len)
			return "";

		uint i = 0;
		string buf(len - 1, 0);
		while(i < (len - 1)) {
			const char c = (char) r.next_char();
			// XXX(stephentu): oltpbench uses java's Character.isLetter(), which
			// is a less restrictive filter than isalnum()
			if(!isalnum(c))
				continue;
			buf[i++] = c;
		}
		return buf;
	}

	// RandomNStr() actually produces a string of length len
	static inline string
	RandomNStr(fast_random &r, uint len) {
		const char base = '0';
		string buf(len, 0);
		for(uint i = 0; i < len; i++)
			buf[i] = (char)(base + (r.next() % 10));
		return buf;
	}
};

string tpcc_worker_mixin::NameTokens[] = {
	string("BAR"),
	string("OUGHT"),
	string("ABLE"),
	string("PRI"),
	string("PRES"),
	string("ESE"),
	string("ANTI"),
	string("CALLY"),
	string("ATION"),
	string("EING"),
};

struct op_prof{
	uint64_t gets;
	uint64_t adds;
	uint64_t dels;
	uint64_t nexts;
	uint64_t prevs;
	uint64_t seeks;
	uint64_t rogets;
	uint64_t ronexts;
	uint64_t roprevs;
	uint64_t roseeks;
};

//STATIC_COUNTER_DECL(scopedperf::tsc_ctr, tpcc_txn, tpcc_txn_cg)

class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
public:
	DBTX tx;
	DBROTX rotx;
	uint64_t dbtx_time[TPCC_TYPES];
	//uint64_t newo_txn_time[NEWO_TXNS];
	//uint64_t payment_txn_time[PAY_TXNS];
	uint64_t orli_time, item_time, stoc_time, orli_time2;
	uint64_t orlis,stocs;
	uint64_t txns[TPCC_TYPES];
	uint64_t succ_commit, fail_commit, excp_commit;
	uint64_t* last_order_line_id_list;
	op_prof Op_prof[TABLE_NUM];
	// resp for [warehouse_id_start, warehouse_id_end)
	tpcc_worker(unsigned int worker_id,
				unsigned long seed, abstract_db *db,
				spin_barrier *barrier_a, spin_barrier *barrier_b,
				uint warehouse_id_start, uint warehouse_id_end, DBTables *store)
		: bench_worker(worker_id, true, seed, db,
					   barrier_a, barrier_b),
		tpcc_worker_mixin(store),
		warehouse_id_start(warehouse_id_start),
		warehouse_id_end(warehouse_id_end),
		tx(store),
		rotx(store) {

		last_order_line_id_list = (uint64_t*)calloc(warehouse_id_end - warehouse_id_start, sizeof(uint64_t));
		
		for(int i = 0; i < TABLE_NUM; i++){
			Op_prof[i]={0,0,0,0,0,0,0,0,0,0};
		}
		/*
		for(int i = 0; i < NEWO_TXNS;i++){
			newo_txn_time[i] = 0;
		}
		for(int i = 0; i < PAY_TXNS; i++){
			payment_txn_time[i] = 0;
		}
		*/
		tx.worker_id = worker_id;
		secs = 0;
		orli_time = item_time = stoc_time = 0;
		orlis = stocs = 0;
		for(int i = 0; i < TPCC_TYPES; i++){
			txns[i] = 0;
			dbtx_time[i] = 0;
		}
		INVARIANT(warehouse_id_start >= 1);
		INVARIANT(warehouse_id_start <= NumWarehouses());
		INVARIANT(warehouse_id_end > warehouse_id_start);
		INVARIANT(warehouse_id_end <= (NumWarehouses() + 1));
		NDB_MEMSET(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
		if(verbose) {
			cerr << "tpcc: worker id " << worker_id
				 << " => warehouses [" << warehouse_id_start
				 << ", " << warehouse_id_end << ")"
				 << endl;
		}
		obj_key0.reserve(2 * CACHELINE_SIZE);
		obj_key1.reserve(2 * CACHELINE_SIZE);
		obj_v.reserve(2 * CACHELINE_SIZE);
	}
	~tpcc_worker(){
#if DBTX_TIME
/*
		for(int i = 0; i < TPCC_TYPES; i++){
			//printf("DBTX Time[%d] = %lf sec\n",i, (double)(dbtx_time[i])/MILLION);
			printf("%ld, ",(dbtx_time[i]));
		}
		printf("\n");

		for(int i =0; i <TPCC_TYPES;i++){
			//printf("DBTX Time[%d] = %lf sec\n",i, (double)(dbtx_time[i])/MILLION);
			printf("%lu, ",txns[i]);
		}
		printf("\n");

		for(int i = 0; i < NEWO_TXNS; i++){
			printf("%ld, ", newo_txn_time[i]);
		}
		printf("\n");
*/
		//printf("ORLI Time = %lf sec (%lu)\n", (double)orli_time/MILLION, orlis);
		//printf("ORLI Time2 = %lf sec\n", (double)orli_time2/MILLION);
		//printf("ITEM Time = %lf sec\n", (double)item_time/MILLION);
		//printf("STOC Time = %lf sec (%lu)\n", (double)stoc_time/MILLION, stocs);
#if DBTX_PROF
		for(int i = 0; i < TABLE_NUM; i++){
			printf("Table %2d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d\n", 
				i, Op_prof[i].gets, Op_prof[i].adds, Op_prof[i].dels, Op_prof[i].nexts, Op_prof[i].prevs,
				   Op_prof[i].seeks, Op_prof[i].rogets, Op_prof[i].ronexts, Op_prof[i].roprevs, Op_prof[i].roseeks);
		}
#endif
#endif
	}
	// XXX(stephentu): tune this
	static const size_t NMaxCustomerIdxScanElems = 512;

	txn_result txn_new_order(bool first_run = true);

	static txn_result
	TxnNewOrder(bench_worker *w, bool first_run) {
//    ANON_REGION("TxnNewOrder:", &tpcc_txn_cg);
		return static_cast<tpcc_worker *>(w)->txn_new_order(first_run);
	}

	txn_result txn_delivery(bool first_run = true);

	static txn_result
	TxnDelivery(bench_worker *w, bool first_run) {
//    ANON_REGION("TxnDelivery:", &tpcc_txn_cg);
		return static_cast<tpcc_worker *>(w)->txn_delivery(first_run);
	}

	txn_result txn_payment(bool first_run = true);

	static txn_result
	TxnPayment(bench_worker *w, bool first_run) {
//    ANON_REGION("TxnPayment:", &tpcc_txn_cg);
		return static_cast<tpcc_worker *>(w)->txn_payment(first_run);
	}

	txn_result txn_order_status(bool first_run = true);

	static txn_result
	TxnOrderStatus(bench_worker *w, bool first_run) {
//    ANON_REGION("TxnOrderStatus:", &tpcc_txn_cg);
		return static_cast<tpcc_worker *>(w)->txn_order_status(first_run);
	}

	txn_result txn_stock_level(bool first_run = true);

	static txn_result
	TxnStockLevel(bench_worker *w, bool first_run) {
//    ANON_REGION("TxnStockLevel:", &tpcc_txn_cg);
		return static_cast<tpcc_worker *>(w)->txn_stock_level(first_run);
	}

	virtual workload_desc_vec
	get_workload() const {
		workload_desc_vec w;
		// numbers from sigmod.csail.mit.edu:
		//w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
		//w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
		//w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k ops/sec
		//w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k ops/sec
		//w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k ops/sec
		unsigned m = 0;
		for(size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
			m += g_txn_workload_mix[i];
		ALWAYS_ASSERT(m == 100);
		if(g_txn_workload_mix[0])
			w.push_back(workload_desc("NewOrder", double(g_txn_workload_mix[0]) / 100.0, TxnNewOrder));
		if(g_txn_workload_mix[1])
			w.push_back(workload_desc("Payment", double(g_txn_workload_mix[1]) / 100.0, TxnPayment));
		if(g_txn_workload_mix[2])
			w.push_back(workload_desc("Delivery", double(g_txn_workload_mix[2]) / 100.0, TxnDelivery));
		if(g_txn_workload_mix[3])
			w.push_back(workload_desc("OrderStatus", double(g_txn_workload_mix[3]) / 100.0, TxnOrderStatus));
		if(g_txn_workload_mix[4])
			w.push_back(workload_desc("StockLevel", double(g_txn_workload_mix[4]) / 100.0, TxnStockLevel));
		return w;
	}

protected:
	virtual void
	on_run_setup() OVERRIDE {
		//printf("%ld wid %d\n", pthread_self(), worker_id);
		store->ThreadLocalInit(worker_id - 8);

		if(!pin_cpus)
			return;
//    const size_t a = worker_id % coreid::num_cpus_online();
//    const size_t b = a % nthreads;
//    rcu::s_instance.pin_current_thread(b);
//   rcu::s_instance.fault_region();
	}

private:
	const uint warehouse_id_start;
	const uint warehouse_id_end;
	int32_t last_no_o_ids[32][10]; // XXX(stephentu): hack

	// some scratch buffer space
	string obj_key0;
	string obj_key1;
	string obj_v;
};

class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
public:
	tpcc_warehouse_loader(unsigned long seed,
						  abstract_db *db,
						  DBTables* store)
		: bench_loader(seed, db),
		  tpcc_worker_mixin(store) {
	}

protected:
	virtual void
	load() {
		string obj_buf;
#if 0
		void *txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
		printf("load warehouse\n");
		uint64_t warehouse_total_sz = 0, n_warehouses = 0;
		try {
			vector<warehouse::value> warehouses;
			for(uint i = 1; i <= NumWarehouses(); i++) {
				const warehouse::key k(i);
				const string w_name = RandomStr(r, RandomNumber(r, 6, 10));
				const string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
				const string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
				const string w_city = RandomStr(r, RandomNumber(r, 10, 20));
				const string w_state = RandomStr(r, 3);
				const string w_zip = "123456789";

				warehouse::value *v = new warehouse::value();
				v->w_ytd = 300000;
				v->w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;
				v->w_name.assign(w_name);
				v->w_street_1.assign(w_street_1);
				v->w_street_2.assign(w_street_2);
				v->w_city.assign(w_city);
				v->w_state.assign(w_state);
				v->w_zip.assign(w_zip);

				checker::SanityCheckWarehouse(&k, v);
				const size_t sz = Size(*v);
				warehouse_total_sz += sz;
				n_warehouses++;
#if 0
				tbl_warehouse(i)->insert(txn, Encode(k), Encode(obj_buf, v));
#endif

				store->TupleInsert(WARE, i, (uint64_t *)v, sizeof(warehouse::value));
				//store->tables[WARE]->Put(i, (uint64_t *)v);

				if(Encode(k).size() != 8) cerr << Encode(k).size() << endl;
				warehouses.push_back(*v);
			}
#if 0
			ALWAYS_ASSERT(db->commit_txn(txn));
			arena.reset();
			txn = db->new_txn(txn_flags, arena, txn_buf());
			for(uint i = 1; i <= NumWarehouses(); i++) {
				const warehouse::key k(i);
				string warehouse_v;
				ALWAYS_ASSERT(tbl_warehouse(i)->get(txn, Encode(k), warehouse_v));
				warehouse::value warehouse_temp;
				const warehouse::value *v = Decode(warehouse_v, warehouse_temp);
				ALWAYS_ASSERT(warehouses[i - 1] == *v);

				checker::SanityCheckWarehouse(&k, v);
			}
			ALWAYS_ASSERT(db->commit_txn(txn));
#endif
		} catch(abstract_db::abstract_abort_exception &ex) {
			// shouldn't abort on loading!
			ALWAYS_ASSERT(false);
		}
		if(verbose) {
			cerr << "[INFO] finished loading warehouse" << endl;
			cerr << "[INFO]   * average warehouse record length: "
				 << (double(warehouse_total_sz) / double(n_warehouses)) << " bytes" << endl;
		}
	}
};

class tpcc_item_loader : public bench_loader, public tpcc_worker_mixin {
public:
	tpcc_item_loader(unsigned long seed,
					 abstract_db *db,
					 DBTables* store)
		: bench_loader(seed, db),
		  tpcc_worker_mixin(store) {
	}

protected:
	virtual void
	load() {
		string obj_buf;
#if 0
		const ssize_t bsize = db->txn_max_batch_size();
		void *txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
		uint64_t total_sz = 0;
		try {
			for(uint i = 1; i <= NumItems(); i++) {
				// items don't "belong" to a certain warehouse, so no pinning
				const item::key k(i);
				item::value *v = new item::value();
				const string i_name = RandomStr(r, RandomNumber(r, 14, 24));
				v->i_name.assign(i_name);
				v->i_price = (float) RandomNumber(r, 100, 10000) / 100.0;
				const int len = RandomNumber(r, 26, 50);
				if(RandomNumber(r, 1, 100) > 10) {
					const string i_data = RandomStr(r, len);
					v->i_data.assign(i_data);
				} else {
					const int startOriginal = RandomNumber(r, 2, (len - 8));
					const string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
					v->i_data.assign(i_data);
				}
				v->i_im_id = RandomNumber(r, 1, 10000);

				checker::SanityCheckItem(&k, v);
				const size_t sz = Size(*v);
				total_sz += sz;

				store->TupleInsert(ITEM, i, (uint64_t *)v, sizeof(item::value));
//		store->tables[ITEM]->Put(i, (uint64_t *)v);
#if 0
				tbl_item(1)->insert(txn, Encode(k), Encode(obj_buf, v)); // this table is shared, so any partition is OK

				if(bsize != -1 && !(i % bsize)) {
					ALWAYS_ASSERT(db->commit_txn(txn));
					txn = db->new_txn(txn_flags, arena, txn_buf());
					arena.reset();
				}
#endif
			}
#if 0
			ALWAYS_ASSERT(db->commit_txn(txn));
#endif
		} catch(abstract_db::abstract_abort_exception &ex) {
			// shouldn't abort on loading!
			ALWAYS_ASSERT(false);
		}
		if(verbose) {
			cerr << "[INFO] finished loading item" << endl;
			cerr << "[INFO]   * average item record length: "
				 << (double(total_sz) / double(NumItems())) << " bytes" << endl;
		}
	}
};

class tpcc_stock_loader : public bench_loader, public tpcc_worker_mixin {
public:
	tpcc_stock_loader(unsigned long seed,
					  abstract_db *db,
					  ssize_t warehouse_id,
					  DBTables* store)
		: bench_loader(seed, db),
		  tpcc_worker_mixin(store),
		  warehouse_id(warehouse_id) {
		ALWAYS_ASSERT(warehouse_id == -1 ||
					  (warehouse_id >= 1 &&
					   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
	}

protected:
	virtual void
	load() {
		printf("warehouse_id = %ld\n", warehouse_id);
		string obj_buf, obj_buf1;

		uint64_t stock_total_sz = 0, n_stocks = 0;
		const uint w_start = (warehouse_id == -1) ?
							 1 : static_cast<uint>(warehouse_id);
		const uint w_end   = (warehouse_id == -1) ?
							 NumWarehouses() : static_cast<uint>(warehouse_id);

		for(uint w = w_start; w <= w_end; w++) {
			const size_t batchsize =
				NumItems() ;
			const size_t nbatches = (batchsize > NumItems()) ? 1 : (NumItems() / batchsize);

			if(pin_cpus)
				PinToWarehouseId(w);

			for(uint b = 0; b < nbatches;) {
//        scoped_str_arena s_arena(arena);
#if 0
				void * const txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
				try {
					const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
					for(uint i = (b * batchsize + 1); i <= iend; i++) {

						uint64_t key = makeStockKey(w, i);
#if SHORTKEY
						const stock::key k(makeStockKey(w, i));
#else
						const stock::key k(w, i);

#endif
//            const stock_data::key k_data(w, i);

						stock::value *v = new stock::value();
						v->s_quantity = RandomNumber(r, 10, 100);
						v->s_ytd = 0;
						v->s_order_cnt = 0;
						v->s_remote_cnt = 0;

//            stock_data::value v_data;
						const int len = RandomNumber(r, 26, 50);
						if(RandomNumber(r, 1, 100) > 10) {
							const string s_data = RandomStr(r, len);
//              v_data.s_data.assign(s_data);
//			  v->s_data.assign(s_data);
						} else {
							const int startOriginal = RandomNumber(r, 2, (len - 8));
							const string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
//			  v->s_data.assign(s_data);
//			  v_data.s_data.assign(s_data);
						}
						/*
						            v_data.s_dist_01.assign(RandomStr(r, 24));
						            v_data.s_dist_02.assign(RandomStr(r, 24));
						            v_data.s_dist_03.assign(RandomStr(r, 24));
						            v_data.s_dist_04.assign(RandomStr(r, 24));
						            v_data.s_dist_05.assign(RandomStr(r, 24));
						            v_data.s_dist_06.assign(RandomStr(r, 24));
						            v_data.s_dist_07.assign(RandomStr(r, 24));
						            v_data.s_dist_08.assign(RandomStr(r, 24));
						            v_data.s_dist_09.assign(RandomStr(r, 24));
						            v_data.s_dist_10.assign(RandomStr(r, 24));

								v->s_dist_01.assign(RandomStr(r, 24));
								v->s_dist_02.assign(RandomStr(r, 24));
								v->s_dist_03.assign(RandomStr(r, 24));
								v->s_dist_04.assign(RandomStr(r, 24));
								v->s_dist_05.assign(RandomStr(r, 24));
								v->s_dist_06.assign(RandomStr(r, 24));
								v->s_dist_07.assign(RandomStr(r, 24));
								v->s_dist_08.assign(RandomStr(r, 24));
								v->s_dist_09.assign(RandomStr(r, 24));
								v->s_dist_10.assign(RandomStr(r, 24));
									*/

						checker::SanityCheckStock(&k, v);
						const size_t sz = Size(*v);
						stock_total_sz += sz;
						n_stocks++;

						store->TupleInsert(STOC, key, (uint64_t *)v, sizeof(stock::value));
						//store->tables[STOC]->Put(key, (uint64_t *)v);
						//	printf("key %ld\n" , key);
#if 0
						tbl_stock(w)->insert(txn, Encode(k), Encode(obj_buf, v));
#endif
//           tbl_stock_data(w)->insert(txn, Encode(k_data), Encode(obj_buf1, v_data));
					}
#if 0
					if(db->commit_txn(txn)) {
#endif
						b++;
#if 0
					} else {
						db->abort_txn(txn);
						if(verbose)
							cerr << "[WARNING] stock loader loading abort" << endl;
					}

#endif
//			store->tables[STOC]->PrintStore();
				} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
					db->abort_txn(txn);
#endif
					ALWAYS_ASSERT(warehouse_id != -1);
					if(verbose)
						cerr << "[WARNING] stock loader loading abort" << endl;
				}
			}
		}

		if(verbose) {
			if(warehouse_id == -1) {
				cerr << "[INFO] finished loading stock" << endl;
				cerr << "[INFO]   * average stock record length: "
					 << (double(stock_total_sz) / double(n_stocks)) << " bytes" << endl;
			} else {
				cerr << "[INFO] finished loading stock (w=" << warehouse_id << ")" << endl;
			}
		}
	}

private:
	ssize_t warehouse_id;
};

class tpcc_district_loader : public bench_loader, public tpcc_worker_mixin {
public:
	tpcc_district_loader(unsigned long seed,
						 abstract_db *db,
						 DBTables* store)
		: bench_loader(seed, db),
		  tpcc_worker_mixin(store) {
	}

protected:
	virtual void
	load() {
		string obj_buf;
		const ssize_t bsize = -1;
#if 0
		void *txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
		uint64_t district_total_sz = 0, n_districts = 0;
		try {
			uint cnt = 0;
			for(uint w = 1; w <= NumWarehouses(); w++) {
				if(pin_cpus)
					PinToWarehouseId(w);
				for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
					uint64_t key = makeDistrictKey(w, d);
#if SHORTKEY
					const district::key k(makeDistrictKey(w, d));
#else
					const district::key k(w, d);
#endif
					district::value *v = new district::value();
					v->d_ytd = 30000;
					v->d_tax = (float)(RandomNumber(r, 0, 2000) / 10000.0);
					v->d_next_o_id = 3001;
					v->d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
					v->d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
					v->d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
					v->d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
					v->d_state.assign(RandomStr(r, 3));
					v->d_zip.assign("123456789");

					checker::SanityCheckDistrict(&k, v);
					const size_t sz = Size(*v);
					district_total_sz += sz;
					n_districts++;

					//store->tables[DIST]->Put(key, (uint64_t *)v);
					store->TupleInsert(DIST, key, (uint64_t *)v, sizeof(district::value));
					//printf("[DIST] TupleInsert key = %lu\n", key);
#if 0
					tbl_district(w)->insert(txn, Encode(k), Encode(obj_buf, v));
					if(bsize != -1 && !((cnt + 1) % bsize)) {
						ALWAYS_ASSERT(db->commit_txn(txn));
						txn = db->new_txn(txn_flags, arena, txn_buf());
						arena.reset();
					}
#endif
				}
			}
#if 0
			ALWAYS_ASSERT(db->commit_txn(txn));
#endif
		} catch(abstract_db::abstract_abort_exception &ex) {
			// shouldn't abort on loading!
			ALWAYS_ASSERT(false);
		}
		if(verbose) {
			cerr << "[INFO] finished loading district" << endl;
			cerr << "[INFO]   * average district record length: "
				 << (double(district_total_sz) / double(n_districts)) << " bytes" << endl;
		}
	}
};

class tpcc_customer_loader : public bench_loader, public tpcc_worker_mixin {
public:
	tpcc_customer_loader(unsigned long seed,
						 abstract_db *db,
						 ssize_t warehouse_id,
						 DBTables* store)
		: bench_loader(seed, db),
		  tpcc_worker_mixin(store),
		  warehouse_id(warehouse_id) {
		ALWAYS_ASSERT(warehouse_id == -1 ||
					  (warehouse_id >= 1 &&
					   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
	}

protected:
	virtual void
	load() {
		string obj_buf;

		const uint w_start = (warehouse_id == -1) ?
							 1 : static_cast<uint>(warehouse_id);
		const uint w_end   = (warehouse_id == -1) ?
							 NumWarehouses() : static_cast<uint>(warehouse_id);
		const size_t batchsize =

			NumCustomersPerDistrict() ;
		const size_t nbatches =
			(batchsize > NumCustomersPerDistrict()) ?
			1 : (NumCustomersPerDistrict() / batchsize);
		cerr << "num batches: " << nbatches << endl;

		uint64_t total_sz = 0;

		for(uint w = w_start; w <= w_end; w++) {
			if(pin_cpus)
				PinToWarehouseId(w);
			for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
				for(uint batch = 0; batch < nbatches;) {
//          scoped_str_arena s_arena(arena);
#if 0
					void * const txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
					const size_t cstart = batch * batchsize;
					const size_t cend = std::min((batch + 1) * batchsize, NumCustomersPerDistrict());
					try {
						for(uint cidx0 = cstart; cidx0 < cend; cidx0++) {
							const uint c = cidx0 + 1;
							uint64_t key = makeCustomerKey(w, d, c);
#if SHORTKEY
							const customer::key k(makeCustomerKey(w, d, c));
#else
							const customer::key k(w, d, c);

#endif
							customer::value *v = new customer::value();
							v->c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
							if(RandomNumber(r, 1, 100) <= 10)
								v->c_credit.assign("BC");
							else
								v->c_credit.assign("GC");

							if(c <= 1000)
								v->c_last.assign(GetCustomerLastName(r, c - 1));
							else
								v->c_last.assign(GetNonUniformCustomerLastNameLoad(r));

							v->c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
							v->c_credit_lim = 50000;

							v->c_balance = -10;
							v->c_ytd_payment = 10;
							v->c_payment_cnt = 1;
							v->c_delivery_cnt = 0;

							v->c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
							v->c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
							v->c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
							v->c_state.assign(RandomStr(r, 3));
							v->c_zip.assign(RandomNStr(r, 4) + "11111");
							v->c_phone.assign(RandomNStr(r, 16));
							v->c_since = GetCurrentTimeMillis();
							v->c_middle.assign("OE");
							v->c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

							checker::SanityCheckCustomer(&k, v);
							const size_t sz = Size(*v);
							total_sz += sz;

//			  Memstore::MemNode *node = store->tables[CUST]->Put(key, (uint64_t *)v);
							store->TupleInsert(CUST, key, (uint64_t *)v, sizeof(customer::value));
							//printf("TupleInsert[CUST] key = %lu\n", key);
#if 0
							tbl_customer(w)->insert(txn, Encode(k), Encode(obj_buf, v));
#endif
							// customer name index

							uint64_t sec = makeCustomerIndex(w, d,
															 v->c_last.str(true), v->c_first.str(true));
#if SHORTKEY
							const customer_name_idx::key k_idx(w * 10 + d, v->c_last.str(true), v->c_first.str(true));
#else
							const customer_name_idx::key k_idx(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));

#endif
							const customer_name_idx::value v_idx(k.c_id);

							// index structure is:
							// (c_w_id, c_d_id, c_last, c_first) -> (c_id)
#if USESECONDINDEX
							store->secondIndexes[CUST_INDEX]->Put(sec, key, node);
#else
							Memstore::MemNode* mn = store->tables[CUST_INDEX]->Get(sec);
							if(mn == NULL) {
								uint64_t *prikeys = new uint64_t[2];
								prikeys[0] = 1;
								prikeys[1] = key;
								//printf("key %ld\n",key);
								//store->tables[CUST_INDEX]->Put(sec, prikeys);
								store->TupleInsert(CUST_INDEX, sec, (uint64_t *)prikeys, 2 * sizeof(uint64_t));
							} else {
								printf("ccccc\n");
								uint64_t *value = mn->value;
								int num = value[0];
								uint64_t *prikeys = new uint64_t[num + 2];
								prikeys[0] = num + 1;
								for(int i = 1; i <= num; i++)
									prikeys[i] = value[i];
								prikeys[num + 1] = key;
//					store->tables[CUST_INDEX]->Put(sec, prikeys);
								store->TupleInsert(CUST_INDEX, sec,
												   (uint64_t *)prikeys, (num + 2) * sizeof(uint64_t));
								//delete[] value;
							}
#endif

#if 0
							tbl_customer_name_idx(w)->insert(txn, Encode(k_idx), Encode(obj_buf, v_idx));
#endif
							//cerr << Encode(k_idx).size() << endl;

							uint64_t hkey = makeHistoryKey(c, d, w, d, w);
#if SHORTKEY
							history::key k_hist(makeHistoryKey(c, d, w, d, w));
#else
							history::key k_hist;
							k_hist.h_c_id = c;
							k_hist.h_c_d_id = d;
							k_hist.h_c_w_id = w;
							k_hist.h_d_id = d;
							k_hist.h_w_id = w;
							k_hist.h_date = GetCurrentTimeMillis();
#endif

							history::value *v_hist = new history::value();
							v_hist->h_amount = 10;
							v_hist->h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

							//  store->tables[HIST]->Put(hkey, (uint64_t *)v_hist);

							store->TupleInsert(HIST, hkey, (uint64_t *)v_hist, sizeof(history::value));
#if 0
							tbl_history(w)->insert(txn, Encode(k_hist), Encode(obj_buf, v_hist));
#endif
							if(Encode(k_hist).size() != 8)cerr << Encode(k_hist).size() << endl;
						}
#if 0
						if(db->commit_txn(txn)) {
#endif
							batch++;
#if 0
						} else {
							db->abort_txn(txn);
							if(verbose)
								cerr << "[WARNING] customer loader loading abort" << endl;
						}
#endif
					} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
						db->abort_txn(txn);
#endif
						if(verbose)
							cerr << "[WARNING] customer loader loading abort" << endl;
					}
				}
			}
		}

		if(verbose) {
			if(warehouse_id == -1) {
				cerr << "[INFO] finished loading customer" << endl;
				cerr << "[INFO]   * average customer record length: "
					 << (double(total_sz) / double(NumWarehouses()*NumDistrictsPerWarehouse()*NumCustomersPerDistrict()))
					 << " bytes " << endl;
			} else {
				cerr << "[INFO] finished loading customer (w=" << warehouse_id << ")" << endl;
			}
		}
	}

private:
	ssize_t warehouse_id;
};

class tpcc_order_loader : public bench_loader, public tpcc_worker_mixin {
public:
	tpcc_order_loader(unsigned long seed,
					  abstract_db *db,
					  ssize_t warehouse_id,
					  DBTables* store)
		: bench_loader(seed, db),
		  tpcc_worker_mixin(store),
		  warehouse_id(warehouse_id) {
		ALWAYS_ASSERT(warehouse_id == -1 ||
					  (warehouse_id >= 1 &&
					   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
	}

protected:
	virtual void
	load() {
		string obj_buf;

		uint64_t order_line_total_sz = 0, n_order_lines = 0;
		uint64_t oorder_total_sz = 0, n_oorders = 0;
		uint64_t new_order_total_sz = 0, n_new_orders = 0;

		const uint w_start = (warehouse_id == -1) ?
							 1 : static_cast<uint>(warehouse_id);
		const uint w_end   = (warehouse_id == -1) ?
							 NumWarehouses() : static_cast<uint>(warehouse_id);

		for(uint w = w_start; w <= w_end; w++) {
			if(pin_cpus)
				PinToWarehouseId(w);
			for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
				set<uint> c_ids_s;
				vector<uint> c_ids;
				while(c_ids.size() != NumCustomersPerDistrict()) {
					const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
					if(c_ids_s.count(x))
						continue;
					c_ids_s.insert(x);
					c_ids.emplace_back(x);
				}
				for(uint c = 1; c <= NumCustomersPerDistrict();) {
//          scoped_str_arena s_arena(arena);
#if 0
					void * const txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
					try {
						uint64_t okey = makeOrderKey(w, d, c);
#if SHORTKEY
						const oorder::key k_oo(makeOrderKey(w, d, c));
#else
						const oorder::key k_oo(w, d, c);
#endif
						oorder::value *v_oo = new oorder::value();
						v_oo->o_c_id = c_ids[c - 1];
						if(c < 2101)
							v_oo->o_carrier_id = RandomNumber(r, 1, 10);
						else
							v_oo->o_carrier_id = 0;
						v_oo->o_ol_cnt = RandomNumber(r, 5, 15);
						v_oo->o_all_local = 1;
						v_oo->o_entry_d = GetCurrentTimeMillis();

						checker::SanityCheckOOrder(&k_oo, v_oo);
						const size_t sz = Size(*v_oo);
						oorder_total_sz += sz;
						n_oorders++;
						//printf("here1\n");						
						store->TupleInsert(ORDE, okey, (uint64_t *)v_oo, sizeof(oorder::value));
						//Memstore::MemNode *node = store->tables[ORDE]->Put(okey, (uint64_t *)v_oo);
						//printf("here2\n");	

						uint64_t sec = makeOrderIndex(w, d, v_oo->o_c_id, c);
#if USESECONDINDEX
						store->secondIndexes[ORDER_INDEX]->Put(sec, okey, node);
#else
						Memstore::MemNode* mn = store->tables[ORDER_INDEX]->Get(sec);

						if(mn == NULL) {
							uint64_t *prikeys = new uint64_t[2];
							prikeys[0] = 1;
							prikeys[1] = okey;
//		store->tables[ORDER_INDEX]->Put(sec, prikeys);
							store->TupleInsert(ORDER_INDEX, sec, prikeys, 2 * sizeof(uint64_t));
						} else {
							printf("oooo\n");
							uint64_t *value = mn->value;
							int num = value[0];
							uint64_t *prikeys = new uint64_t[num + 2];
							prikeys[0] = num + 1;
							for(int i = 1; i <= num; i++)
								prikeys[i] = value[i];
							prikeys[num + 1] = okey;
//				store->tables[ORDER_INDEX]->Put(sec, prikeys);
							store->TupleInsert(ORDER_INDEX, sec, prikeys, (num + 2) * sizeof(uint64_t));

							delete[] value;
						}
#endif

#if 0
						tbl_oorder(w)->insert(txn, Encode(k_oo), Encode(obj_buf, v_oo));
#endif
#if SHORTKEY
						const oorder_c_id_idx::key k_oo_idx(makeOrderIndex(w, d, v_oo->o_c_id, c));
#else
						const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo->o_c_id, k_oo.o_id);
#endif
						const oorder_c_id_idx::value v_oo_idx(0);

#if 0
						tbl_oorder_c_id_idx(w)->insert(txn, Encode(k_oo_idx), Encode(obj_buf, v_oo_idx));
#endif
						if(c >= 2101) {
							uint64_t nokey = makeNewOrderKey(w, d, c);
#if SHORTKEY
							const new_order::key k_no(makeNewOrderKey(w, d, c));
#else
							const new_order::key k_no(w, d, c);
#endif
							const new_order::value *v_no = new new_order::value();

							checker::SanityCheckNewOrder(&k_no, v_no);
							const size_t sz = Size(*v_no);
							new_order_total_sz += sz;
							n_new_orders++;

							//store->tables[NEWO]->Put(nokey, (uint64_t *)v_no);
							store->TupleInsert(NEWO, nokey, (uint64_t *)v_no, sizeof(new_order::value));

#if 0
							tbl_new_order(w)->insert(txn, Encode(k_no), Encode(obj_buf, v_no));
#endif
						}

						for(uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
							uint64_t olkey = makeOrderLineKey(w, d, c, l);
#if SHORTKEY
							const order_line::key k_ol(makeOrderLineKey(w, d, c, l));
#else
							const order_line::key k_ol(w, d, c, l);
#endif
							order_line::value *v_ol = new order_line::value();
							v_ol->ol_i_id = RandomNumber(r, 1, 100000);
							if(c < 2101) {
								v_ol->ol_delivery_d = v_oo->o_entry_d;
								v_ol->ol_amount = 0;
							} else {
								v_ol->ol_delivery_d = 0;
								// random within [0.01 .. 9,999.99]
								v_ol->ol_amount = (float)(RandomNumber(r, 1, 999999) / 100.0);
							}

							v_ol->ol_supply_w_id = w;
							v_ol->ol_quantity = 5;
							// v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
							//v_ol.ol_dist_info = RandomStr(r, 24);

							checker::SanityCheckOrderLine(&k_ol, v_ol);
							const size_t sz = Size(*v_ol);
							order_line_total_sz += sz;
							n_order_lines++;

							store->TupleInsert(ORLI, olkey, (uint64_t *)v_ol, sizeof(order_line::value));
//			  store->tables[ORLI]->Put(olkey, (uint64_t *)v_ol);
#if 0
							tbl_order_line(w)->insert(txn, Encode(k_ol), Encode(obj_buf, v_ol));
#endif
						}
#if 0
						if(db->commit_txn(txn)) {
#endif
							c++;
#if 0
						} else {
							db->abort_txn(txn);
							ALWAYS_ASSERT(warehouse_id != -1);
							if(verbose)
								cerr << "[WARNING] order loader loading abort" << endl;
						}
#endif
					} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
						db->abort_txn(txn);
#endif
						ALWAYS_ASSERT(warehouse_id != -1);
						if(verbose)
							cerr << "[WARNING] order loader loading abort" << endl;
					}
				}
			}
		}

		if(verbose) {
			if(warehouse_id == -1) {
				cerr << "[INFO] finished loading order" << endl;
				cerr << "[INFO]   * average order_line record length: "
					 << (double(order_line_total_sz) / double(n_order_lines)) << " bytes" << endl;
				cerr << "[INFO]   * average oorder record length: "
					 << (double(oorder_total_sz) / double(n_oorders)) << " bytes" << endl;
				cerr << "[INFO]   * average new_order record length: "
					 << (double(new_order_total_sz) / double(n_new_orders)) << " bytes" << endl;
			} else {
				cerr << "[INFO] finished loading order (w=" << warehouse_id << ")" << endl;
			}
		}
	}

private:
	ssize_t warehouse_id;
};

tpcc_worker::txn_result
tpcc_worker::txn_new_order(bool first_run) {
	//cout << "[Alex]txn_new_order" <<endl;
	uint64_t elapse;
	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
	const uint customerID = GetCustomerId(r);
	const uint numItems = RandomNumber(r, 5, 15);
	//const uint numItems = 1;
	uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
	bool allLocal = true;
	for(uint i = 0; i < numItems; i++) {
		itemIDs[i] = GetItemId(r);
		if(likely(g_disable_xpartition_txn ||
				  NumWarehouses() == 1 ||
				  RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
			supplierWarehouseIDs[i] = warehouse_id;
		} else {
			do {
				supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
			} while(supplierWarehouseIDs[i] == warehouse_id);
			allLocal = false;
		}
		orderQuantities[i] = RandomNumber(r, 1, 10);
	}
	INVARIANT(!g_disable_xpartition_txn || allLocal);

	// XXX(stephentu): implement rollback
	//
	// worst case txn profile:
	//   1 customer get
	//   1 warehouse get
	//   1 district get
	//   1 new_order insert
	//   1 district put
	//   1 oorder insert
	//   1 oorder_cid_idx insert
	//   15 times:
	//      1 item get
	//      1 stock get
	//      1 stock put
	//      1 order_line insert
	//
	// output from txn counters:
	//   max_absent_range_set_size : 0
	//   max_absent_set_size : 0
	//   max_node_scan_size : 0
	//   max_read_set_size : 15
	//   max_write_set_size : 15
	//   num_txn_contexts : 9
#if 0
	void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCC_NEW_ORDER);
#endif
//  scoped_str_arena s_arena(arena);
//  scoped_multilock<spinlock> mlock;
//  char dummy[sizeof(customer::value)];
	try {
		uint64_t slstart ;
//  while (true) {
//  slstart = rdtsc();
#if DBTX_TIME
		timer txn_tim;
#endif
		tx.Begin();
#if DBTX_TIME
		elapse = txn_tim.lap();
		atomic_inc64(&txns[NEW_ORDER]);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
#endif
//	secs += (rdtsc() - slstart);
		ssize_t ret = 0;
		uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);
#if 0
#if SHORTKEY
		const customer::key k_c(makeCustomerKey(warehouse_id, districtID, customerID));
#else
		const customer::key k_c(warehouse_id, districtID, customerID);
#endif
		ALWAYS_ASSERT(tbl_customer(warehouse_id)->get(txn, Encode(obj_key0, k_c), obj_v));
#endif
		uint64_t *c_value;
//	slstart = rdtsc();
#if DBTX_TIME
		txn_tim.lap();
#endif
		bool found = tx.Get(CUST, c_key, &c_value, 1);//Tx.1
		assert(found);

		//printf("Get[CUST] key = %lu\n", c_key);

#if DBTX_TIME
#if DBTX_PROF
		Op_prof[CUST].gets++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[0], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
#endif
		//memcpy(dummy,c_value, sizeof(customer::value));
		//secs += (rdtsc() - slstart);

		customer::value *v_c = (customer::value *)c_value;

		checker::SanityCheckCustomer(NULL, v_c);
#if 0
		checker::SanityCheckCustomer(&k_c, v_c);

		const warehouse::key k_w(warehouse_id);
		ALWAYS_ASSERT(tbl_warehouse(warehouse_id)->get(txn, Encode(obj_key0, k_w), obj_v));
#endif
		uint64_t *w_value;
//	slstart = rdtsc();
#if DBTX_TIME
		txn_tim.lap();
#endif
		found = tx.Get(WARE, warehouse_id, &w_value, 2);//Tx.2
		//printf("warehouse_id = %u\n", warehouse_id);
		assert(found);
		
#if DBTX_TIME
#if DBTX_PROF
		Op_prof[WARE].gets++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[1], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
#endif
//	memcpy(dummy, w_value, sizeof(warehouse::value));
//	secs += (rdtsc() - slstart);
		
		warehouse::value *v_w = (warehouse::value *)w_value;
		checker::SanityCheckWarehouse(NULL, v_w);
#if 0
		checker::SanityCheckWarehouse(&k_w, v_w);

#if SHORTKEY
		const district::key k_d(makeDistrictKey(warehouse_id, districtID));
#else
		const district::key k_d(warehouse_id, districtID);
#endif
		ALWAYS_ASSERT(tbl_district(warehouse_id)->get(txn, Encode(obj_key0, k_d), obj_v));
#endif
		uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

		//printf("d_key = %lu, warehouse_id = %u, districtID = %u\n", d_key, warehouse_id, districtID);

		uint64_t *d_value;
		//slstart = rdtsc();
#if DBTX_TIME
		txn_tim.lap();
#endif
		uint64_t next_order_line_id;

#if NEW_INTERFACE
		if(first_run){
			found = tx.Atomic_Fetch(DIST, d_key, &d_value, &next_order_line_id); //Tx.3 (atomic fetch version)
			last_order_line_id_list[warehouse_id - warehouse_id_start] = next_order_line_id;
		}else{
			next_order_line_id = last_order_line_id_list[warehouse_id - warehouse_id_start];
		}
#else
		found = tx.Get(DIST, d_key, &d_value, 3);//Tx.3
#endif
		
		//printf("[%2d] d_key = %lu, warehouse_id = %u, districtID = %u\n", sched_getcpu(), d_key, warehouse_id, districtID);
#if DBTX_TIME
	#if DBTX_PROF
		Op_prof[DIST].gets++;
	#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[2], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
#endif
		//memcpy(dummy, d_value, sizeof(district::value));
		//secs += (rdtsc() - slstart);
		assert(found); //should be removed for new interface
		district::value *v_d = (district::value *)d_value;

		//printf("d_key = %lu, v_d->d_next_o_id = %d\n", d_key, v_d->d_next_o_id);

		checker::SanityCheckDistrict(NULL, v_d);
#if 0
		checker::SanityCheckDistrict(&k_d, v_d);
#endif
		if((uint64_t)v_d == 0x7f3938373635) {
			DBTables::DEBUGGC();
			printf("txn_new_order key %lu v_d %p\n", d_key, v_d);
			fflush(stdout);
		}
		
#if NEW_INTERFACE
		const uint64_t my_next_o_id = g_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID) : next_order_line_id;
#else
		const uint64_t my_next_o_id = g_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID) : v_d->d_next_o_id;
#endif
		//printf("[%2d] district_id = %d, first_run = %d, oid %ld\n", sched_getcpu(), districtID, first_run, my_next_o_id);
#if 0
#if SHORTKEY
		const new_order::key k_no(makeNewOrderKey(warehouse_id, districtID, my_next_o_id));
#else
		const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
#endif
#endif

#if RANDOM_KEY
		uint64_t no_key = RandomNumber(r, 1, 10000000);
#else
		uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
#endif
		const new_order::value v_no;
		//const size_t new_order_sz = Size(v_no);
//	slstart = rdtsc();
#if DBTX_TIME
		txn_tim.lap();
#endif
		tx.Add(NEWO, no_key, (uint64_t *)(&v_no), sizeof(v_no));//Tx.4
#if DBTX_TIME
	#if DBTX_PROF
		Op_prof[NEWO].adds++;
	#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[3], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
#endif
//	secs += (rdtsc() - slstart);
#if 0
		tbl_new_order(warehouse_id)->insert(txn, Encode(str(), k_no), Encode(str(), v_no));
#endif

#if !NEW_INTERFACE
//   ret += new_order_sz;
		if(!g_new_order_fast_id_gen) {
			district::value v_d_new(*v_d);
			v_d_new.d_next_o_id++;
//	  slstart = rdtsc();
	#if DBTX_TIME
			txn_tim.lap();
	#endif
			tx.Add(DIST, d_key, (uint64_t *)(&v_d_new), sizeof(v_d_new));//Tx.5
	#if DBTX_TIME
		#if DBTX_PROF
			Op_prof[DIST].adds++;
		#endif
			elapse = txn_tim.lap();
			atomic_add64(&newo_txn_time[4], elapse);
			atomic_add64(&dbtx_time[NEW_ORDER],elapse);
	#endif
//	  secs += (rdtsc() - slstart);
	#if 0
			tbl_district(warehouse_id)->put(txn, Encode(str(), k_d), Encode(str(), v_d_new));
	#endif
		} else printf("en?\n");
#endif

#if 0
#if SHORTKEY
		const oorder::key k_oo(makeOrderKey(warehouse_id, districtID, my_next_o_id));
#else
		const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
#endif
#endif

		uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

		oorder::value v_oo;
		v_oo.o_c_id = int32_t(customerID);
		v_oo.o_carrier_id = 0; // seems to be ignored
		v_oo.o_ol_cnt = int8_t(numItems);
		v_oo.o_all_local = allLocal;
		v_oo.o_entry_d = GetCurrentTimeMillis();
		// const size_t oorder_sz = Size(v_oo);
#if 0
		tbl_oorder(warehouse_id)->insert(txn, Encode(str(), k_oo), Encode(str(), v_oo));
#endif
//   ret += oorder_sz;
#if 0
#if SHORTKEY
		const oorder_c_id_idx::key k_oo_idx(makeOrderIndex(warehouse_id, districtID, customerID,
											my_next_o_id));
#else
		const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID, k_no.no_o_id);
#endif
#endif
		uint64_t o_sec = makeOrderIndex(warehouse_id, districtID, customerID, my_next_o_id);
		
#if SEC_INDEX //True
	#if USESECONDINDEX
		tx.Add(ORDE, ORDER_INDEX, o_key, o_sec, (uint64_t *)(&v_oo), sizeof(v_oo));
	#else
//		  slstart = rdtsc();
		#if DBTX_TIME
		txn_tim.lap();
		#endif
		tx.Add(ORDE, o_key, (uint64_t *)(&v_oo), sizeof(v_oo));//Tx.6
		#if DBTX_TIME
			#if DBTX_PROF
		Op_prof[ORDE].adds++;
			#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[5], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
		#endif
//		  secs += (rdtsc() - slstart);
		uint64_t *value;
//		  slstart = rdtsc();
		#if DBTX_TIME
		txn_tim.lap();
		#endif
		bool f = tx.Get(ORDER_INDEX, o_sec, &value,4);//Tx.7
		#if DBTX_TIME
			#if DBTX_PROF
		Op_prof[ORDER_INDEX].gets++;
			#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[6], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
		#endif
//		secs += (rdtsc() - slstart);
		if(f) {
			int num = value[0];
			uint64_t *prikeys = new uint64_t[num + 2];
			prikeys[0] = num + 1;
			for(int i = 1; i <= num; i++)
				prikeys[i] = value[i];
			prikeys[num + 1] = o_key;
//			slstart = rdtsc();
		#if DBTX_TIME
			txn_tim.lap();
		#endif
			tx.Add(ORDER_INDEX, o_sec, prikeys, (num + 2) * 8);//Tx.8 [mutually exclusive with Tx.9]
		#if DBTX_TIME
			#if DBTX_PROF
			Op_prof[ORDER_INDEX].adds++;
			#endif
			elapse = txn_tim.lap();
			atomic_add64(&newo_txn_time[7], elapse);
			atomic_add64(&dbtx_time[NEW_ORDER],elapse);
		#endif
//			secs += (rdtsc() - slstart);
		} else {
			uint64_t array_dummy[2];
			array_dummy[0] = 1;
			array_dummy[1] = o_key;
//			slstart = rdtsc();
		#if DBTX_TIME
			txn_tim.lap();
		#endif
			tx.Add(ORDER_INDEX, o_sec, array_dummy, 16);//Tx.9 [mutually exclusive with Tx.8]
		#if DBTX_TIME
			#if DBTX_PROF
			Op_prof[ORDER_INDEX].adds++;
			#endif
			elapse = txn_tim.lap();
			atomic_add64(&newo_txn_time[8], elapse);
			atomic_add64(&dbtx_time[NEW_ORDER],elapse);
		#endif
//			secs += (rdtsc() - slstart);
		}
	#endif
#else
	#if DBTX_TIME
		txn_tim.lap();
	#endif
		tx.Add(ORDE, o_key, (uint64_t *)(&v_oo), oorder_sz);//Tx.10 [Never executed]
	#if DBTX_TIME
		#if DBTX_PROF
		Op_prof[ORDE].adds++;
		#endif
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[9], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER],elapse);
	#endif
#endif

#if 0
		tbl_oorder_c_id_idx(warehouse_id)->insert(txn, Encode(str(), k_oo_idx), Encode(str(), v_oo_idx));
#endif
		for(uint ol_number = 1; ol_number <= numItems; ol_number++) { //numItems: [5,15]
			const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
			const uint ol_i_id = itemIDs[ol_number - 1];
			const uint ol_quantity = orderQuantities[ol_number - 1];
#if 0
			const item::key k_i(ol_i_id);
			ALWAYS_ASSERT(tbl_item(1)->get(txn, Encode(obj_key0, k_i), obj_v));
#endif
			uint64_t* i_value;
//	  slstart = rdtsc();
#if DBTX_TIME
			txn_tim.lap();
#endif
			found = tx.ROGet(ITEM, ol_i_id, &i_value,5);//Tx.11
			assert(found);
#if DBTX_TIME
#if DBTX_PROF
			Op_prof[ITEM].gets++;
#endif
			elapse = txn_tim.lap();
			atomic_add64(&newo_txn_time[10], elapse);
			atomic_add64(&dbtx_time[NEW_ORDER], elapse);
			atomic_add64(&item_time, elapse);
#endif

//	  memcpy(dummy, i_value, sizeof(item::value));
//	  secs += (rdtsc() - slstart);
			item::value *v_i = (item::value *)i_value;
			checker::SanityCheckItem(NULL, v_i);
#if 0
			checker::SanityCheckItem(&k_i, v_i);
#if SHORTKEY
			const stock::key k_s(makeStockKey(ol_supply_w_id, ol_i_id));
#else
			const stock::key k_s(ol_supply_w_id, ol_i_id);
#endif
			ALWAYS_ASSERT(tbl_stock(ol_supply_w_id)->get(txn, Encode(obj_key0, k_s), obj_v));
#endif

			uint64_t s_key = makeStockKey(ol_supply_w_id, ol_i_id);
			//printf("[%2d] s_key = %lu\n", sched_getcpu(), s_key);
			uint64_t* s_value;
//	  slstart = rdtsc();
#if DBTX_TIME
			txn_tim.lap();
#endif
			found = tx.Get(STOC, s_key, &s_value,6); //Tx.12
			assert(found);
#if DBTX_TIME
#if DBTX_PROF
			Op_prof[STOC].gets++;
#endif
			elapse = txn_tim.lap();
			atomic_add64(&newo_txn_time[11], elapse);
			atomic_add64(&dbtx_time[NEW_ORDER], elapse);
			atomic_add64(&stoc_time, elapse);
#endif

#if TPCC_DUMP
			printf("[GET] coreid = %2d, tableid = %2d, key = %12d\n", sched_getcpu(), STOC, s_key);
#endif

//	  memcpy(dummy, s_value, sizeof(stock::value));
//	  secs += (rdtsc() - slstart);
			stock::value *v_s = (stock::value *)s_value;
			checker::SanityCheckStock(NULL, v_s);
#if 0
			checker::SanityCheckStock(&k_s, v_s);
#endif
			stock::value v_s_new(*v_s);
			if(v_s_new.s_quantity - ol_quantity >= 10)
				v_s_new.s_quantity -= ol_quantity;
			else
				v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
			v_s_new.s_ytd += ol_quantity;
			v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;
//	  slstart = rdtsc();
#if DBTX_TIME
			txn_tim.lap();
#endif
			//printf("s_key = %lu\n", s_key);
			tx.Add(STOC, s_key, (uint64_t *)(&v_s_new), sizeof(v_s_new));//Tx.13
#if DBTX_TIME
	#if DBTX_PROF
			Op_prof[STOC].adds++;
	#endif
			//stocs++;
			elapse = txn_tim.lap();
			atomic_add64(&newo_txn_time[12], elapse);
			atomic_add64(&dbtx_time[NEW_ORDER],elapse);
			atomic_add64(&stoc_time,elapse);
#endif

#if TPCC_DUMP
			printf("[ADD] coreid = %2d, tableid = %2d, key = %12d\n", sched_getcpu(), STOC, s_key);
#endif
//	  secs += (rdtsc() - slstart);
#if 0
			tbl_stock(ol_supply_w_id)->put(txn, Encode(str(), k_s), Encode(str(), v_s_new));
#if SHORTKEY
			const order_line::key k_ol(makeOrderLineKey(warehouse_id, districtID,
									   my_next_o_id, ol_number));
#else
			const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id, ol_number);
#endif
#endif

#if RANDOM_KEY
			uint64_t ol_key = RandomNumber(r, 1, 100000000);
#else
			uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
#endif
			//printf("[%2d] ol_key = %lu, districtID = %2u, my_next_o_id = %u, ol_number = %u\n", 
			//	sched_getcpu(),ol_key,districtID,my_next_o_id,ol_number);

			order_line::value v_ol;
			v_ol.ol_i_id = int32_t(ol_i_id); //item ID
			v_ol.ol_delivery_d = 0; //not delivered yet
			v_ol.ol_amount = float(ol_quantity) * v_i->i_price; //total price of items
			v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
			v_ol.ol_quantity = int8_t(ol_quantity);
			//  const size_t order_line_sz = Size(v_ol);
			//  printf("key %lx q %d size %d \n", ol_key,v_ol.ol_quantity ,sizeof(v_ol));
//	  slstart = rdtsc();
#if DBTX_TIME
			txn_tim.lap();
#endif
			tx.Add_Label(ORLI, ol_key, (uint64_t *)(&v_ol), sizeof(v_ol));//Tx.14
#if DBTX_TIME
#if DBTX_PROF
			Op_prof[ORLI].adds++;
#endif
			//orlis++;
			elapse = txn_tim.lap();
			atomic_add64(&dbtx_time[NEW_ORDER], elapse);
			atomic_add64(&orli_time, elapse);
#endif
//	  secs += (rdtsc() - slstart);
#if 0
			tbl_order_line(warehouse_id)->insert(txn, Encode(str(), k_ol), Encode(str(), v_ol));
#endif
		}
//	slstart = rdtsc();
#if DBTX_TIME
		txn_tim.lap();
#endif
		bool b = tx.End();
#if DBTX_TIME
		elapse = txn_tim.lap();
		atomic_add64(&newo_txn_time[14], elapse);
		atomic_add64(&dbtx_time[NEW_ORDER], elapse);
#endif
//	secs += (rdtsc() - slstart);

#if 0
		measure_txn_counters(txn, "txn_new_order");

		if(likely(db->commit_txn(txn)))

			return txn_result(true, ret);
#endif
		return txn_result(b, ret);
	} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
		db->abort_txn(txn);
#endif
	}
	return txn_result(false, 0);
}

tpcc_worker::txn_result
tpcc_worker::txn_payment(bool first_run) {
	timer txn_tim;
	uint64_t elapse = 0;
	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
	uint customerDistrictID, customerWarehouseID;
	if(likely(g_disable_xpartition_txn ||
			  NumWarehouses() == 1 ||
			  RandomNumber(r, 1, 100) <= 85)) {
		customerDistrictID = districtID;
		customerWarehouseID = warehouse_id;
	} else {
		customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
		do {
			customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
		} while(customerWarehouseID == warehouse_id);
	}
	const float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
	const uint32_t ts = GetCurrentTimeMillis();
	INVARIANT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

	// output from txn counters:
	//   max_absent_range_set_size : 0
	//   max_absent_set_size : 0
	//   max_node_scan_size : 10
	//   max_read_set_size : 71
	//   max_write_set_size : 1
	//   num_txn_contexts : 5
#if 0
	void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCC_PAYMENT);
#endif
//  scoped_str_arena s_arena(arena);
	try {
		ssize_t ret = 0;
#if DBTX_TIME
		txn_tim.lap();
#endif
		tx.Begin();
#if DBTX_TIME
		atomic_add64(&dbtx_time[PAYMENT],txn_tim.lap());
#endif

#if 0
		const warehouse::key k_w(warehouse_id);
		ALWAYS_ASSERT(tbl_warehouse(warehouse_id)->get(txn, Encode(obj_key0, k_w), obj_v));
#endif
		uint64_t *w_value;

#if DBTX_TIME
		txn_tim.lap();
#endif

		tx.Get(WARE, warehouse_id, &w_value,7);//Tx.1
#if DBTX_TIME
#if DBTX_PROF
		Op_prof[WARE].gets++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&payment_txn_time[0], elapse);
		atomic_add64(&dbtx_time[PAYMENT], elapse);
#endif

		warehouse::value *v_w = (warehouse::value *)w_value;
		checker::SanityCheckWarehouse(NULL, v_w);
#if 0
		const warehouse::value *v_w = Decode(obj_v, v_w_temp);
		checker::SanityCheckWarehouse(&k_w, v_w);
#endif
		warehouse::value v_w_new(*v_w);
		v_w_new.w_ytd += paymentAmount;

#if DBTX_TIME
		txn_tim.lap();
#endif
		tx.Add(WARE, warehouse_id, (uint64_t *)(&v_w_new), sizeof(v_w_new));//Tx.2
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[WARE].adds++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&payment_txn_time[1], elapse);

		atomic_add64(&dbtx_time[PAYMENT], elapse);
#endif

#if 0
		tbl_warehouse(warehouse_id)->put(txn, Encode(str(), k_w), Encode(str(), v_w_new));
#if SHORTKEY
		const district::key k_d(makeDistrictKey(warehouse_id, districtID));
#else
		const district::key k_d(warehouse_id, districtID);
#endif
		ALWAYS_ASSERT(tbl_district(warehouse_id)->get(txn, Encode(obj_key0, k_d), obj_v));
#endif
		uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
		uint64_t *d_value;
#if DBTX_TIME
		txn_tim.lap();
#endif

		tx.Get(DIST, d_key, &d_value,8);//Tx.3
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[DIST].gets++;
#endif
		elapse = txn_tim.lap();

		atomic_add64(&payment_txn_time[2], elapse);

		atomic_add64(&dbtx_time[PAYMENT],elapse);
#endif

		district::value *v_d = (district::value *)d_value;
		checker::SanityCheckDistrict(NULL, v_d);

#if 0
		const district::value *v_d = Decode(obj_v, v_d_temp);
		checker::SanityCheckDistrict(&k_d, v_d);
#endif
		district::value v_d_new(*v_d);
		v_d_new.d_ytd += paymentAmount; //source of true conflict

#if DBTX_TIME
		txn_tim.lap();
#endif

		tx.Add(DIST, d_key, (uint64_t *)(&v_d_new), sizeof(v_d_new));//Tx.4
#if DBTX_TIME

#if DBTX_PROF
		Op_prof[DIST].adds++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&payment_txn_time[3], elapse);
		atomic_add64(&dbtx_time[PAYMENT],elapse);
#endif

#if 0
		tbl_district(warehouse_id)->put(txn, Encode(str(), k_d), Encode(str(), v_d_new));
#endif

#if 0
		customer::key k_c;
#endif
		uint64_t c_key;
		customer::value v_c;
		if(RandomNumber(r, 1, 100) <= 60) {
			// cust by name
			uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
			static_assert(sizeof(lastname_buf) == 16, "xx");
			NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
			GetNonUniformCustomerLastNameRun(lastname_buf, r);

			static const string zeros(16, 0);
			static const string ones(16, 255);

			string clast;
			clast.assign((const char *) lastname_buf, 16);
			uint64_t c_start = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, zeros);
			uint64_t c_end = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, ones);


#if USESECONDINDEX
			DBTX::SecondaryIndexIterator iter(&tx, CUST_INDEX);
#else
			DBTX::Iterator iter(&tx, CUST_INDEX);
#endif
#if DBTX_TIME
			txn_tim.lap();
#endif
			iter.Seek(c_start);//Tx.5
			
#if DBTX_TIME
#if DBTX_PROF
			Op_prof[CUST_INDEX].seeks++;
#endif
			elapse = txn_tim.lap();
			atomic_add64(&payment_txn_time[4], elapse);
			atomic_add64(&dbtx_time[PAYMENT],elapse);
#endif

#if USESECONDINDEX
			uint64_t *c_values[100];
#endif
			uint64_t c_keys[100];
			int j = 0;
			while(iter.Valid()) {
				if(compareCustomerIndex(iter.Key(), c_end)) {
#if USESECONDINDEX
					DBTX::KeyValues *kvs = iter.Value();
					int num = kvs->num;
					for(int i = 0; i < num; i++)  {
						c_values[j] = kvs->values[i];
						c_keys[j] = kvs->keys[i];
						j++;
					}
					delete kvs;
#else
					uint64_t *prikeys = iter.Value();
					int num = prikeys[0];
					for(int i = 1; i <= num; i++) {
						c_keys[j] = prikeys[i];
						j++;
					}
#endif
					if(j >= 100) {
						printf("P Array Full\n");
						exit(0);
					}
				} else break;
#if DBTX_TIME
				txn_tim.lap();
#endif

				iter.Next(); //Tx.6
#if DBTX_TIME
#if DBTX_PROF
				Op_prof[CUST_INDEX].nexts++;
#endif
				elapse = txn_tim.lap();
				atomic_add64(&payment_txn_time[5], elapse);
				atomic_add64(&dbtx_time[PAYMENT], elapse);
#endif
			}

			
			j = (j + 1) / 2 - 1;
			c_key = c_keys[j];
#if USESECONDINDEX
			uint64_t *c_value = c_values[j];
#else
			uint64_t *c_value;

#if DBTX_TIME
			txn_tim.lap();
#endif
			tx.Get(CUST, c_key, &c_value,9);//Tx.7 (mutually exclusive with Tx.8)
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[CUST].gets++;
#endif
			elapse = txn_tim.lap();
			atomic_add64(&payment_txn_time[6], elapse);
			atomic_add64(&dbtx_time[PAYMENT],elapse);
#endif

#endif
			v_c = *(customer::value *)c_value;
#if 0
			customer_name_idx::key k_c_idx_0;
#if SHORTKEY
			k_c_idx_0.c_index_id = customerWarehouseID * 10 + customerDistrictID;
#else
			k_c_idx_0.c_w_id = customerWarehouseID;
			k_c_idx_0.c_d_id = customerDistrictID;
#endif
			k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
			k_c_idx_0.c_first.assign(zeros);

			customer_name_idx::key k_c_idx_1;
#if SHORTKEY
			k_c_idx_1.c_index_id = customerWarehouseID * 10 + customerDistrictID;
#else
			k_c_idx_1.c_w_id = customerWarehouseID;
			k_c_idx_1.c_d_id = customerDistrictID;
#endif
			k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
			k_c_idx_1.c_first.assign(ones);

			static_limit_callback<NMaxCustomerIdxScanElems> c(s_arena.get(), true); // probably a safe bet for now
			tbl_customer_name_idx(customerWarehouseID)->scan(txn, Encode(obj_key0, k_c_idx_0), &Encode(obj_key1, k_c_idx_1), c, s_arena.get());
			ALWAYS_ASSERT(c.size() > 0);
			INVARIANT(c.size() < NMaxCustomerIdxScanElems); // we should detect this
			int index = c.size() / 2;
			if(c.size() % 2 == 0)
				index--;
			evt_avg_cust_name_idx_scan_size.offer(c.size());

			customer_name_idx::value v_c_idx_temp;
			const customer_name_idx::value *v_c_idx = Decode(*c.values[index].second, v_c_idx_temp);

#if SHORTKEY
			k_c.c_id = v_c_idx->c_id;
#else
			k_c.c_w_id = customerWarehouseID;
			k_c.c_d_id = customerDistrictID;
			k_c.c_id = v_c_idx->c_id;
#endif
			ALWAYS_ASSERT(tbl_customer(customerWarehouseID)->get(txn, Encode(obj_key0, k_c), obj_v));
			Decode(obj_v, v_c);
#endif
		} else {
			// cust by ID
			const uint customerID = GetCustomerId(r); //random number in [1,3000]
			c_key = makeCustomerKey(customerWarehouseID, customerDistrictID, customerID);
			uint64_t *c_value;

#if DBTX_TIME
			txn_tim.lap();
#endif

			tx.Get(CUST, c_key, &c_value,10);//Tx.8
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[CUST].gets++;
#endif
			elapse = txn_tim.lap();
			atomic_add64(&payment_txn_time[7], elapse);
			atomic_add64(&dbtx_time[PAYMENT], elapse);
#endif

			v_c = *(customer::value *)c_value;
#if 0
#if SHORTKEY
			k_c.c_id = makeCustomerKey(customerWarehouseID, customerDistrictID, customerID);
#else
			k_c.c_w_id = customerWarehouseID;
			k_c.c_d_id = customerDistrictID;
			k_c.c_id = customerID;
#endif
			ALWAYS_ASSERT(tbl_customer(customerWarehouseID)->get(txn, Encode(obj_key0, k_c), obj_v));
			Decode(obj_v, v_c);
#endif
		}
#if 0
		checker::SanityCheckCustomer(&k_c, &v_c);
#endif
		checker::SanityCheckCustomer(NULL, &v_c);
		customer::value v_c_new(v_c);

		v_c_new.c_balance -= paymentAmount;
		v_c_new.c_ytd_payment += paymentAmount;
		v_c_new.c_payment_cnt++;
		if(strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
			char buf[501];
#if 0
			int d_id = static_cast<int32_t>(k_c.c_id >> 32) % 10;
#endif
			int d_id = static_cast<int32_t>(c_key >> 32) % 10;
			if(d_id == 0) d_id = 10;
			int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
							 static_cast<int32_t>(c_key << 32 >> 32),
							 d_id,
							 (static_cast<int32_t>(c_key >> 32) - d_id) / 10,
#if 0
#if SHORTKEY
							 static_cast<int32_t>(k_c.c_id << 32 >> 32),
							 d_id,
							 (static_cast<int32_t>(k_c.c_id >> 32) - d_id) / 10,
#else
							 k_c.c_id,
							 k_c.c_d_id,
							 k_c.c_w_id,
#endif
#endif
							 districtID,
							 warehouse_id,
							 paymentAmount,
							 v_c.c_data.c_str());
			v_c_new.c_data.resize_junk(
				min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
			NDB_MEMCPY((void *) v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
		}
#if DBTX_TIME
					txn_tim.lap();
#endif
		tx.Add(CUST, c_key, (uint64_t *)(&v_c_new), sizeof(v_c_new));//Tx.9
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[CUST].adds++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&payment_txn_time[8], elapse);
		atomic_add64(&dbtx_time[PAYMENT],elapse);
#endif

#if 0
		tbl_customer(customerWarehouseID)->put(txn, Encode(str(), k_c), Encode(str(), v_c_new));

#if SHORTKEY
		int d_id = static_cast<int32_t>(k_c.c_id >> 32) % 10;
		if(d_id == 0) d_id = 10;
		const history::key k_h(makeHistoryKey(static_cast<int32_t>(k_c.c_id << 32 >> 32),
											  d_id, (static_cast<int32_t>(k_c.c_id >> 32) - d_id) / 10,
											  districtID, warehouse_id));
#else
		const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID, warehouse_id, ts);
#endif
#endif
		int d_id = static_cast<int32_t>(c_key >> 32) % 10;
		if(d_id == 0) d_id = 10;
		uint64_t h_key = makeHistoryKey(static_cast<int32_t>(c_key << 32 >> 32),
										d_id, (static_cast<int32_t>(c_key >> 32) - d_id) / 10,
										districtID, warehouse_id);
		history::value v_h;
#if SHORTKEY
		v_h.h_date = ts;
#endif
		v_h.h_amount = paymentAmount;
		v_h.h_data.resize_junk(v_h.h_data.max_size());
		int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
						 "%.10s    %.10s",
						 v_w->w_name.c_str(),
						 v_d->d_name.c_str());
		v_h.h_data.resize_junk(min(static_cast<size_t>(n), v_h.h_data.max_size()));

//    const size_t history_sz = Size(v_h);
#if 0
		tbl_history(warehouse_id)->insert(txn, Encode(str(), k_h), Encode(str(), v_h));
#endif

#if DBTX_TIME
		txn_tim.lap();
#endif
		tx.Add(HIST, h_key, (uint64_t *)(&v_h), sizeof(v_h));//Tx.10
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[HIST].adds++;
#endif
		elapse = txn_tim.lap();
		atomic_add64(&payment_txn_time[9], elapse);
		atomic_add64(&dbtx_time[PAYMENT], elapse);
#endif

		// ret += history_sz;
#if 0
		measure_txn_counters(txn, "txn_payment");
		if(likely(db->commit_txn(txn)))
			return txn_result(true, ret);
#endif
#if DBTX_TIME
		txn_tim.lap();
#endif
		bool res = tx.End();
#if DBTX_TIME
		atomic_add64(&dbtx_time[PAYMENT],txn_tim.lap());
#endif

		return txn_result(res, ret);
	} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
		db->abort_txn(txn);
#endif
	}
	return txn_result(false, 0);
}

#if 1
tpcc_worker::txn_result
tpcc_worker::txn_delivery(bool first_run) {
	timer txn_tim, orli_tim;

	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
	const uint32_t ts = GetCurrentTimeMillis();

	// worst case txn profile:
	//   10 times:
	//     1 new_order scan node
	//     1 oorder get
	//     2 order_line scan nodes
	//     15 order_line puts
	//     1 new_order remove
	//     1 oorder put
	//     1 customer get
	//     1 customer put
	//
	// output from counters:
	//   max_absent_range_set_size : 0
	//   max_absent_set_size : 0
	//   max_node_scan_size : 21
	//   max_read_set_size : 133
	//   max_write_set_size : 133
	//   num_txn_contexts : 4
#if 0
	void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCC_DELIVERY);
#endif
//  scoped_str_arena s_arena(arena);
	try {
		ssize_t ret = 0;
#if DBTX_TIME
		txn_tim.lap();
#endif
		tx.Begin();
#if DBTX_TIME
		atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

		for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
			//printf("d = %u\n", d);
			int32_t no_o_id = 1;
			uint64_t *no_value;
			
			//uint64_t last_delete = tx.Fetch_last_dist_id(warehouse_id, d);
			int64_t start = makeNewOrderKey(warehouse_id, d, last_no_o_ids[warehouse_id - 1][d - 1]);
			int64_t end = makeNewOrderKey(warehouse_id, d, numeric_limits<int32_t>::max());

			int64_t no_key  = -1;
#if DBTX_TIME
			txn_tim.lap();
#endif
			//printf("start = %ld, end = %ld\n",start,end);

			DBTX::Iterator iter(&tx, NEWO);

			iter.Seek(start); //Tx.1 

			bool valid = iter.Valid();

#if DBTX_TIME
#if DBTX_PROF
			Op_prof[NEWO].seeks++;
#endif
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif
			if(valid) {
				no_key = iter.Key();
				if(no_key <= end) {
//			  no_value = iter.Value();
					no_o_id = static_cast<int32_t>(no_key << 32 >> 32);//lower 32 bits
#if DBTX_TIME
					txn_tim.lap();
#endif
					//printf("Before Tx2\n");
					tx.Delete(NEWO, no_key);//Tx.2
					//printf("After Tx2\n");
#if DBTX_TIME
#if DBTX_PROF
					Op_prof[NEWO].dels++;
#endif
					atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif
				} else no_key = -1;
			}
			if(no_key == -1) {
				printf("NoOrder for district %d!!\n",  d);
#if DBTX_TIME
				txn_tim.lap();
#endif
				//printf("Before Tx3\n");
				iter.SeekToFirst();//Tx.3
				//printf("After Tx3\n");
#if DBTX_TIME
#if DBTX_PROF

				Op_prof[NEWO].seeks++;
#endif
				atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

				printf("Key %ld\n", iter.Key());
				continue;
			}

#if 0
#if SHORTKEY
			const new_order::key k_no_0(makeNewOrderKey(warehouse_id, d, last_no_o_ids[d - 1]));
			const new_order::key k_no_1(makeNewOrderKey(warehouse_id, d, numeric_limits<int32_t>::max()));
#else
			const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
			const new_order::key k_no_1(warehouse_id, d, numeric_limits<int32_t>::max());
#endif
			new_order_scan_callback new_order_c;
			{
				ANON_REGION("DeliverNewOrderScan:", &delivery_probe0_cg);
				tbl_new_order(warehouse_id)->scan(txn, Encode(obj_key0, k_no_0), &Encode(obj_key1, k_no_1), new_order_c, s_arena.get());
			}

			const new_order::key *k_no = new_order_c.get_key();
			if(unlikely(!k_no))
				continue;
#if SHORTKEY
			int32_t no_o_id = static_cast<int32_t>(k_no->no_id << 32 >> 32);
#else
			int32_t no_o_id = k_no->no_o_id;
#endif
#endif
			/*
			if(no_o_id!=-1){
				tx.Store_last_dist_id(warehouse_id, d, no_o_id);
			}
			*/
			last_no_o_ids[warehouse_id - 1][d - 1] = no_o_id + 1; // XXX: update last seen
#if 0
#if SHORTKEY
			const oorder::key k_oo(makeOrderKey(warehouse_id, d, no_o_id));
#else
			const oorder::key k_oo(warehouse_id, d, no_o_id);
#endif
			if(unlikely(!tbl_oorder(warehouse_id)->get(txn, Encode(obj_key0, k_oo), obj_v))) {
				// even if we read the new order entry, there's no guarantee
				// we will read the oorder entry: in this case the txn will abort,
				// but we're simply bailing out early
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
#endif
			uint64_t o_key = makeOrderKey(warehouse_id, d, no_o_id);
			uint64_t* o_value;
#if DBTX_TIME
			txn_tim.lap();
#endif
			//printf("Before Tx4\n");
			bool found = tx.Get(ORDE, o_key, &o_value); //Tx.4
			//printf("After Tx4 found = %s\n", found ? "true":"false");
			if(!found){
				return txn_result(false, 0);
			}
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[ORDE].gets++;
#endif
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif
			oorder::value *v_oo = (oorder::value *)o_value;

			float sum_ol_amount = 0;
#if DBTX_TIME
			txn_tim.lap();
#endif

			DBTX::Iterator iter1(&tx, ORLI);

#if DBTX_TIME
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif
			start = makeOrderLineKey(warehouse_id, d, no_o_id, 1);
#if DBTX_TIME
			txn_tim.lap();
#endif
			//printf("Before Tx5\n");
			iter1.Seek(start);//Tx.5
			//printf("After Tx5\n");
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[ORLI].seeks++;
#endif
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

			end = makeOrderLineKey(warehouse_id, d, no_o_id, 15);

			while(true) {
				//printf("Before valid\n");
				if(!iter.Valid()){
					break;
				}
				//printf("After valid\n");
				//printf("before getkey\n");
				int64_t ol_key = iter1.Key();
				//printf("after getkey. ol_key = %ld. ol_value = %p. start = %ld. end = %ld\n", ol_key, iter1.Value(), start, end);
				if(ol_key > end){ 
					//printf("ol_key = %ld, end = %ld, break!\n", ol_key, end);
					break;
				}

				uint64_t *ol_value = iter1.Value();
				//printf("ol_value = %p\n", ol_value);
				order_line::value *v_ol = (order_line::value *)ol_value;
				//printf("v_ol = %p\n", v_ol);
				sum_ol_amount += v_ol->ol_amount;
				//printf("sum_ol_amount = %f\n", sum_ol_amount);
				order_line::value v_ol_new(*v_ol);
				v_ol_new.ol_delivery_d = ts;
#if DBTX_TIME
				txn_tim.lap();
#endif
				//printf("Before Tx6\n");
				tx.Add(ORLI, ol_key, (uint64_t *)(&v_ol_new), sizeof(v_ol_new)); //Tx.6
				//printf("Before Tx6\n");
#if DBTX_TIME
#if DBTX_PROF

				Op_prof[ORLI].adds++;
#endif
				//atomic_add64(&orli_time2, orli_tim.lap());
#endif
				iter1.Next(); //Tx.7

#if DBTX_TIME
#if DBTX_PROF

				Op_prof[ORLI].nexts++;
#endif
				atomic_add64(&dbtx_time[DELIVERY], txn_tim.lap());
#endif
			}
#if 0
			const oorder::value *v_oo = Decode(obj_v, v_oo_temp);
			checker::SanityCheckOOrder(&k_oo, v_oo);

			static_limit_callback<15> c(s_arena.get(), false); // never more than 15 order_lines per order
#if SHORTKEY
			const order_line::key k_oo_0(makeOrderLineKey(warehouse_id, d, no_o_id, 1));
			const order_line::key k_oo_1(makeOrderLineKey(warehouse_id, d, no_o_id, 15));
#else
			const order_line::key k_oo_0(warehouse_id, d, no_o_id, 0);
			const order_line::key k_oo_1(warehouse_id, d, no_o_id, numeric_limits<int32_t>::max());
#endif
			// XXX(stephentu): mutable scans would help here
			tbl_order_line(warehouse_id)->scan(txn, Encode(obj_key0, k_oo_0), &Encode(obj_key1, k_oo_1), c, s_arena.get());

			float sum = 0.0;
			for(size_t i = 0; i < c.size(); i++) {
				order_line::value v_ol_temp;
				const order_line::value *v_ol = Decode(*c.values[i].second, v_ol_temp);

#ifdef CHECK_INVARIANTS
				order_line::key k_ol_temp;
				const order_line::key *k_ol = Decode(*c.values[i].first, k_ol_temp);
				checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

				sum += v_ol->ol_amount;
				order_line::value v_ol_new(*v_ol);
				v_ol_new.ol_delivery_d = ts;
				INVARIANT(s_arena.get()->manages(c.values[i].first));
				tbl_order_line(warehouse_id)->put(txn, *c.values[i].first, Encode(str(), v_ol_new));
			}

			// delete new order
			tbl_new_order(warehouse_id)->remove(txn, Encode(str(), *k_no));
			ret -= 0 /*new_order_c.get_value_size()*/;
#endif
			//printf("I must reach here\n");
			// update oorder
			oorder::value v_oo_new(*v_oo);
			v_oo_new.o_carrier_id = o_carrier_id;
#if DBTX_TIME
			txn_tim.lap();
#endif
			
			tx.Add(ORDE, o_key, (uint64_t *)(&v_oo_new), sizeof(v_oo_new)); //Tx.8
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[ORDE].adds++;
#endif
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

#if 0
			tbl_oorder(warehouse_id)->put(txn, Encode(str(), k_oo), Encode(str(), v_oo_new));
#endif
			const uint c_id = v_oo->o_c_id;
			const float ol_total = sum_ol_amount;
#if 0
			const float ol_total = sum;

			// update customer
#if SHORTKEY
			const customer::key k_c(makeCustomerKey(warehouse_id, d, c_id));
#else
			const customer::key k_c(warehouse_id, d, c_id);
#endif
			ALWAYS_ASSERT(tbl_customer(warehouse_id)->get(txn, Encode(obj_key0, k_c), obj_v));
#endif
			uint64_t c_key = makeCustomerKey(warehouse_id, d, c_id);
			uint64_t *c_value;
#if DBTX_TIME
			txn_tim.lap();
#endif
			tx.Get(CUST, c_key, &c_value); //Tx.9
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[CUST].gets++;
#endif
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

			customer::value *v_c = (customer::value *)c_value;
#if 0
			const customer::value *v_c = Decode(obj_v, v_c_temp);
#endif
			customer::value v_c_new(*v_c);
			v_c_new.c_balance += ol_total;

#if DBTX_TIME
			txn_tim.lap();
#endif

			tx.Add(CUST, c_key, (uint64_t *)(&v_c_new), sizeof(v_c_new)); //Tx.10
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[CUST].adds++;
#endif
			atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

#if 0
			tbl_customer(warehouse_id)->put(txn, Encode(str(), k_c), Encode(str(), v_c_new));
#endif
		}//Loop End
#if 0
		measure_txn_counters(txn, "txn_delivery");
		if(likely(db->commit_txn(txn)))
			return txn_result(true, ret);
#endif

#if DBTX_TIME
		txn_tim.lap();
#endif
		//printf("Before End\n");
		bool res = tx.End();
		//printf("After End\n");
#if DBTX_TIME
		
		atomic_add64(&dbtx_time[DELIVERY],txn_tim.lap());
#endif

		return txn_result(res, ret);
	} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
		db->abort_txn(txn);
#endif
	}
	return txn_result(false, 0);
}
#else

tpcc_worker::txn_result
tpcc_worker::txn_delivery(bool first_run) {
	timer txn_tim, orli_tim, piece_tim;
	uint64_t elapse = 0;
	uint64_t piece_elapse = 0;
	INVARIANT(NumDistrictsPerWarehouse() == 10);
	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
	const uint32_t ts = GetCurrentTimeMillis();

	// worst case txn profile:
	//   10 times:
	//     1 new_order scan node
	//     1 oorder get
	//     2 order_line scan nodes
	//     15 order_line puts
	//     1 new_order remove
	//     1 oorder put
	//     1 customer get
	//     1 customer put
	//
	// output from counters:
	//   max_absent_range_set_size : 0
	//   max_absent_set_size : 0
	//   max_node_scan_size : 21
	//   max_read_set_size : 133
	//   max_write_set_size : 133
	//   num_txn_contexts : 4
//  scoped_str_arena s_arena(arena);
	try {
		ssize_t retries = 0;
		int32_t last_no_o_id[10];
		uint c_ids[10];
		float ol_totals[10];
		tx.Begin();

		ssize_t local_retries = 0;

		bool first_run = true;
		
NEWORDER_PIECE:
		for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
			//printf("d = %u\n", d);
			int32_t no_o_id = 1;
			uint64_t *no_value;
			
			uint64_t last_delete = tx.Fetch_last_dist_id(warehouse_id, d);
			//int64_t start = makeNewOrderKey(warehouse_id, d, dist_last_id[warehouse_id - 1][d - 1]);
			int64_t start = makeNewOrderKey(warehouse_id, d, last_delete);
			int64_t end = makeNewOrderKey(warehouse_id, d, numeric_limits<int32_t>::max());
			int64_t no_key  = -1;

			DBTX::Iterator iter(&tx, NEWO);
			
			iter.Seek(start); //Tx.1
			//printf("[DELIVERY] d = %2d, start = %ld\n",d, start);

			bool valid = iter.Valid();

			no_key = iter.Key();

			if(unlikely(!valid || no_key > end)) {
				last_no_o_id[d - 1] = -1;
				printf("[DELIVERY] SeekToFirst\n");
				iter.SeekToFirst();
				continue;
			}
			
			last_no_o_id[d - 1] = static_cast<int32_t>(no_key << 32 >> 32); //memorize the deleted key from the last iteration

			tx.Delete(NEWO, no_key); //Tx.2

		}
		for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
			if(last_no_o_id[d - 1] != -1) {
				tx.Store_last_dist_id(warehouse_id, d, last_no_o_id[d - 1]);
			}
		}

ORDERLINE_PIECE:
		for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
			if(last_no_o_id[d - 1] == -1) {
				continue;
			}
			int no_o_id = last_no_o_id[d - 1];

			uint64_t o_key = makeOrderKey(warehouse_id, d, no_o_id); //key of ORDER <- no_o_id
			//printf("o_key = %d\n", no_o_id);

			uint64_t* o_value;

			bool found = tx.Get(ORDE, o_key, &o_value); //Tx.3
			// even if we read the new order entry, there's no guarantee
			// we will read the oorder entry: in this case the txn will abort,
			// but we're simply bailing out early
			if(unlikely(!found)) {
				continue;
			}

			oorder::value *v_oo = (oorder::value *)o_value;

			float sum_ol_amount = 0;

			DBTX::Iterator iter1(&tx, ORLI);

			int64_t start = makeOrderLineKey(warehouse_id, d, no_o_id, 1);
			int64_t end = makeOrderLineKey(warehouse_id, d, no_o_id, 15);

			iter1.Seek(start);//Tx.4

			while(true) {
				//printf("before getkey\n");
				int64_t ol_key = iter1.Key();
				//printf("after getkey. ol_key = %ld. start = %ld. end = %ld\n", ol_key, start,end);

				if(ol_key > end) {
					//printf("ol_key = %ld, end = %ld, break!\n", ol_key, end);
					break;
				}

				uint64_t *ol_value = iter1.Value();
				order_line::value *v_ol = (order_line::value *)ol_value;
				sum_ol_amount += v_ol->ol_amount;
				order_line::value v_ol_new(*v_ol);
				v_ol_new.ol_delivery_d = ts;
				

				tx.Add(ORLI, ol_key, (uint64_t *)(&v_ol_new), sizeof(v_ol_new)); //Tx.5

				iter1.Next(); //Tx.6

			}

			//printf("I must reach here\n");
			// update oorder
			oorder::value v_oo_new(*v_oo);
			v_oo_new.o_carrier_id = o_carrier_id;


			tx.Add(ORDE, o_key, (uint64_t *)(&v_oo_new), sizeof(v_oo_new)); //Tx.7

			c_ids[d - 1] = v_oo->o_c_id;
			ol_totals[d - 1] = sum_ol_amount;
		}

CUST_PIECE:
		for(uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
			if(last_no_o_id[d - 1] == -1) {
				continue;
			}

			const uint c_id = c_ids[d - 1];
			const float ol_total = ol_totals[d - 1];

			uint64_t c_key = makeCustomerKey(warehouse_id, d, c_id); //key of CUST <- no_o_id
			uint64_t *c_value;
			bool found = tx.Get(CUST, c_key, &c_value); //Tx.8

			INVARIANT(found);

			customer::value *v_c = (customer::value *)c_value;
			customer::value v_c_new(*v_c);
			v_c_new.c_balance += ol_total;

			tx.Add(CUST, c_key, (uint64_t *)(&v_c_new), sizeof(v_c_new)); //Tx.9
			//printf("[DELIVERY] c_key = %lu\n", c_key);
		}//Loop End

		bool res = tx.End();
		return txn_result(res, retries);
	} catch(abstract_db::abstract_abort_exception &ex) {
	}
	return txn_result(false, 0);
}


#endif
tpcc_worker::txn_result
tpcc_worker::txn_order_status(bool first_run) {
	timer txn_tim;
	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

	// output from txn counters:
	//   max_absent_range_set_size : 0
	//   max_absent_set_size : 0
	//   max_node_scan_size : 13
	//   max_read_set_size : 81
	//   max_write_set_size : 0
	//   num_txn_contexts : 4
#if 0
	void *txn = db->new_txn(txn_flags | read_only_mask, arena, txn_buf(), hint);
#endif
/*
#if SLDBTX
	leveldb::DBTX txos(store);
#else
	leveldb::DBROTX txos(store);
#endif
*/
//  scoped_str_arena s_arena(arena);
	// NB: since txn_order_status() is a RO txn, we assume that
	// locking is un-necessary (since we can just read from some old snapshot)
	try {
#if DBTX_TIME
		txn_tim.lap();
#endif
		rotx.Begin();
#if DBTX_TIME
		atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif

		uint64_t c_key;
		customer::value v_c;
		if(RandomNumber(r, 1, 100) <= 60) {
			// cust by name
			uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
			static_assert(sizeof(lastname_buf) == 16, "xx");
			NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
			GetNonUniformCustomerLastNameRun(lastname_buf, r);

			static const string zeros(16, 0);
			static const string ones(16, 255);

			string clast;
			clast.assign((const char *) lastname_buf, 16);
			uint64_t c_start = makeCustomerIndex(warehouse_id, districtID, clast, zeros);
			uint64_t c_end = makeCustomerIndex(warehouse_id, districtID, clast, ones);
#if USESECONDINDEX
	#if SLDBTX

			DBTX::SecondaryIndexIterator citer(&rotx, CUST_INDEX);
	#else
			DBROTX::SecondaryIndexIterator citer(&rotx, CUST_INDEX);
	#endif
#else
	#if SLDBTX
			DBTX::Iterator citer(&rotx, CUST_INDEX);
	#else
			DBROTX::Iterator citer(&rotx, CUST_INDEX); //executed
	#endif
#endif
#if DBTX_TIME
			txn_tim.lap();
#endif
			citer.Seek(c_start);//Tx.1
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[CUST_INDEX].roseeks++;
#endif
			atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif

			uint64_t *c_values[100];
			uint64_t c_keys[100];
			int j = 0;
			while(citer.Valid()) {
				if(compareCustomerIndex(citer.Key(), c_end)) {
#if 0
					for(int i = 0; i < 38; i++)
						printf("%d ", ((char *)citer.Key())[i]);
					printf("\n");
#endif

#if USESECONDINDEX
#if SLDBTX

					DBTX::KeyValues *kvs = citer.Value();
#else
					DBROTX::KeyValues *kvs = citer.Value();
#endif
					int num = kvs->num;
					for(int i = 0; i < num; i++)  {
						c_values[j] = kvs->values[i];
						c_keys[j] = kvs->keys[i];
						//						printf("j %d\n",j);
						j++;
					}
					delete kvs;
#else
					uint64_t *prikeys = citer.Value();
					int num = prikeys[0];

					for(int i = 1; i <= num; i++) {
						c_keys[j] = prikeys[i];
						j++;
					}
#endif
					if(j >= 100) {
						printf("OS Array Full\n");
						exit(0);
					}
				} else break;
#if DBTX_TIME
				txn_tim.lap();
#endif

				citer.Next();//Tx.2
#if DBTX_TIME
#if DBTX_PROF

				Op_prof[CUST_INDEX].ronexts++;
#endif
				atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif

			}
			j = (j + 1) / 2 - 1;
			c_key = c_keys[j];
#if USESECONDINDEX
			uint64_t *c_value = c_values[j];
#else
			uint64_t *c_value;
	#if DBTX_TIME
			txn_tim.lap();
	#endif
			rotx.Get(CUST, c_key, &c_value);//Tx.3
	#if DBTX_TIME
		#if DBTX_PROF
			Op_prof[CUST].rogets++;
		#endif
			atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
	#endif

#endif
			v_c = *(customer::value *)c_value;
		}
		else {
			// cust by ID
			const uint customerID = GetCustomerId(r);
			c_key = makeCustomerKey(warehouse_id, districtID, customerID);
			uint64_t *c_value;
#if DBTX_TIME
			txn_tim.lap();
#endif
			rotx.Get(CUST, c_key, &c_value);//Tx.4
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[CUST].rogets++;
#endif
			atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif
			v_c = *(customer::value *)c_value;
		}
		checker::SanityCheckCustomer(NULL, &v_c);

#if 0
		customer::key k_c;
		customer::value v_c;
		if(RandomNumber(r, 1, 100) <= 60) {
			//cerr << "name" <<endl;
			// cust by name
			uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
			static_assert(sizeof(lastname_buf) == 16, "xx");
			NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
			GetNonUniformCustomerLastNameRun(lastname_buf, r);

			static const string zeros(16, 0);
			static const string ones(16, 255);
			customer_name_idx::key k_c_idx_0;
#if SHORTKEY
			k_c_idx_0.c_index_id = warehouse_id * 10 + districtID;
#else
			k_c_idx_0.c_w_id = warehouse_id;
			k_c_idx_0.c_d_id = districtID;
#endif
			k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
			k_c_idx_0.c_first.assign(zeros);

			customer_name_idx::key k_c_idx_1;
#if SHORTKEY
			k_c_idx_1.c_index_id = warehouse_id * 10 + districtID;
#else
			k_c_idx_1.c_w_id = warehouse_id;
			k_c_idx_1.c_d_id = districtID;
#endif
			k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
			k_c_idx_1.c_first.assign(ones);


			static_limit_callback<NMaxCustomerIdxScanElems> c(s_arena.get(), true); // probably a safe bet for now
			tbl_customer_name_idx(warehouse_id)->scan(txn, Encode(obj_key0, k_c_idx_0), &Encode(obj_key1, k_c_idx_1), c, s_arena.get());
			ALWAYS_ASSERT(c.size() > 0);
			INVARIANT(c.size() < NMaxCustomerIdxScanElems); // we should detect this
			int index = c.size() / 2;
			if(c.size() % 2 == 0)
				index--;
			evt_avg_cust_name_idx_scan_size.offer(c.size());

			customer_name_idx::value v_c_idx_temp;
			const customer_name_idx::value *v_c_idx = Decode(*c.values[index].second, v_c_idx_temp);
#if SHORTKEY
			k_c.c_id = v_c_idx->c_id;
#else
			k_c.c_w_id = warehouse_id;
			k_c.c_d_id = districtID;
			k_c.c_id = v_c_idx->c_id;
#endif

			ALWAYS_ASSERT(tbl_customer(warehouse_id)->get(txn, Encode(obj_key0, k_c), obj_v));
			Decode(obj_v, v_c);

		} else {
			// cust by ID
			//cerr << "id" <<endl;
			const uint customerID = GetCustomerId(r);
			//cerr << customerID << endl;
#if SHORTKEY
			k_c.c_id = makeCustomerKey(warehouse_id, districtID, customerID);
#else
			k_c.c_w_id = warehouse_id;
			k_c.c_d_id = districtID;
			k_c.c_id = customerID;
#endif

			ALWAYS_ASSERT(tbl_customer(warehouse_id)->get(txn, Encode(obj_key0, k_c), obj_v));
			Decode(obj_v, v_c);
		}
		checker::SanityCheckCustomer(&k_c, &v_c);
#endif

		// XXX(stephentu): HACK- we bound the # of elems returned by this scan to
		// 15- this is because we don't have reverse scans. In an ideal system, a
		// reverse scan would only need to read 1 btree node. We could simulate a
		// lookup by only reading the first element- but then we would *always*
		// read the first order by any customer.  To make this more interesting, we
		// randomly select which elem to pick within the 1st or 2nd btree nodes.
		// This is obviously a deviation from TPC-C, but it shouldn't make that
		// much of a difference in terms of performance numbers (in fact we are
		// making it worse for us)

		int32_t o_id = -1;
		int o_ol_cnt;
#if USESECONDINDEX
	#if SLDBTX
		DBTX::SecondaryIndexIterator iter(&rotx, ORDER_INDEX);
	#else
		DBROTX::SecondaryIndexIterator iter(&rotx, ORDER_INDEX);
	#endif
#else
	#if SLDBTX
		DBTX::Iterator iter(&rotx, ORDER_INDEX);
	#else
		DBROTX::Iterator iter(&rotx, ORDER_INDEX); //executed
	#endif
#endif

		uint64_t start = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 10000000 + 1);
		uint64_t end = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 1);
#if DBTX_TIME
		txn_tim.lap();
#endif
		iter.Seek(start);//Tx.5
		if(iter.Valid()){
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[ORDER_INDEX].roprevs++;
#endif
#endif
			iter.Prev();//Tx.6
		}else{
			printf("!!SeekOut\n");
		}
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[ORDER_INDEX].roseeks++;
#endif
		atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif

		//printf("okey %lx %d %d %lx\n", iter.Key(),warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32) );
		if(iter.Valid() && iter.Key() >= end) {
#if USESECONDINDEX
	#if SLDBTX
			DBTX::KeyValues *kvs = iter.Value();
	#else
			DBROTX::KeyValues *kvs = iter.Value();
	#endif
			o_id = static_cast<int32_t>(kvs->keys[0] << 32 >> 32);
			uint64_t *o_value = kvs->values[0];
#else
			//executed
			//std::vector<uint64_t> *prikeys = (std::vector<uint64_t> *)(iter.Value());
	#if DBTX_TIME
			txn_tim.lap();
	#endif

			uint64_t *prikeys = iter.Value();
	#if DBTX_TIME
			atomic_add64(&dbtx_time[ORDER_STATUS], txn_tim.lap());
	#endif
			o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);

			uint64_t *o_value;
	#if DBTX_TIME
			txn_tim.lap();
	#endif

			rotx.Get(ORDE, prikeys[1], &o_value);//Tx.7
	#if DBTX_TIME
		#if DBTX_PROF

			Op_prof[ORDE].rogets++;
		#endif
			atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
	#endif

			oorder::value *v_ol = (oorder::value *)o_value;
			o_ol_cnt = v_ol->o_ol_cnt;
#endif
		}

		if(o_id != -1) {
			for(int32_t line_number = 1; line_number <= o_ol_cnt; ++line_number) {
				uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, o_id, line_number);

				uint64_t *ol_value;
#if DBTX_TIME
				txn_tim.lap();
#endif
				bool found = rotx.Get(ORLI, ol_key, &ol_value);//Tx.8
#if DBTX_TIME
#if DBTX_PROF

				Op_prof[ORLI].rogets++;
#endif
				atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif
			}
		} //else printf("No order\n");

#if 0
		latest_key_callback c_oorder(str(), (r.next() % 15) + 1);
#if SHORTKEY
		//cerr << k_c.c_id << endl;
		//cerr << static_cast<int32_t>(k_c.c_id << 32 >> 32) <<endl;
		const oorder_c_id_idx::key k_oo_idx_0(makeOrderIndex(warehouse_id, districtID,
											  static_cast<int32_t>(k_c.c_id << 32 >> 32), 0));
		const oorder_c_id_idx::key k_oo_idx_1(makeOrderIndex(warehouse_id, districtID,
											  static_cast<int32_t>(k_c.c_id << 32 >> 32), numeric_limits<int32_t>::max()));
#else
		const oorder_c_id_idx::key k_oo_idx_0(warehouse_id, districtID, k_c.c_id, 0);
		const oorder_c_id_idx::key k_oo_idx_1(warehouse_id, districtID, k_c.c_id, numeric_limits<int32_t>::max());
#endif
		{
			ANON_REGION("OrderStatusOOrderScan:", &order_status_probe0_cg);
			tbl_oorder_c_id_idx(warehouse_id)->scan(txn, Encode(obj_key0, k_oo_idx_0), &Encode(obj_key1, k_oo_idx_1), c_oorder, s_arena.get());
		}
		ALWAYS_ASSERT(c_oorder.size());

		oorder_c_id_idx::key k_oo_idx_temp;
		const oorder_c_id_idx::key *k_oo_idx = Decode(c_oorder.kstr(), k_oo_idx_temp);
#if SHORTKEY
		const uint o_id = static_cast<uint32_t>(k_oo_idx->o_index_id << 32 >> 32);
#else
		const uint o_id = k_oo_idx->o_o_id;
#endif
		order_line_nop_callback c_order_line;
#if SHORTKEY

		const order_line::key k_ol_0(makeOrderLineKey(warehouse_id, districtID, o_id, 1));
		const order_line::key k_ol_1(makeOrderLineKey(warehouse_id, districtID, o_id, 15));
		//cerr << k_ol_0.ol_id << endl;
		//cerr << k_ol_1.ol_id << endl;
#else
		const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
		const order_line::key k_ol_1(warehouse_id, districtID, o_id, numeric_limits<int32_t>::max());
#endif
		tbl_order_line(warehouse_id)->scan(txn, Encode(obj_key0, k_ol_0), &Encode(obj_key1, k_ol_1), c_order_line, s_arena.get());
		//cerr << c_order_line.n <<endl;
		ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);

		measure_txn_counters(txn, "txn_order_status");
		if(likely(db->commit_txn(txn)))
			return txn_result(true, 0);
#endif
#if DBTX_TIME
		txn_tim.lap();
#endif

		bool res = rotx.End();
#if DBTX_TIME
		atomic_add64(&dbtx_time[ORDER_STATUS],txn_tim.lap());
#endif
		return txn_result(res, 0);
	} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
		db->abort_txn(txn);
#endif
	}
	return txn_result(false, 0);
}

tpcc_worker::txn_result
tpcc_worker::txn_stock_level(bool first_run) {
	timer txn_tim;
	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint threshold = RandomNumber(r, 10, 20);
	const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

	// output from txn counters:
	//   max_absent_range_set_size : 0
	//   max_absent_set_size : 0
	//   max_node_scan_size : 19
	//   max_read_set_size : 241
	//   max_write_set_size : 0
	//   n_node_scan_large_instances : 1
	//   n_read_set_large_instances : 2
	//   num_txn_contexts : 3
#if 0
	void *txn = db->new_txn(txn_flags | read_only_mask, arena, txn_buf(), hint);
#endif
/*
#if SLDBTX
	leveldb::DBTX txsl(store);
#else
	leveldb::DBROTX txsl(store);
#endif
*/
//  scoped_str_arena s_arena(arena);
	// NB: since txn_stock_level() is a RO txn, we assume that
	// locking is un-necessary (since we can just read from some old snapshot)
	try {
#if DBTX_TIME
		txn_tim.lap();
#endif

		rotx.Begin();
#if DBTX_TIME
		atomic_add64(&dbtx_time[STOCK_LEVEL],txn_tim.lap());
#endif
		uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
#if 0
#if SHORTKEY
		const district::key k_d(makeDistrictKey(warehouse_id, districtID));
#else
		const district::key k_d(warehouse_id, districtID);
#endif
//	uint64_t slstart = rdtsc();
		ALWAYS_ASSERT(tbl_district(warehouse_id)->get(txn, Encode(obj_key0, k_d), obj_v));

//	secs += (rdtsc() - slstart);
#endif
		uint64_t *d_value;

#if DBTX_TIME
		txn_tim.lap();
#endif

		rotx.Get(DIST, d_key, &d_value);//Tx.1
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[DIST].rogets++;
#endif
		atomic_add64(&dbtx_time[STOCK_LEVEL],txn_tim.lap());
#endif

		district::value *v_d = (district::value *)d_value;
		checker::SanityCheckDistrict(NULL, v_d);
#if 0
		const district::value *v_d = Decode(obj_v, v_d_temp);
		checker::SanityCheckDistrict(&k_d, v_d);
#endif
		const uint64_t cur_next_o_id = g_new_order_fast_id_gen ?
									   NewOrderIdHolder(warehouse_id, districtID).load(memory_order_acquire) :
									   v_d->d_next_o_id;

		// manual joins are fun!
#if 0
		order_line_scan_callback c;
#endif
		const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
		uint64_t start = makeOrderLineKey(warehouse_id, districtID, lower, 0);
		uint64_t end = makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0);
#if SLDBTX
		DBTX::Iterator iter(&rotx, ORLI);
#else
		DBROTX::Iterator iter(&rotx, ORLI);
#endif

#if DBTX_TIME
		txn_tim.lap();
#endif
		iter.Seek(start);//Tx.2
#if DBTX_TIME
#if DBTX_PROF

		Op_prof[ORLI].roseeks++;
#endif
		atomic_add64(&dbtx_time[STOCK_LEVEL],txn_tim.lap());
#endif

		std::vector<int32_t> s_i_ids;
		// Average size is more like ~30.
		s_i_ids.reserve(300);
		while(iter.Valid()) {
			int64_t ol_key = iter.Key();
			if(ol_key >= end) break;
			uint64_t *ol_value = iter.Value();
			order_line::value *v_ol = (order_line::value *)ol_value;
			int32_t s_i_id = v_ol->ol_i_id;
			int64_t s_key = makeStockKey(warehouse_id, s_i_id);
			uint64_t *s_value;
			
#if DBTX_TIME
			txn_tim.lap();
#endif
			bool found = rotx.Get(STOC, s_key, &s_value);//Tx.3
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[STOC].rogets++;
#endif
			atomic_add64(&dbtx_time[STOCK_LEVEL],txn_tim.lap());
#endif

			stock::value *v_s = (stock::value *)s_value;
			if(v_s->s_quantity < int(threshold))
				s_i_ids.push_back(s_i_id);
#if DBTX_TIME
			txn_tim.lap();
#endif

			iter.Next();//Tx.4
#if DBTX_TIME
#if DBTX_PROF

			Op_prof[ORLI].ronexts++;
#endif
			atomic_add64(&dbtx_time[STOCK_LEVEL],txn_tim.lap());
#endif

		}
		int num_distinct = 0;
		std::sort(s_i_ids.begin(), s_i_ids.end());

		int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
		for(size_t i = 0; i < s_i_ids.size(); ++i) {
			if(s_i_ids[i] != last) {
				last = s_i_ids[i];
				num_distinct += 1;
			}
		}
#if 0
#if SHORTKEY
		const order_line::key k_ol_0(makeOrderLineKey(warehouse_id, districtID, lower, 0));
		const order_line::key k_ol_1(makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0));
#else
		const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
		const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id, 0);
#endif
		{
			ANON_REGION("StockLevelOrderLineScan:", &stock_level_probe0_cg);
//	  uint64_t slstart = rdtsc();
			tbl_order_line(warehouse_id)->scan(txn, Encode(obj_key0, k_ol_0), &Encode(obj_key1, k_ol_1), c, s_arena.get());
//	  secs += (rdtsc() - slstart);
		}
		{
			small_unordered_map<uint, bool, 512> s_i_ids_distinct;
			for(auto &p : c.s_i_ids) {
				ANON_REGION("StockLevelLoopJoinIter:", &stock_level_probe1_cg);

				const size_t nbytesread = serializer<int16_t, true>::max_nbytes();
#if SHORTKEY
				const stock::key k_s(makeStockKey(warehouse_id, p.first));
#else
				const stock::key k_s(warehouse_id, p.first);
#endif
				INVARIANT(p.first >= 1 && p.first <= NumItems());
				{
					ANON_REGION("StockLevelLoopJoinGet:", &stock_level_probe2_cg);
//			slstart = rdtsc();
					ALWAYS_ASSERT(tbl_stock(warehouse_id)->get(txn, Encode(obj_key0, k_s), obj_v, nbytesread));
					//	  secs += (rdtsc() - slstart);
				}
				INVARIANT(obj_v.size() <= nbytesread);
				const uint8_t *ptr = (const uint8_t *) obj_v.data();
				int16_t i16tmp;
				ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
				if(i16tmp < int(threshold))
					s_i_ids_distinct[p.first] = 1;
			}
			evt_avg_stock_level_loop_join_lookups.offer(c.s_i_ids.size());
			// NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
		}
#endif

#if 0
		measure_txn_counters(txn, "txn_stock_level");
		if(likely(db->commit_txn(txn)))
			return txn_result(true, 0);
#endif
#if DBTX_TIME
		txn_tim.lap();
#endif

		bool res = rotx.End();
#if DBTX_TIME
		atomic_add64(&dbtx_time[STOCK_LEVEL],txn_tim.lap());
#endif
		return txn_result(res, 0);
	} catch(abstract_db::abstract_abort_exception &ex) {
#if 0
		db->abort_txn(txn);
#endif
	}
	return txn_result(false, 0);
}

template <typename T>
static vector<T>
unique_filter(const vector<T> &v) {
	set<T> seen;
	vector<T> ret;
	for(auto &e : v)
		if(!seen.count(e)) {
			ret.emplace_back(e);
			seen.insert(e);
		}
	return ret;
}

class tpcc_bench_runner : public bench_runner {
public:

	DBTables* store;
private:

	static bool
	IsTableReadOnly(const char *name) {
		return strcmp("item", name) == 0;
	}

	static bool
	IsTableAppendOnly(const char *name) {
		return strcmp("history", name) == 0 ||
			   strcmp("oorder_c_id_idx", name) == 0;
	}
#if 0

	static vector<abstract_ordered_index *>
	OpenTablesForTablespace(abstract_db *db, const char *name, size_t expected_size) {

		const bool is_read_only = IsTableReadOnly(name);
		const bool is_append_only = IsTableAppendOnly(name);
		const string s_name(name);
		vector<abstract_ordered_index *> ret(NumWarehouses());
		if(g_enable_separate_tree_per_partition && !is_read_only) {
			if(NumWarehouses() <= nthreads) {
				for(size_t i = 0; i < NumWarehouses(); i++)
					ret[i] = db->open_index(s_name + "_" + to_string(i), expected_size, is_append_only);
			} else {
				const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
				for(size_t partid = 0; partid < nthreads; partid++) {
					const unsigned wstart = partid * nwhse_per_partition;
					const unsigned wend   = (partid + 1 == nthreads) ?
											NumWarehouses() : (partid + 1) * nwhse_per_partition;
					abstract_ordered_index *idx =
						db->open_index(s_name + "_" + to_string(partid), expected_size, is_append_only);
					for(size_t i = wstart; i < wend; i++)
						ret[i] = idx;
				}
			}
		} else {
			abstract_ordered_index *idx = db->open_index(s_name, expected_size, is_append_only);
			for(size_t i = 0; i < NumWarehouses(); i++)
				ret[i] = idx;
		}
		return ret;
	}
#endif
public:
	~tpcc_bench_runner() {
		printf("[Alex]~tpcc_bench_runner\n");
		delete store;
		//delete tx;
	}

	tpcc_bench_runner(abstract_db *db)
		: bench_runner(db) {

#if USESECONDINDEX
		store = new DBTables(9, nthreads);
#else
		store = new DBTables(11, nthreads);
		//store->RCUInit(NumWarehouses());

#endif
		//insert an end value
#if SEPERATE
		store->AddTable(WARE, HASH, NONE);
		store->AddTable(DIST, HASH, NONE);
		store->AddTable(CUST, HASH, SBTREE);
		store->AddTable(HIST, HASH, NONE);
		store->AddTable(NEWO, BTREE, NONE);
		store->AddTable(ORDE, HASH, IBTREE);
		store->AddTable(ORLI, BTREE, NONE);
		store->AddTable(ITEM, HASH, NONE);
		store->AddTable(STOC, HASH, NONE);
#else

#if EUNO_TREE
		//printf("AddTable\n");
		for(int i = 0; i < 9; i++)
			if(i == CUST) store->AddTable(i, EUNO_BTREE, SBTREE);
			else if(i == ORDE) store->AddTable(i, EUNO_BTREE, IBTREE);
			else if(i == ORLI) store->AddTable(i, EUNO_BTREE, NONE);
			else if(i == NEWO) store->AddTable(i, BTREE, NONE);
			else store->AddTable(i, EUNO_BTREE, NONE);

#else
		for(int i = 0; i < 9; i++)
			if(i == CUST) store->AddTable(i, BTREE, SBTREE);
			else if(i == ORDE) store->AddTable(i, BTREE, IBTREE);
			else if(i == ORLI) store->AddTable(i, BTREE, NONE);
			else if(i == NEWO) store->AddTable(i, BTREE, NONE);
			else store->AddTable(i, BTREE, NONE);
#endif

#endif

#if !USESECONDINDEX
		store->AddTable(CUST_INDEX, SBTREE, NONE);
		store->AddTable(ORDER_INDEX, BTREE, NONE);
#endif

		//Add the schema
		store->AddSchema(WARE, sizeof(uint64_t), sizeof(warehouse::value));
		store->AddSchema(DIST, sizeof(uint64_t), sizeof(district::value));
		store->AddSchema(CUST, sizeof(uint64_t), sizeof(customer::value));
		store->AddSchema(HIST, sizeof(uint64_t), sizeof(history::value));
		store->AddSchema(NEWO, sizeof(uint64_t), sizeof(new_order::value));
		store->AddSchema(ORDE, sizeof(uint64_t), sizeof(oorder::value));
		store->AddSchema(ORLI, sizeof(uint64_t), sizeof(order_line::value));
		store->AddSchema(ITEM, sizeof(uint64_t), sizeof(item::value));
		store->AddSchema(STOC, sizeof(uint64_t), sizeof(stock::value));

		//XXX FIXME: won't serialize sec index currently
		store->AddSchema(CUST_INDEX, sizeof(uint64_t), 0);
		store->AddSchema(CUST_INDEX, sizeof(uint64_t), 0);

	}

protected:

	virtual void sync_log() {
		store->Sync();
		while(store->pbuf_->last_safe_sn < store->pbuf_->GetSafeSN());

		printf("Last Snapshot %ld\n", store->pbuf_->last_safe_sn);
	}

	virtual void print_persisted_info() {
		store->pbuf_->Print();
	}

	virtual void initPut() {

		Memstore::MemNode *mn;
		for(int i = 0; i < 9; i++) {
			//Fixme: invalid value pointer
			Memstore::MemNode *node = store->tables[i]->Put((uint64_t)1 << 60, (uint64_t *)new Memstore::MemNode());
			if(i == ORDE) mn = node;
		}
#if USESECONDINDEX
		store->secondIndexes[ORDER_INDEX]->Put((uint64_t)1 << 60, (uint64_t)1 << 60, mn);
#else

		//XXX: add empty record to identify the end of the table./
		uint64_t *temp = new uint64_t[2];
		temp[0] = 1;
		temp[1] = 0xFFFF;
		store->tables[ORDER_INDEX]->Put((uint64_t)1 << 60, temp);
#endif
	}

	virtual vector<bench_loader *>
	make_loaders() {
		vector<bench_loader *> ret;
		ret.push_back(new tpcc_warehouse_loader(9324, db,  store));
		ret.push_back(new tpcc_item_loader(235443, db,  store));
		if(enable_parallel_loading) {
			fast_random r(89785943);
			for(uint i = 1; i <= NumWarehouses(); i++)
				ret.push_back(new tpcc_stock_loader(r.next(), db,  i, store));
		} else {
			ret.push_back(new tpcc_stock_loader(89785943, db, -1, store));
		}
		ret.push_back(new tpcc_district_loader(129856349, db,  store));
		if(enable_parallel_loading) {
			fast_random r(923587856425);
			for(uint i = 1; i <= NumWarehouses(); i++)
				ret.push_back(new tpcc_customer_loader(r.next(), db, i, store));
		} else {
			ret.push_back(new tpcc_customer_loader(923587856425, db,  -1, store));
		}
		if(enable_parallel_loading) {
			fast_random r(2343352);
			for(uint i = 1; i <= NumWarehouses(); i++)
				ret.push_back(new tpcc_order_loader(r.next(), db,  i, store));
		} else {
			ret.push_back(new tpcc_order_loader(2343352, db, -1, store));
		}
		return ret;
	}

	virtual vector<bench_worker *>
	make_workers() {
//   const unsigned alignment = coreid::num_cpus_online();
//   const int blockstart =
//     coreid::allocate_contiguous_aligned_block(nthreads, alignment);
//   ALWAYS_ASSERT(blockstart >= 0);
//   ALWAYS_ASSERT((blockstart % alignment) == 0);
		const int blockstart = TOTAL_CPUS_ONLINE;
		fast_random r(23984543);
		vector<bench_worker *> ret;
#if BINDWAREHOUSE
		if(NumWarehouses() <= nthreads) {
			for(size_t i = 0; i < nthreads; i++)
				ret.push_back(
					new tpcc_worker(
						blockstart + i,
						r.next(), db,
						&barrier_a, &barrier_b,
						(i % NumWarehouses()) + 1, (i % NumWarehouses()) + 2, store));
		} else {
			const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
			for(size_t i = 0; i < nthreads; i++) {
				const unsigned wstart = i * nwhse_per_partition;
				const unsigned wend   = (i + 1 == nthreads) ?
										NumWarehouses() : (i + 1) * nwhse_per_partition;
				ret.push_back(
					new tpcc_worker(
						blockstart + i,
						r.next(), db,
						&barrier_a, &barrier_b, wstart + 1, wend + 1, store));
			}
		}
#else
		const unsigned wstart = 0;
		const unsigned wend	= NumWarehouses();

		for(size_t i = 0; i < nthreads; i++) {
			ret.push_back(
				new tpcc_worker(
					blockstart + i,
					r.next(), db,
					&barrier_a, &barrier_b, wstart + 1, wend + 1, store));
		}
#endif
		return ret;
	}

};

void
tpcc_do_test(int argc, char **argv) {
	// parse options
	abstract_db *db = NULL;
	optind = 1;
	bool did_spec_remote_pct = false;
	while(1) {
		static struct option long_options[] = {
			{"disable-cross-partition-transactions" , no_argument       , &g_disable_xpartition_txn             , 1}   ,
			{"disable-read-only-snapshots"          , no_argument       , &g_disable_read_only_scans            , 1}   ,
			{"enable-partition-locks"               , no_argument       , &g_enable_partition_locks             , 1}   ,
			{"enable-separate-tree-per-partition"   , no_argument       , &g_enable_separate_tree_per_partition , 1}   ,
			{"new-order-remote-item-pct"            , required_argument , 0                                     , 'r'} ,
			{"new-order-fast-id-gen"                , no_argument       , &g_new_order_fast_id_gen              , 1}   ,
			{"uniform-item-dist"                    , no_argument       , &g_uniform_item_dist                  , 1}   ,
			{"workload-mix"                         , required_argument , 0                                     , 'w'} ,
			{0, 0, 0, 0}
		};
		int option_index = 0;
		int c = getopt_long(argc, argv, "r:", long_options, &option_index);
		if(c == -1)
			break;
		switch(c) {
		case 0:
			if(long_options[option_index].flag != 0)
				break;
			abort();
			break;

		case 'r':
			g_new_order_remote_item_pct = strtoul(optarg, NULL, 10);
			ALWAYS_ASSERT(g_new_order_remote_item_pct >= 0 && g_new_order_remote_item_pct <= 100);
			did_spec_remote_pct = true;
			break;

		case 'w': {
			const vector<string> toks = split(optarg, ',');
			ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
			unsigned s = 0;
			for(size_t i = 0; i < toks.size(); i++) {
				unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
				ALWAYS_ASSERT(p >= 0 && p <= 100);
				s += p;
				g_txn_workload_mix[i] = p;
			}
			ALWAYS_ASSERT(s == 100);
		}
		break;

		case '?':
			/* getopt_long already printed an error message. */
			exit(1);

		default:
			abort();
		}
	}

	if(did_spec_remote_pct && g_disable_xpartition_txn) {
		cerr << "WARNING: --new-order-remote-item-pct given with --disable-cross-partition-transactions" << endl;
		cerr << "  --new-order-remote-item-pct will have no effect" << endl;
	}
	
	if(verbose) {
		cerr << "tpcc settings:" << endl;
		cerr << "  cross_partition_transactions : " << !g_disable_xpartition_txn << endl;
		cerr << "  read_only_snapshots          : " << !g_disable_read_only_scans << endl;
		cerr << "  partition_locks              : " << g_enable_partition_locks << endl;
		cerr << "  separate_tree_per_partition  : " << g_enable_separate_tree_per_partition << endl;
		cerr << "  new_order_remote_item_pct    : " << g_new_order_remote_item_pct << endl;
		cerr << "  new_order_fast_id_gen        : " << g_new_order_fast_id_gen << endl;
		cerr << "  uniform_item_dist            : " << g_uniform_item_dist << endl;
		cerr << "  workload_mix                 : " <<
			 format_list(g_txn_workload_mix,
						 g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
	}

	tpcc_bench_runner r(db);
	r.run();
}
