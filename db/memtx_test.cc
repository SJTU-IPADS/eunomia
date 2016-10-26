// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/hashtable_template.h"
#include "db/dbtransaction_template.h"
#include "db/txmemstore_template.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"


static int FLAGS_txs = 100;
static int FLAGS_threads = 4;
static const char* FLAGS_benchmarks =
	"equal,"
	"equalbatch,"
	"counter,"
	"counterbatch,"
	"nocycle,"
	"nocyclebatch,"
	"consistency";

namespace leveldb {

typedef uint64_t Key;

class KeyComparator : public leveldb::Comparator {
public:
	int operator()(const Key& a, const Key& b) const {
		if(a < b) {
			return -1;
		} else if(a > b) {
			return +1;
		} else {
			return 0;
		}
	}

	virtual int Compare(const Slice& a, const Slice& b) const {
		assert(0);
		return 0;
	}

	virtual const char* Name()  const {
		assert(0);
		return 0;
	};

	virtual void FindShortestSeparator(
		std::string* start,
		const Slice& limit)  const {
		assert(0);

	}

	virtual void FindShortSuccessor(std::string* key)  const {
		assert(0);
	}

};

class KeyHash : public leveldb::HashFunction  {

public:

	uint64_t MurmurHash64A(const void * key, int len, unsigned int seed)	{

		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;
		uint64_t h = seed ^ (len * m);
		const uint64_t * data = (const uint64_t *)key;
		const uint64_t * end = data + (len / 8);

		while(data != end)	{
			uint64_t k = *data++;
			k *= m;
			k ^= k >> r;
			k *= m;
			h ^= k;
			h *= m;
		}

		const unsigned char * data2 = (const unsigned char*)data;

		switch(len & 7)	{
		case 7:
			h ^= uint64_t(data2[6]) << 48;
		case 6:
			h ^= uint64_t(data2[5]) << 40;
		case 5:
			h ^= uint64_t(data2[4]) << 32;
		case 4:
			h ^= uint64_t(data2[3]) << 24;
		case 3:
			h ^= uint64_t(data2[2]) << 16;
		case 2:
			h ^= uint64_t(data2[1]) << 8;
		case 1:
			h ^= uint64_t(data2[0]);
			h *= m;
		};

		h ^= h >> r;
		h *= m;
		h ^= h >> r;

		return h;
	}

	virtual uint64_t hash(uint64_t& key)	{
		return key;
//		return MurmurHash64A((void *)&key, 8, 0);
	}
};


struct SharedState {
	port::Mutex mu;
	port::CondVar cv;
	int total;

	volatile double start_time;
	volatile double end_time;

	int num_initialized;
	int num_done;
	bool start;
	bool fail;

	SharedState() : cv(&mu) { }
};

struct ThreadState {
	int tid;
	SharedState *shared;
	ThreadState(int index)
		: tid(index) {
	}
};

class Benchmark {
private:
	HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs;
	TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>* store;
	KeyComparator *cmp;
public:
	Benchmark(HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *t,
			  TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *s , KeyComparator *c) {
		seqs = t;
		store = s;
		cmp = c;
	}
	struct ThreadArg {
		ThreadState *thread;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store;
		KeyComparator *cmp;

	};

	static void ConsistencyTest(void* v) {

		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		ValueType t = kTypeValue;
		for(int i = tid; i < tid + FLAGS_txs * 2; i += 2) {
			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();
				uint64_t* key = new uint64_t();
				*key = i;
				uint64_t* value = new uint64_t();
				*value = tid;
				tx.Add(t, *key, value);
				//printf("tid %d iter %d\n",tid, i);

				uint64_t* key1 = new uint64_t();
				*key1 = i + 1;
				uint64_t* value1 = new uint64_t();
				*value1 = tid;
				tx.Add(t, *key1, value1);

				b = tx.End();
				//printf("tid %d iter %d\n",tid, i);
			}
		}
		{
			MutexLock l(&shared->mu);
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}

	static void NocycleTest(void* v) {

		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		int num = shared->total;
		ValueType t = kTypeValue;
		uint64_t *str  = new uint64_t[num];
		//printf("start %d\n",tid);
		bool fail = false;

		for(int i = tid * FLAGS_txs; i < (tid + 1)*FLAGS_txs; i++) {

			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();
				uint64_t* key = new uint64_t();
				*key = tid;
				uint64_t* value = new uint64_t();
				*value = 1;
				tx.Add(t, *key, value);

				uint64_t* key1 = new uint64_t();
				*key1 = (tid + 1) % num;
				uint64_t* value1 = new uint64_t();
				*value1 = 2;
				tx.Add(t,
					   *key1, value1);
				b = tx.End();
			}

			if(i % 10 == (tid % 10) && i > 10) {
				leveldb::DBTransaction<leveldb::Key, leveldb::Key,
						leveldb::KeyHash, leveldb::KeyComparator> tx1(seqs, store, *cmp);
				b = false;

				while(b == false) {
					tx1.Begin();

					for(int j = 0; j < num; j++) {

						uint64_t *k = new uint64_t();
						*k = j;
						uint64_t *v;
						Status s;
						tx1.Get(*k, &v, &s);
						str[j] = *v;

					}
					b = tx1.End();

				}

				bool e = true;
				for(int j = 0; j < num - 1; j++) {
					e = e && (str[j] == str[j + 1]);
				}

				//assert(!e);
				if(e) {
					fail = true;
					printf("all keys have same value\n");
					break;
				}
			}

		}

		{
			MutexLock l(&shared->mu);
			if(fail) shared->fail = fail;
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}
	static void CounterTest(void* v) {

		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		//printf("start %d\n",tid);
		ValueType t = kTypeValue;
		for(int i = tid * FLAGS_txs; i < (tid + 1)*FLAGS_txs; i++) {
			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();

				uint64_t *k = new uint64_t();
				*k = 1;
				uint64_t *v;
				leveldb::Status s;
				tx.Get(*k, &v, &s);

				uint64_t *key = new uint64_t();
				*key = 1;
				uint64_t *value = new uint64_t();
				*value = *v + 1;

				//printf("Insert %s ", key);
				//printf(" Value %s\n", value);
				tx.Add(t, *key, value);

				b = tx.End();

			}
		}
		{
			MutexLock l(&shared->mu);
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
		//printf("end %d\n",tid);
	}

	static void EqualTest(void* v) {

		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		ValueType t = kTypeValue;
		uint64_t* str  = new uint64_t[3];

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);

		bool fail = false;
		for(int i = tid * FLAGS_txs; i < (tid + 1)*FLAGS_txs; i++) {

			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();

				for(int j = 1; j < 4; j++) {

					uint64_t *key = new uint64_t();
					*key = j;
					uint64_t *value = new uint64_t();
					*value = i;

					tx.Add(t, *key, value);
				}
				b = tx.End();
			}

			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx1(seqs, store, *cmp);
			b = false;
			while(b == false) {
				tx1.Begin();

				for(int j = 1; j < 4; j++) {

					uint64_t *key = new uint64_t();
					*key = j;
					uint64_t *value;
					Status s;
					tx1.Get(*key, &value, &s);
					str[j - 1] = *value;

				}
				b = tx1.End();
			}

			if(!(str[0] == str[1])) {
				printf("Key 1 has value %d, Key 2 has value %d, not equal\n", str[0], str[1]);
				fail = true;
				break;
			}
			if(!(str[1] == str[2])) {
				printf("Key 2 has value %d, Key 3 has value %d, not equal\n", str[1], str[2]);
				fail = true;
				break;
			}



		}
		{
			MutexLock l(&shared->mu);
			if(fail) shared->fail = fail;
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}

	static void NocycleBatchTest(void* v) {
		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		int num = shared->total;
		ValueType t = kTypeValue;
		uint64_t *str  = new uint64_t[num];
		//printf("start %d\n",tid);
		bool fail = false;

		for(int i = tid * FLAGS_txs; i < (tid + 1)*FLAGS_txs; i++) {

			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();
				uint64_t* key = new uint64_t();
				*key = tid;
				uint64_t* value = new uint64_t();
				*value = 1;
				tx.Add(t, *key, value);

				uint64_t* key1 = new uint64_t();
				*key1 = (tid + 1) % num;
				uint64_t* value1 = new uint64_t();
				*value1 = 2;
				tx.Add(t,
					   *key1, value1);
				b = tx.End();
			}

			if(i % 10 == (tid % 10) && i > 10) {
				leveldb::DBTransaction<leveldb::Key, leveldb::Key,
						leveldb::KeyHash, leveldb::KeyComparator> tx1(seqs, store, *cmp);


				typename leveldb::DBTransaction<leveldb::Key, leveldb::Key,
						 leveldb::KeyHash, leveldb::KeyComparator>::Batch *bat
						 = new leveldb::DBTransaction<leveldb::Key, leveldb::Key,
				leveldb::KeyHash, leveldb::KeyComparator>::Batch[num];

				uint64_t** values = new uint64_t*[num];
				Status s;
				b = false;
				int count = 0;
				while(b == false) {

					tx1.Begin();

					for(int j = 0; j < num; j++) {
						bat[j].key = j;
						bat[j].value = &values[j];
						bat[j].s = &s;
					}
					tx1.GetBatch(bat, num);

					b = tx1.End();

				}

				bool e = true;
				for(int j = 0; j < num - 1; j++) {
					e = e && (*values[j] == *values[j + 1]);
				}

				//assert(!e);
				if(e) {
					fail = true;
					printf("all keys have same value\n");
					break;
				}
			}

		}

		{
			MutexLock l(&shared->mu);
			if(fail) shared->fail = fail;
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}

	static void CounterBatchTest(void* v) {
		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		//printf("start %d\n",tid);
		ValueType t = kTypeValue;
		for(int i = tid * FLAGS_txs; i < (tid + 1)*FLAGS_txs; i++) {
			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			typename leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					 leveldb::KeyHash, leveldb::KeyComparator>::Batch bat[1];

			uint64_t *value[3];
			Status stats;
			bool b = false;
			while(b == false) {
				tx.Begin();

				uint64_t *v;
				bat[0].key = 1;
				bat[0].value = &v;
				bat[0].s = &stats;

				tx.GetBatch(bat, 1);

				//printf("v %lx s %s\n", v, bat[0].s->ToString().c_str());
				uint64_t *key = new uint64_t();
				*key = 1;
				uint64_t *value = new uint64_t();
				*value = *v + 1;

				//printf("Insert %s ", key);
				//printf(" Value %s\n", value);
				tx.Add(t, *key, value);

				b = tx.End();

			}
		}
		{
			MutexLock l(&shared->mu);
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
		//printf("end %d\n",tid);
	}

	static void EqualBatchTest(void* v) {
		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs = arg->seqs;
		TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> *store = arg->store;
		KeyComparator *cmp = arg->cmp;

		ValueType t = kTypeValue;
		uint64_t* str  = new uint64_t[3];

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);

		bool fail = false;
		for(int i = tid * FLAGS_txs; i < (tid + 1)*FLAGS_txs; i++) {

			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();

				for(int j = 1; j < 4; j++) {

					uint64_t *key = new uint64_t();
					*key = j;
					uint64_t *value = new uint64_t();
					*value = i;

					tx.Add(t, *key, value);
				}
				b = tx.End();

			}

			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx1(seqs, store, *cmp);

			typename leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					 leveldb::KeyHash, leveldb::KeyComparator>::Batch bat[3];

			uint64_t *value[3];
			Status stats[3];

			b = false;
			int count = 0;
			while(b == false) {
				tx1.Begin();

				for(int j = 0; j < 3; j++) {
					bat[j].key = j + 1;
					bat[j].value = &value[j];
					bat[j].s = &stats[j];
				}
				tx1.GetBatch(bat, 3);

				b = tx1.End();
			}

			if(!(*value[0] == *value[1])) {
				printf("Key 1 has value %d, Key 2 has value %d, not equal\n", *value[0], *value[1]);
				fail = true;
				break;
			}
			if(!(*value[1] == *value[2])) {
				printf("Key 2 has value %d, Key 3 has value %d, not equal\n", *value[1], *value[2]);
				fail = true;
				break;
			}

		}
		{
			MutexLock l(&shared->mu);
			if(fail) shared->fail = fail;
			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}

	void Run(void (*method)(void* arg), Slice name) {
		int num = FLAGS_threads;
		printf("%s start\n", name.ToString().c_str());
		if(name == Slice("counter") || name == Slice("counterbatch")) {
			leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator>::Table::ForceThreadLocalClear();
			ValueType t = kTypeValue;
			leveldb::DBTransaction<leveldb::Key, leveldb::Key,
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
			bool b = false;
			while(b == false) {
				tx.Begin();


				uint64_t *key = new uint64_t();
				*key = 1;
				uint64_t *value = new uint64_t();
				*value = 0;

				tx.Add(t, *key, value);

				b = tx.End();

				//if (b==true)printf("%d\n", i);
			}
			//printf("init \n");
		}
		//else if (name == Slice("nocycle")) num = 4;

		SharedState shared;
		shared.total = num;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		shared.fail = false;
		ThreadArg* arg = new ThreadArg[num];
		for(int i = 0; i < num; i++) {
			arg[i].thread = new ThreadState(i);
			arg[i].seqs = seqs;
			arg[i].store = store;
			arg[i].cmp = cmp;
			arg[i].thread->shared = &shared;
			//printf("Out tid %lx\n", &arg[i]);
			Env::Default()->StartThread(method, &arg[i]);

		}

		shared.mu.Lock();
		while(shared.num_done < num) {
			shared.cv.Wait();
		}
		shared.mu.Unlock();
		//printf("all done\n");
		if(shared.fail) {
			printf("%s fail!\n", name.ToString().c_str());
		} else {
			if(name == Slice("equal")) printf("EqualTest pass!\n");
			else if(name == Slice("equalbatch")) printf("EqualBatchTest pass!\n");
			else if(name == Slice("nocycle")) printf("NocycleTest pass!\n");
			else if(name == Slice("nocyclebatch")) printf("NocycleBatchTest pass!\n");
			else if(name == Slice("counter") || name == Slice("counterbatch")) {

				ValueType t = kTypeValue;
				leveldb::DBTransaction<leveldb::Key, leveldb::Key,
						leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
				bool b = false;
				int result;
				//printf("verify\n");
				while(b == false) {
					tx.Begin();

					uint64_t* k = new uint64_t();
					*k = 1;
					Status s;
					uint64_t *str;
					tx.Get(*k, &str, &s);
					result = *str;
					//printf("result %d\n",result);
					b = tx.End();
				}
				if(result != (FLAGS_txs * num)) {
					printf("Get %d instead of %d from the counter\ncounter fail!\n", result, FLAGS_txs * num);
					printf("HashTable\n");
					seqs->PrintHashTable();
					printf("MemStore\n");
					store->DumpTXMemStore();
				} else printf("CounterTest/Counterbatch pass!\n");
			} else if(name == Slice("consistency")) {
				//printf("verify\n");
				bool succ = true;
				for(int i = 0; i < num - 1 + FLAGS_txs * 2; i++) {
					uint64_t* key = new uint64_t();
					*key = i;
					bool found = false;
					uint64_t seq = 0;
					leveldb::KeyHash kh;
					found = seqs->GetMaxWithHash(kh.hash(*key), &seq);
					//assert(found);
					if(!found) {
						printf("Key %d is not found in the hashtable\nconsistency fail!\n", i);
						succ = false;
						break;
					}

					found = false;
					std::string value;
					Status s;
					int j = 0;
					uint64_t mseq = 0;
					Status founds;

					do {
						j++;
						founds = store->GetMaxSeq(*key, &mseq);
					} while(founds.IsNotFound() && j < 5);


					if(founds.IsNotFound()) {
						printf("seq %ld\n", mseq);
						printf("Key %d is not found in the memstore\nconsistency fail!\n", i);
						//store->DumpTXSkiplist();
						succ = false;
						break;
					}
					if(mseq > seq) {
						succ = false;
						printf("Key %d 's seqno in memstore(%d) is larger than in hashtable\nconsistency fail!\n", i, seq, mseq);
						break;
						//assert(found);
						//assert(mseq<=seq);
					}

				}
				if(succ) printf("ConsistencyTest pass!\n");
			}
		}
	}
};
}// end namespace leveldb

int main(int argc, char**argv) {
	for(int i = 1; i < argc; i++) {

		int n;
		char junk;
		if(leveldb::Slice(argv[i]).starts_with("--help")) {
			printf("To Run :\n./tx_test [--benchmarks=Benchmark Name(default: all)] [--num=number of tx per thread(default: 100)] [--threads= number of threads (defaults: 4)]\n");
			printf("Benchmarks : \nequal\t Each tx write (KeyA, x) (KeyB, x) , check get(KeyA)==get(KeyB) in other transactions\ncounter\t badcount\nnocycle\t n threads, each tx write (tid,1) ((tid+1) %n,2) , never have all keys' value are the same\nconsistency\t Check the (key,seq) in hashtable is consistent with memstore\n");
			return 0;
		}
		if(leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
			FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
		} else if(sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
			FLAGS_threads = n;
		} else if(sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
			FLAGS_txs = n;
		}

	}

	const char* benchmarks = FLAGS_benchmarks;
	void (* method)(void* arg) = NULL;
	leveldb::KeyHash kh;
	leveldb::KeyComparator cmp;
	while(benchmarks != NULL) {
		const char* sep = strchr(benchmarks, ',');
		leveldb::Slice name;
		if(sep == NULL) {
			name = benchmarks;
			benchmarks = NULL;
		} else {
			name = leveldb::Slice(benchmarks, sep - benchmarks);
			benchmarks = sep + 1;
		}
		if(name == leveldb::Slice("equal")) {
			method = &leveldb::Benchmark::EqualTest;
		} else if(name == leveldb::Slice("equalbatch")) {
			method = &leveldb::Benchmark::EqualBatchTest;
		} else if(name == leveldb::Slice("counter")) {
			method = &leveldb::Benchmark::CounterTest;
		} else if(name == leveldb::Slice("counterbatch")) {
			method = &leveldb::Benchmark::CounterBatchTest;
		} else if(name == leveldb::Slice("nocycle")) {
			method = &leveldb::Benchmark::NocycleTest;
		} else if(name == leveldb::Slice("nocyclebatch")) {
			method = &leveldb::Benchmark::NocycleBatchTest;
		} else if(name == leveldb::Slice("consistency")) {
			method = &leveldb::Benchmark::ConsistencyTest;
		}

		leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> store(cmp);
		/*
			  double start = leveldb::Env::Default()->NowMicros();

			  for(int i = 0; i < 1000000; i++)
			  {
			  	uint64_t *v;
			    store.Get(i-1, &v, i-1);
				store.Put(i, (uint64_t *)&i, i);
			  }

			  double end = leveldb::Env::Default()->NowMicros();
			  benchmarks = NULL;
			  printf("Total Time %lf\n", (end - start));
		*/

		leveldb::HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> seqs(kh, cmp);

		leveldb::Benchmark *benchmark = new leveldb::Benchmark(&seqs, &store, &cmp);

		benchmark->Run(method, name);
	}

	return 0;
}
