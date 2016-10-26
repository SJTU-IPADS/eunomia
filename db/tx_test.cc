// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtransaction.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "db/txskiplist.h"


static int FLAGS_txs = 100;
static int FLAGS_threads = 4;
static const char* FLAGS_benchmarks =
	"equal,"
	"counter,"
	"nocycle,"
	"consistency";

namespace leveldb {


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
      : tid(index)
    {
    }
};

class Benchmark {
  private:
	HashTable *seqs;
	TXSkiplist* store;
	port::Mutex *mutex;
  public:
	Benchmark(HashTable *t, TXSkiplist *s , port::Mutex *m) {
		seqs = t;
		store = s;
		mutex = m;
	}
	struct ThreadArg {
		ThreadState *thread;
		HashTable *seqs;
		TXSkiplist *store;
		port::Mutex *mutex;
		
	};

	static void ConsistencyTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		TXSkiplist *store = arg->store;
		port::Mutex *mutex = arg->mutex;
		
		ValueType t = kTypeValue;
		for (int i=tid; i<tid+FLAGS_txs*2; i+=2 ){
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
		    char* key = new char[100];
			snprintf(key, sizeof(key), "%d", i);
			Slice k(key);
			char* value = new char[100];
			snprintf(value, sizeof(value), "%d", tid);
			Slice *v = new leveldb::Slice(value);
			tx.Add(t, k, *v);
			//printf("tid %d iter %d\n",tid, i);
				

			char* key1 = new char[100];
			snprintf(key1, sizeof(key1), "%d", i+1);
			Slice k1(key1);
			char* value1 = new char[100];
			snprintf(value1, sizeof(value1), "%d", tid);
			Slice *v1 = new leveldb::Slice(value1);
			tx.Add(t, k1, *v1);
			
		
			b = tx.End();
			//printf("tid %d iter %d\n",tid, i);	
			}			
		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	}
	
	static void NocycleTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		TXSkiplist *store = arg->store;
		port::Mutex *mutex = arg->mutex;
		int num = shared->total;
		ValueType t = kTypeValue;
		Slice *str  = new Slice[num];
		//printf("start %d\n",tid);
		bool fail = false;
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {
			
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
			char* key = new char[100];
			snprintf(key, sizeof(key), "%d", tid);
			Slice k(key);
			char* value = new char[100];
			snprintf(value, sizeof(value), "%d", 1);
			Slice *v = new leveldb::Slice(value);
			tx.Add(t, k, *v);		

			char* key1 = new char[100];
			snprintf(key1, sizeof(key1), "%d", (tid+1) % num);
			Slice k1(key1);
			char* value1 = new char[100];
			snprintf(value1, sizeof(value1), "%d", 2);
			Slice *v1 = new leveldb::Slice(value1);
			tx.Add(t, k1, *v1);	

			b = tx.End();
//			printf("tid %d finish tx %d\n", tid, i);
			}
			

			if (i % 10 == (tid%10) && i>10) {
				DBTransaction tx1(seqs, store, mutex);
				b =false;
				while (b==false) {
				tx1.Begin();
			
				for (int j=0; j<num; j++) {
					char* key = new char[100];
					snprintf(key, sizeof(key), "%d", j);
					Slice k(key);				

					Status s;
	//				printf("Get %d\n",tid);
					tx1.Get(k, &(str[j]), &s);
		//			printf("Tid %d get %s %s\n",tid,key,&str[j]);
				}						
				b = tx1.End();
			   
				}
				bool e = true;
				for (int j=0;j<num-1; j++) {
					e = e && (str[j].compare(str[j+1])==0);
				}
				
				//assert(!e); 
				if (e) {
					fail = true;  
					printf("all keys have same value\n");
					break;
				}
			}
			
		}
		
		{
		  MutexLock l(&shared->mu);
		  if (fail) shared->fail = fail; 
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	}
	static void CounterTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		TXSkiplist *store = arg->store;
		port::Mutex *mutex = arg->mutex;
		//printf("start %d\n",tid);
		ValueType t = kTypeValue;
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
			
			
	 		char* key = new char[100];
			snprintf(key, sizeof(key), "%d", 1);
			Slice k(key);
			Status s;
			Slice sli;
			tx.Get(k, &sli, &s);
			
			char* value = new char[4];
			EncodeFixed32(value,  DecodeFixed32(sli.data()) + 1);
			Slice *v = new leveldb::Slice(value, 4);
			
			//printf("Insert %s ", key);
			//printf(" Value %s\n", value);
			tx.Add(t, k, *v);			
			
			b = tx.End();
			
			}
		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
		//printf("end %d\n",tid);
	}
	
	static void EqualTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		TXSkiplist *store = arg->store;
		port::Mutex *mutex = arg->mutex;

		ValueType t = kTypeValue;
		Slice *str  = new Slice[3];

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);
		
		bool fail = false;
		for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {

			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b == false) {
				tx.Begin();
				
				for (int j=1; j<4; j++) {
		 			char* key = new char[100];
					snprintf(key, sizeof(key), "%d", j);
					Slice k(key);
		
					char* value = new char[100];
					snprintf(value, sizeof(value), "%d", i);
					Slice *v = new leveldb::Slice(value);

					tx.Add(t, k, *v);			
				}
				b = tx.End();

			}
			
			DBTransaction tx1(seqs, store, mutex);
			b = false;
			while (b == false) {
				tx1.Begin();
				
				for (int j = 1; j < 4; j++) {
					
					char* key = new char[100];
					snprintf(key, sizeof(key), "%d", j);
					Slice k(key);
					

					Status s;
					tx1.Get(k, &(str[j-1]), &s);
				
				}						
				b = tx1.End();
			
			}
			
			if (!(str[0].compare(str[1])==0)){
				printf("Key 1 has value %s, Key 2 has value %s, not equal\n",str[0].data(),str[1].data());
				fail = true;
				break;
			}
			if (!(str[1].compare(str[2])==0)) {
				printf("Key 2 has value %s, Key 3 has value %s, not equal\n",str[1].data(),str[2].data());
				fail = true;
				break;
			}
			
			//printf("Tid %d Iter %d\n",tid,i);

		}
		{
		  MutexLock l(&shared->mu);
		  if (fail) shared->fail = fail;
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}

	}
	void Run(void (*method)(void* arg), Slice name ) {
		int num = FLAGS_threads;
		printf("%s start\n", name.ToString().c_str());				 		
		if (name == Slice("counter")) {
			
			ValueType t = kTypeValue;
			DBTransaction tx(seqs, store, mutex);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			
			char* key = new char[100];
			snprintf(key, sizeof(key), "%d", 1);
			Slice k(key);
			char* value = new char[4];
			EncodeFixed32(value, 100);
			Slice *v = new leveldb::Slice(value, 4);

			tx.Add(t, k, *v);				
												
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
		 for (int i = 0; i < num; i++) {	 	
		 	arg[i].thread = new ThreadState(i);
			arg[i].seqs = seqs;
			arg[i].store = store;
			arg[i].mutex = mutex;
			arg[i].thread->shared = &shared;
			//printf("Out tid %lx\n", &arg[i]);
			Env::Default()->StartThread(method, &arg[i]);
			
		 }

		 shared.mu.Lock();
		 while (shared.num_done < num) {
		  shared.cv.Wait();
		 }
		 shared.mu.Unlock();
		 //printf("all done\n");
		 if (shared.fail) {
		 	printf("%s fail!\n", name.ToString().c_str());	
		 }else {
		 if (name == Slice("equal")) printf("EqualTest pass!\n");
		 else if (name == Slice("nocycle")) printf("NocycleTest pass!\n");
		 else if (name == Slice("counter")) {
		 	ValueType t = kTypeValue;
		 	DBTransaction tx(seqs, store, mutex);
			bool b =false; int result;
			//printf("verify\n");
			while (b==false) {
			tx.Begin();			
			
			char* key = new char[100];
			snprintf(key, sizeof(key), "%d", 1);
			Slice k(key);
			Status s;
			Slice sli;
			tx.Get(k, &sli, &s);
			result = DecodeFixed32(sli.data());
			//printf("result %d\n",result);
			b = tx.End();
			}
			if (result != (FLAGS_txs*num + 100)){ printf("Get %d instead of %d from the counter\ncounter fail!\n",result,FLAGS_txs*num);

//				seqs->PrintHashTable();
	//			store->DumpTXSkiplist();
			}
			//assert();
			else printf("CounterTest pass!\n");
		 }
		 else if (name == Slice("consistency")) {
		 	//printf("verify\n");
		 	bool succ = true;
		 	for (int i = 0; i< num-1+FLAGS_txs*2; i++) {
				char* key = new char[100];
				snprintf(key, sizeof(key), "%d", i);
				Slice k(key);
				bool found = false;
				uint64_t seq = 0;
				found = seqs->GetMaxWithHash(seqs->HashSlice(key), &seq);
				//assert(found);
				if (!found) {
					printf("Key %d is not found in the hashtable\nconsistency fail!\n",i);
					succ = false;
					break;
				}


				found = false;
				Slice value;
				Status s; 
				int j = 0;
				uint64_t mseq = 0;
				Status founds;

				do{
					j++;
					mutex->Lock();
					founds = store->GetMaxSeq(key, &mseq);
					mutex->Unlock();	
				}while(founds.IsNotFound() && j < 5);

				
				if (founds.IsNotFound()) {
					printf("seq %ld\n", mseq);
					printf("Key %d is not found in the memstore\nconsistency fail!\n",i);
					store->DumpTXSkiplist();
					succ = false;
					break;
				}
				if (mseq > seq) {
					succ = false;
					printf("Key %d 's seqno in memstore(%d) is larger than in hashtable\nconsistency fail!\n",i,seq, mseq);
					break;
				//assert(found);
				//assert(mseq<=seq);
				}
				
		 	}
		 	if (succ) printf("ConsistencyTest pass!\n");
		 }
		 }
	}
};
}// end namespace leveldb

int main(int argc, char**argv)
{
	for (int i = 1; i < argc; i++) {

		 int n;
		 char junk;
	 	 if (leveldb::Slice(argv[i]).starts_with("--help")){
		 	printf("To Run :\n./tx_test [--benchmarks=Benchmark Name(default: all)] [--num=number of tx per thread(default: 100)] [--threads= number of threads (defaults: 4)]\n");
			printf("Benchmarks : \nequal\t Each tx write (KeyA, x) (KeyB, x) , check get(KeyA)==get(KeyB) in other transactions\ncounter\t badcount\nnocycle\t n threads, each tx write (tid,1) ((tid+1) %n,2) , never have all keys' value are the same\nconsistency\t Check the (key,seq) in hashtable is consistent with memstore\n");
			return 0;
	 	 }
		 if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
		   FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
		 } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
		   FLAGS_threads = n;
	 	 } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
		   FLAGS_txs = n;
	 	 }
		 
 	}

	const char* benchmarks = FLAGS_benchmarks;
	void (* method)(void* arg) = NULL;
	leveldb::Options options;
	leveldb::InternalKeyComparator cmp(options.comparator);
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      leveldb::Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = leveldb::Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }
	  if (name == leveldb::Slice("equal")){
        method = &leveldb::Benchmark::EqualTest;
      } 
	  else if (name == leveldb::Slice("counter")) {
	  	method = &leveldb::Benchmark::CounterTest;
	  }
	  else if (name == leveldb::Slice("nocycle")) {
	  	method = &leveldb::Benchmark::NocycleTest;
	  }
	  else if (name == leveldb::Slice("consistency")) {
	  	method = &leveldb::Benchmark::ConsistencyTest;
	  }
	  
	  leveldb::HashTable seqs;
	  leveldb::TXSkiplist* store = new leveldb::TXSkiplist(cmp);
	  leveldb::port::Mutex mutex;
	
	  leveldb::Benchmark *benchmark = new leveldb::Benchmark(&seqs, store, &mutex);
  	  benchmark->Run(method, name);
    }
	

	
  	return 0;
}
	
	

