// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtx.h"
#include "db/dbrotx.h"
#include "memstore/memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"


static int FLAGS_txs = 100;
static int FLAGS_threads = 4;
static const char* FLAGS_benchmarks =
	"equal,"
	"counter,"
	"nocycle,"
	"delete,"	
	"readonly,"
    "equalrange,"
	"nocycle_readonly,"
	"rwiter,"
	"freedelete,"
	"secdelete,"
	"bigdelete,"
	"range";
	

namespace leveldb {

typedef uint64_t Key;


struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  volatile double start_time;
  volatile double end_time;
	  	
  volatile int num_initialized;
  volatile int num_done;
  bool start;
  bool fail;
  volatile int num_phase;	
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
	
	DBTables* store;
	
  public:
	Benchmark(	DBTables *s ) {
		store = s;
		
	}
	struct ThreadArg {
		ThreadState *thread;
		DBTables *store;
		volatile uint8_t start;
		
	};

	static void NocycleReadonlyTest(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		DBTables *store = arg->store;
		store->ThreadLocalInit(tid);
		
		int num = shared->total;
		assert(num >  1);
		uint64_t *str  = new uint64_t[num];
		//printf("start %d\n",tid);
		bool fail = false;
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {

			leveldb::DBTX tx(store);
			bool b = false;
			while (b==false) {
				tx.Begin();

				uint64_t* value = new uint64_t(); 
				*value = 1;
				tx.Add(0, tid, value, 8);
				delete value;
		

				uint64_t* value1 = new uint64_t(); 
				*value1 = 2;
				tx.Add(0, (tid+1) % num, value1, 8);
				delete value1;
				
				b = tx.End();
			}
			
			if (i % 10 == (tid%10) && i>10) {
				leveldb::DBROTX tx1(store);
							
				tx1.Begin();
			
				for (int j=0; j<num; j++) {


					uint64_t *v;
					
					tx1.Get(0, j, &v);
					str[j] = *v;
					
				}						
				b = tx1.End();
			   

				bool e = true;
				for (int j=0;j<num-1; j++) {
					e = e && (str[j]==str[j+1]);
				}
				
				//assert(!e); 
				if (e) {
					fail = true;  
					printf("all keys have same value\n");
					break;
				}

				leveldb::DBROTX tx2(store);
							
				tx2.Begin();
			
				DBROTX::Iterator iter(&tx2, 0);
				iter.SeekToFirst();
				int key = 0;
				while (iter.Valid()) {
					uint64_t k = iter.Key();
					
					uint64_t *v = iter.Value();
					
					
					str[key] = *v;
					key++;
					iter.Next();
				}						
				b = tx2.End();
			   

				e = true;
				for (int j=0;j<num-1; j++) {
					e = e && (str[j]==str[j+1]);
				}
				
				//assert(!e); 
				if (e) {
					fail = true;  
					printf("all keys have same value in range query\n");
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
	
	static void NocycleTest(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		DBTables *store = arg->store;
		store->ThreadLocalInit(tid);
		
		int num = shared->total;
		assert(num > 1);
		uint64_t *str  = new uint64_t[num];
		//printf("start %d\n",tid);
		bool fail = false;
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {

			leveldb::DBTX tx(store);
			bool b = false;
			while (b==false) {
				tx.Begin();

				uint64_t* value = new uint64_t(); 
				*value = 1;
				tx.Add(0, tid, value, 8);
				delete value;
		

				uint64_t* value1 = new uint64_t(); 
				*value1 = 2;
				tx.Add(0, (tid+1) % num, value1, 8);
				delete value1;
				b = tx.End();
			}
			//printf("Add %d %d\n",tid,(tid+1) % num);
			if (i % 10 == (tid%10) && i>10) {
				leveldb::DBTX tx1(store);
				b =false;
				
				while (b==false) {
				tx1.Begin();
			
				for (int j=0; j<num; j++) {


					uint64_t *v;
					
					tx1.Get(0, j, &v);
					//printf("%d-----1\n",j);
					str[j] = *v;
					//printf("%d------2\n",j);
					
				}						
				b = tx1.End();
			   
				}

				bool e = true;
				for (int j=0;j<num-1; j++) {
					e = e && (str[j]==str[j+1]);
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
		DBTables *store = arg->store;
		store->ThreadLocalInit(tid);
		
		//printf("start %d\n",tid);
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {
			leveldb::DBTX tx( store);
			bool b = false;
			while (b==false) {
			tx.Begin();
			
			
	 		uint64_t k = 1;
			uint64_t *v;
			
			tx.Get(0, k, &v);

			uint64_t *value = new uint64_t();
			
			*value = *v + 1;

			//printf("Insert %s ", key);
			//printf(" Value %s\n", value);
			tx.Add(0, k, value, 8);			
			delete value;
			
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

	static void EqualRangeTest(void* v) {
		
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		DBTables *store = arg->store;
		store->ThreadLocalInit(tid);
	//	store->tables[0]->PrintStore();

		

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);
		
		bool fail = false;
		for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {
			if (i == 0) continue;
			//printf("[EqualTest]snapshot %d\n", store->snapshot);
			leveldb::DBTX tx( store);
			bool b = false;
			while (b == false) {
				tx.Begin();
				
				for (int j=0; j<10; j++) {
		 			

					uint64_t *value = new uint64_t();
					*value = i;
					//printf("[%d] insert key %ld value %ld\n",tid, j, *value);
		 			tx.Add(0, j, value, 8);
					delete value;				
				}
				b = tx.End();

			}
//			store->tables[0]->PrintStore();

			{
					leveldb::DBROTX tx2( store);

					tx2.Begin();
					DBROTX::Iterator iter(&tx2, 0);
					iter.SeekToFirst();
					uint64_t count = 0;
					uint64_t v = 0;
					while (iter.Valid()) {
					
						
						uint64_t key = iter.Key();
						//printf("%d \n",key);
						if (key != count) {
							printf("tx2 Key %d not found \n", count);
							fail = true;
							break;
						}
						count++;
						uint64_t *value = iter.Value();
						if (v == 0) v = *value;
						else if (v != *value) {
							printf("tx2 Key %d has value %d, not same with others (%d)\n", key, *value, v);
							fail = true;
							break;
						}
						iter.Next();
						
					}						
					if (fail) break;
					tx2.End();
			}

			{
					leveldb::DBROTX tx3( store);

					tx3.Begin();
					DBROTX::Iterator iter(&tx3, 0);
					uint64_t count = 5;
					uint64_t v = 0;
					iter.Seek(5);
					while (iter.Valid()) {
					
						
						uint64_t key = iter.Key();
						if (key != count) {
							printf("tx3 Key %d not found\n", count);
							fail = true;
							break;
						}
						count++;
						uint64_t *value = iter.Value();					
						if (v == 0) v = *value;
						else if (v != *value) {
							printf("tx3 Key %d has value %d, not same with others (%d)\n", key, *value, v);
							store->tables[0]->PrintStore();
							fail = true;
							break;
						}

						iter.Next();
						
					}						
					if (fail) break;
					tx3.End();
			}
				//printf("Pass 2\n");
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
						
	static void EqualTest(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		DBTables *store = arg->store;
		store->ThreadLocalInit(tid);

		
		uint64_t* str  = new uint64_t[3];

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);
		
		bool fail = false;
		for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {
			
			//printf("[EqualTest]snapshot %d\n", store->snapshot);
			leveldb::DBTX tx( store);
			bool b = false;
			while (b == false) {
				tx.Begin();
				for (int j=1; j<4; j++) {
		 			

					uint64_t *value = new uint64_t();
					*value = i;
		 			
					tx.Add(0, j, value, 8);
					delete value;
				}
				b = tx.End();

			}
			//printf("[%d]End %d\n",tid, i);

			leveldb::DBTX tx1( store);
			b = false;
			while (b == false) {
				tx1.Begin();

				for (int j = 1; j < 4; j++) {
					

					uint64_t *value;					
					
					tx1.Get(0, j, &value);
					str[j-1] = *value;
				
				}						
				b = tx1.End();

			}
			
			if (!(str[0]==str[1])){
				printf("Key 1 has value %d, Key 2 has value %d, not equal\n",str[0],str[1]);
				fail = true;
				
	//			DBTX::slock.Lock();
				store->tables[0]->PrintStore();
				
	//			DBTX::slock.Unlock();
				break;
			}
			if (!(str[1]==str[2])) {
				printf("Key 2 has value %d, Key 3 has value %d, not equal\n",str[1],str[2]);
				fail = true;
				
	//			DBTX::slock.Lock();
				store->tables[0]->PrintStore();
	//			DBTX::slock.Unlock();

				break;
			}
			//printf("Pass 1\n");
#if 1			
			{
				leveldb::DBROTX tx2( store);
				bool found[3];
				
				tx2.Begin();

				uint64_t snapshot = tx2.oldsnapshot;
				int j;
				for (j = 1; j < 4; j++) {
					

					uint64_t *value;					
						
					found[j-1] = tx2.Get(0, j, &value);
					if (found[j-1]) str[j-1] = *value;
					
				}						

				tx2.End();
				bool f = true;
				for (j = 1; j < 4; j++)
					if (!found[j-1]) {
						printf("[%d]Key %d not found\n", i, j);
						f = false;
					}
				if (!f) {

					fail = true;
					//store->PrintList();
					printf("snapshot %ld\n", snapshot);
					DBTX::slock.Lock();
					store->tables[0]->PrintStore();
					DBTX::slock.Unlock();
					break;
				}
				
				if (!(str[0]==str[1])){
					printf("In RO, Key 1 has value %d, Key 2 has value %d, not equal\n",str[0],str[1]);
					printf("snapshot %ld\n", snapshot);
					DBTX::slock.Lock();
					store->tables[0]->PrintStore();
					DBTX::slock.Unlock();
				
					fail = true;
					break;
				}
				if (!(str[1]==str[2])) {
					printf("In RO, Key 2 has value %d, Key 3 has value %d, not equal\n",str[1],str[2]);
					printf("snapshot %ld\n", snapshot);
					DBTX::slock.Lock();
					store->tables[0]->PrintStore();				
					DBTX::slock.Unlock();

					fail = true;
					break;
				}
				
			}
#endif

			//printf("Pass 2\n");
		}
//		printf("Finish %d\n",tid);
		{
		  MutexLock l(&shared->mu);
		  if (fail) shared->fail = fail;
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}

	}

	
	static void BigdeleteTest(void* v) {
	
			ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
			int tid = (arg->thread)->tid;
			SharedState *shared = arg->thread->shared;
			DBTables *store = arg->store;
			store->ThreadLocalInit(tid);
	
	
			//printf("In tid %lx\n", arg);
			//printf("start %d\n",tid);
			
			bool fail = false;
			for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {
				//printf("the %d\n",i);
				leveldb::DBTX tx( store);
				bool b = false; 
				bool f = true;
				bool check1 = false;
				bool check2 = false;
				bool check3 = false;
				uint64_t start = 10;
				while (b == false) {
					tx.Begin();
					check1 = false;
					check2 = false;
					check3 = false;
					f = true;
					uint64_t *value;
					start = 10;
					while (f) {
						if (start > (10 + FLAGS_threads)) {
							check1 = true;
							break;
						}
						f = tx.Get(0, start, &value);
						start++;
					}
					if (start <= 10) printf("~!\n");
					if (!check1) { 

					for (int j = start; i < start+9; i++) {
						f = tx.Get(0, j, &value);
						if (f) {
							check2 = true;
							break;
						}
					}
					if (!check2) {
					//
					f = tx.Get(0, start + 9, &value);
					if (!f) {
						check3 = true;
						//printf("Get %d\n",start+9);
					}
					if (!check3) {
					//operation
					for (int j=10; j<10+tid; j++) {
						uint64_t *value1 = new uint64_t();
						*value1 = 1;
						tx.Add(0, j, value1, 8);
						delete value1;
					}
					for (int j = 0; j<10; j++) {
						tx.Delete(0, 10+tid+j);
					}
					for (int j = 0; j<5; j++) {
						uint64_t *value2 = new uint64_t();
						*value2 = 1;
						tx.Add(0, 20+tid+j, value2, 8);
						delete value2;
					}
					}
					}
					}
					b = tx.End();
	
				}
				//
				if (check1) {
					printf("none of 10 to 10+num_threads-1 does not exist\n");
					fail = true;
					break;
				}
				if (check2){
					printf("Did not get continuous not found\n");
					fail = true;
					break;
				}
				if (check3) {
					printf("start %d\n",start-1);
					printf("Get more than 10 continuous not found\n");
					fail = true;
					break;
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

	static void DeleteTest(void* v) {
					ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
					int tid = (arg->thread)->tid;
					SharedState *shared = arg->thread->shared;
					DBTables *store = arg->store;
					store->ThreadLocalInit(tid);
			
					assert( store->nodeGCQueue != NULL);
					//printf("In tid %lx\n", arg);
					//printf("start %d\n",tid);
					uint64_t v3; uint64_t v6; uint64_t v4;
					bool fail = false;
					for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {
						if (i % 500000 == 0)printf("[Tid %d] Iter %d\n",tid, i-tid*FLAGS_txs);
						leveldb::DBTX tx( store);
						bool b = false;
						while (b == false) {
							tx.Begin();
		//					printf("[%ld] TX1 Begin\n", pthread_self());
		//					printf("[%ld]TX1 Delete 3\n", pthread_self());
							tx.Delete(0, 3);	
							
							uint64_t *value = new uint64_t();
							*value = i;
		//					printf("[%ld] TX1 Put 4\n", pthread_self());
							tx.Add(0, 4, value, 8);
							delete value;
#if 1
							uint64_t *value1 = new uint64_t();
							*value1 = i;
							tx.Add(0, 5, value1, 8);
							delete value1;
#endif					
							
							b = tx.End();
		//				printf("[%ld] TX1 End\n", pthread_self());
		
			
						}
		
						leveldb::DBTX tx1( store);
						b = false; 
						bool f1 = true; 
						bool f2 = false;
						bool f3 = true;
						bool c1 = false;
						bool c2 = false;
						uint64_t *value; uint64_t *value1;
						while (b == false) {
							tx1.Begin();
				//			printf("[%ld] RTX Begin\n", pthread_self());						
				
							c1 = false;
							c2 = false;
			//				printf("[%ld]RTX Get 4\n", pthread_self());
							uint64_t sum = 0;
							f1 = tx1.Get(0, 4, &value);
							if (f1) {
								v4 = *value;
							}
							
		//					printf("[%ld]RTX Get 3\n", pthread_self());
		
							f3 = tx1.Get(0, 3, &value);	
							if (f3) {
								v3 = *value;	
								//printf("k3 %ld\n",v3);
							}


		
#if 1		
							f2 = tx1.Get(0, 5, &value); 	
							bool f4 = tx1.Get(0, 6, &value1); 
							
							
							if (f4 && f3) {
								v6 = *value1;
								//printf("k6 %ld\n",v6);
			
							}
								
					
							
							DBTX::Iterator iter(&tx1, 0);
							iter.Seek(3);
							if (f3 && iter.Key()!=3) c1 = true;
							if (!f3 && iter.Key()!=4) c1 = true;
							iter.Next();
							if (f3 && iter.Key()!=6) c2 = true;
							if (!f3 && iter.Key()!=5) c2 = true;


#endif	

							
							b = tx1.End();
				//			if(b == true)
			//					printf("[%ld]RTX End\n", pthread_self());
		//					else
		//						printf("[%ld]RTX Rollback\n", pthread_self());
						}
						if (f1 == f3) {
							printf("[%ld] Get Key 4 return %d, Get Key 3 return %d, should be diff\n", pthread_self(), f1,f3);
							if (f1) {
								printf("Value3 %ld, Value4 %ld\n",v3,v4);
							}
							fail = true;
							break;
						}
#if 1
						if (f1 != f2){
							printf("Get Key 4 return %d, Get Key 5 return %d, not equal\n",f1,f2);
							fail = true;
							break;
						}
						if (f3 && v3 != v6) {
							printf("Key 3 value %d and Key 6 value %d, should have same values\n",v3, v6);
							fail = true;
							break;
						}
			
						if (c1) {
							printf("Get Key 3 return %d, Seek Wrong\n");
							fail = true;
							break;
						}
						if (c2) {
							printf("Get Key 3 return %d, Next Wrong\n");
							fail = true;
							break;
						}
#endif	

						
						leveldb::DBTX tx2( store);
						b = false;
						while (b == false) {
							tx2.Begin();
		//					printf("[%ld] TX2 Begin\n", pthread_self());
		
							uint64_t *value = new uint64_t();
							*value = i;
							
		//					printf("[%ld] TX2 Put 3\n", pthread_self());
							tx2.Add(0,  3,  value, 8);
							delete value;
		
		//					printf("[%ld] TX2 Delete 4\n", pthread_self());
		
							tx2.Delete(0, 4);			
#if 1
		
							tx2.Delete(0, 5);
		
				
		
							uint64_t *value1 = new uint64_t();
							*value1 = i;
							tx2.Add(0,  6, value1, 8);
							delete value1;
#endif			
		
							b = tx2.End();
		//				printf("[%ld] TX2 End\n", pthread_self());
		
			
						}
		
#if 1				
						leveldb::DBROTX tx3( store);
		
						
						f1 = true; f2 = false;
						
						
						tx3.Begin();				
																
						f1 = tx3.Get(0, 4, &value);
						f2 = tx3.Get(0, 5, &value); 
						
						tx3.End();
			
#if 1						
						
						if (f1 != f2){
							printf("In read-only tx, Get Key 4 return %d, Get Key 5 return %d, not equal\n",f1,f2);
							fail = true;
							break;
		
						}
#endif
#endif		
			
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
		

	static void FreeDeleteTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		DBTables *store = arg->store;
		store->ThreadLocalInit(tid);

		//printf("Thread[%d] start\n",tid);	
		for (int i = 0; i < FLAGS_txs; i++) {
			//printf("Thread[%d] Tx[%d]\n",tid,i);
			bool b = false;
			DBTX tx(store);
			while (!b) {
				//if (i == 0) printf("!!!! Thread[%d] Tx[%d]\n",tid,i);
				tx.Begin();
				//if (i == 0) printf("---- Thread[%d] Tx[%d]\n",tid,i);
				tx.Delete(0, 1);
				
				tx.Delete(0, 3);
				tx.Delete(0, 5);
				for (int j=10; j<20; j++) {
					uint64_t *value = new uint64_t();
					*value = 20;
					tx.Add(0, j, value, 8);
					delete value;
				}
				b = tx.End();
				//if (i == 0) printf("**** Thread[%d] Tx[%d]\n",tid,i);
			}
			b = false;
			DBTX tx1(store);
			while (!b) {
				tx1.Begin();
				uint64_t *value = new uint64_t();
				*value = 100;
				tx1.Add(0, 1, value,8);
				delete value;
				value = new uint64_t();
				*value = 100;
				tx1.Add(0, 3, value,8);
				delete value;
				value = new uint64_t();
				*value = 100;
				tx1.Add(0, 5, value,8);
				delete value;
				for (int j=10; j<20; j++)
					tx1.Delete(0, j);
				b = tx1.End();
			}

			//printf("Thread[%d] Tx[%d]\n",tid,i);
		}
		//printf("Thread[%d] phase %d\n",tid, shared->num_phase );
		
		{
			  MutexLock l(&shared->mu);
			  shared->num_phase++;
		}

		//printf("Thread[%d] Main completed\n",tid);
		
		//printf("Thread[%d] phase %d\n",tid, shared->num_phase );
		while (shared->num_phase < shared->total);
		//printf("Thread[%d] Enter sec\n",tid);
		bool f = false;
		DBTX tx2(store);
		while (!f) {
			tx2.Begin();
	/*		uint64_t *value = new uint64_t();
			*value = 100;
			tx2.Add(0, 2, value, 8);
			delete value;*/
			f = tx2.End();
		}

		{
			  MutexLock l(&shared->mu);
			  shared->num_phase++;
		}

		//printf("Thread[%d] Second completed\n",tid);
		//printf("Thread[%d] phase %d\n",tid, shared->num_phase );
		
		while (shared->num_phase < shared->total*2){
	//		printf("---------%d %d\n", shared->num_phase, shared->total*2);
		}
		//printf("Thread[%d] Enter third\n",tid);
		f = false;
		DBTX tx3(store);
		while (!f) {
			tx3.Begin();
	/*		uint64_t *value = new uint64_t();
			*value = 100;
			tx3.Add(0, 4, value, 8);
			delete value;*/
			f = tx3.End();
		}

		//printf("Thread[%d] Third completed\n",tid);
		//printf("Thread[%d] phase %d\n",tid, shared->num_phase );


		bool fail = false;
		if (store->nodeGCQueue->need_del != store->nodeGCQueue->actual_del) {
			printf("Thread [%d] GC Need deleted %ld , actually deleted %ld\n", tid,
				store->nodeGCQueue->need_del, store->nodeGCQueue->actual_del);
			fail = true;
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
	
	static void SecDeleteTest(void* v) {
	
			ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
			int tid = (arg->thread)->tid;
			SharedState *shared = arg->thread->shared;
			DBTables *store = arg->store;
			store->ThreadLocalInit(tid);
	

			//printf("In tid %lx\n", arg);
		
			uint64_t *value; uint64_t *value1;
			bool fail = false;
			for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {
				if (shared->fail) break;
				bool b = false;
				leveldb::DBTX tx( store);
				
				while (b == false) {
					tx.Begin();
//					printf("[%ld] TX1 Begin\n", pthread_self());
//					printf("[%ld]TX1 Delete 3\n", pthread_self());
					tx.Delete(0, 3);	

					uint64_t *value = new uint64_t();
					*value = i;
//					printf("[%ld] TX1 Put 4\n", pthread_self());
					tx.Add(0, 0, 4, 2, value, 8);
					delete value;
#if 1
					uint64_t *value1 = new uint64_t();
					*value1 = i;
					tx.Add(0, 5, value1, 8);
					delete value1;
#endif					
                   
					b = tx.End();
//				printf("[%ld] TX1 End\n", pthread_self());

	
				}

				leveldb::DBTX tx1( store);
				b = false; 
				bool f1 = true; 
				bool f2 = false;
				bool f3 = true;
				bool check1; uint64_t sk1; uint64_t sk2;
				bool check2; bool check3; bool check4;
				uint64_t num,num1;
				DBTX::KeyValues* kvs = NULL;
				DBTX::KeyValues* kvs1 = NULL;
				while (b == false) {
					tx1.Begin();
					if (kvs != NULL) delete kvs;
					if (kvs1 != NULL) delete kvs1;
		//			printf("[%ld] RTX Begin\n", pthread_self());
					check4 = false;
					check3 = false;
					check2 = false;
					check1 = false; 
					kvs = tx1.GetByIndex(0, 1);
					if (kvs == NULL) num = 0;
					else num = kvs->num;
					
					kvs1 = tx1.GetByIndex(0, 2);
					if (kvs1 == NULL) num1 = 0;
					else num1 = kvs1->num;
					
					if (num == 0 && num1 == 0)	{
						check1 = true;
					}


	//				printf("[%ld]RTX Get 4\n", pthread_self());
#if 1	
					f2 = tx1.Get(0, 5, &value);	


					f1 = tx1.Get(0, 4, &value);
					
//					printf("[%ld]RTX Get 3\n", pthread_self());
					f3 = tx1.Get(0, 3, &value);
					
				
					tx1.Get(0, 6, &value1);	
				
					if (f3 && !((num == 2 && num1 == 0) || (num == 0 && num1 == 2))) {
						check3 = true;	
						
					}
					else if (!f3 && !((num == 1 && num1 == 1) || (num == 0 && num1 == 2))) {
/*
						if (num1 == 3) {
							printf("%d %d %d\n",kvs1->keys[0],kvs1->keys[1],kvs1->keys[2]);
							printf("%lx %lx %lx\n",kvs1->values[0],kvs1->values[1],kvs1->values[2]);
						}*/
						check4 = true;
					//	if (num == 1) 
					//		printf("%d\n", kvs->keys[0]);
					}

#endif

					
					b = tx1.End();
		//			if(b == true)
	//					printf("[%ld]RTX End\n", pthread_self());
//					else
//						printf("[%ld]RTX Rollback\n", pthread_self());
				}

				if (check1) {
					//store->secondIndexes[0]->PrintStore();
					//printf("[%ld] TX num %d\n", pthread_self(), (i - tid*FLAGS_txs));
					fail = true;
					break;
				}
#if 1				
				if (f1 == f3) {
					printf("[%ld] Get Key 4 return %d, Get Key 3 return %d, should be diff\n", pthread_self(), f1,f3);

					fail = true;
					break;
				}


				if (f1 != f2){
					printf("[%ld] Get Key 4 return %d, Get Key 5 return %d, not equal\n", pthread_self(), f1,f2);
					fail = true;
					break;
				}
				

				
				if (check3) {
					printf("[%ld] key 3 exists , secondary key 1 get %d , key 2 get %d\n", pthread_self(), num, num1);
					printf("sec 1\n");
					for (int l=0; l<num ; l++)
						printf("%d\n", kvs->keys[l]);
					
					printf("sec 2\n");
					for (int l=0; l<num1 ; l++)
						printf("%d\n", kvs1->keys[l]);
					fail = true;


					//store->secondIndexes[0]->PrintStore();
					break;
				}
				
				if (check4) {
					printf("[%ld] key 3 does not exist , secondary key 1 get %d , key 2 get %d\n", pthread_self(), num, num1);
					fail = true;
					break;
				}
#endif
				delete kvs;
				delete kvs1;
				if (shared->fail) break;
				leveldb::DBTX tx2( store);
				b = false;
				while (b == false) {
					tx2.Begin();
//					printf("[%ld] TX2 Begin\n", pthread_self());

					
					
//					printf("[%ld] TX2 Put 3\n", pthread_self());
					

//					printf("[%ld] TX2 Delete 4\n", pthread_self());
#if 1

					uint64_t *value = new uint64_t();
					*value = 10;
					tx2.Add(0, 0, 3, (tid%2)+1, value, 8);
					delete value;

					tx2.Delete(0, 4);			


					tx2.Delete(0, 5);

		
#endif	

					uint64_t *value1 = new uint64_t();
					*value1 = 10;
					tx2.Add(0, 0, 6, (tid%2)+1, value1, 8);
					delete value1;
		

					b = tx2.End();
//				printf("[%ld] TX2 End\n", pthread_self());

	
				}

#if 1				
				leveldb::DBROTX tx3( store);

				
				f1 = true; f2 = false;
				
				
				tx3.Begin();				
														
				f1 = tx3.Get(0, 4, &value);
				f2 = tx3.Get(0, 5, &value);	
				
				tx3.End();
	
				
				
				if (f1 != f2){
					printf("[%ld] In read-only tx, Get Key 4 return %d, Get Key 5 return %d, not equal\n",pthread_self(), f1,f2);
					fail = true;
					break;

				}
#endif

	
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
	static void Company(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		DBTables *store = arg->store;
		store->ThreadLocalInit(1);
		while (arg->start == 0) ;
		
		leveldb::DBTX tx( store);
		bool b =false;
			 	
		for (int i=FLAGS_txs/2 ; i<FLAGS_txs+10;i++) {
//		for (int i=15 ; i<25;i++) {
			if (i % 10 == 0) continue;
			b = false;
			while (b==false) {
				tx.Begin();	
				  		

			  	uint64_t *value = new uint64_t();
			  	*value = 4;			
			  	tx.Add(0, i, value, 8);
				delete value;
				b = tx.End();
						  
			}
			  
		}
	//	store->tables[0]->PrintStore();
		arg->start = 2;			
	}	

	static void InsertNode(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		DBTables *store = arg->store;
		store->ThreadLocalInit(1);
		while (arg->start == 0) ;
		
		leveldb::DBTX tx( store);
		bool b =false;
			 	
		b = false;
		while (b==false) {
			tx.Begin();					  		

		  	uint64_t *value = new uint64_t();
		  	*value = 4;			
		  	tx.Add(0, 12, value, 8);				
			delete value;
			b = tx.End();						  
		}			  
		arg->start = 2;
		printf("2\n");
		while (arg->start == 2) ;

		b = false;
		while (b==false) {
			tx.Begin();					  		

		  	uint64_t *value = new uint64_t();
		  	*value = 4;			
		  	tx.Add(0, 10, value, 8);				
			delete value;
			b = tx.End();						  
		}			  
		arg->start = 4;	
		printf("4\n");
	}	


	
	void Run(void (*method)(void* arg), Slice name ) {
		int num = FLAGS_threads;
		printf("%s start\n", name.ToString().c_str());				 		


		if (name == Slice("readonly")  || name == Slice("range") || name == Slice("rwiter")) {
			store->ThreadLocalInit(0);
			for (int j = 1; j<=3; j++) {
			  leveldb::DBTX tx( store);
			  bool b =false;
			 
				for (int i=1; i<FLAGS_txs;i++) {
					if (i % 10 == 0) continue;
					b = false;
				 	while (b==false) {
			    		tx.Begin();	
				  		

				  	  	uint64_t *value = new uint64_t();
				  	  	*value = j;			
					  	tx.Add(0, i, value, 8);
						delete value;
				  		b = tx.End();
						  
					}

				  
				}
				
			
				leveldb::DBROTX tx1( store);
			//	leveldb::DBTX tx1( store);
				tx1.Begin();

					
				uint64_t *r;
				tx1.Get(0, 1,  &r);
				if (*r != j) {
					printf("Key 1 get %d should be %d\n", *r, j);
					return;
				}
				tx1.End();
			}	  
		}



		if (name == Slice("readonly")) {	
			//check
			leveldb::DBROTX tx2(store);
			int m; uint64_t *r; uint64_t res;
			bool c1 = false;bool c2 = false;bool c3 = false;
			ThreadArg *arg = new ThreadArg();
			arg->start = 0;
			arg->store = store;
			Env::Default()->StartThread(method, arg);
			
			arg->start = 1;
			while (arg->start <2) ;
			
			tx2.Begin();				
			for (m=1; m<FLAGS_txs;m++) {
			  
			    bool found;  

			    
			    
			    found = tx2.Get(0, m,  &r);		

			    if (m % 10 == 0 ) {
				  if (found) c1 = true;
				  break;
			    }
			    else if (!found)  {
			  	  c2 = true;
				  break;
			    }
			    else if (*r != 3) {
				  res = *r;
			  	  c3 = true;
				  break;			 			  
			    }
				
			}
			tx2.End();

			if (c1 ) {
				printf("Key %d not inserted but found\n", m);
				return;
			}
			else if (c2)  {
			  	printf("Key %d inserted but not found\n", m);
				return;
			}
			else if (c3) {
			  	printf("Key %d get %d instead of 3\n", m, res);
				return;			 			  
			}
			
			printf("ReadonlyTest pass!\n");	
			return;
		}

		else if (name == Slice("range")) {
	//		store->tables[0]->PrintStore();
			leveldb::DBROTX tx2(store);
			//DBTX tx2(store);
			bool c1 = false;bool c2 = false;bool c3 = false;			 
			  ThreadArg *arg = new ThreadArg();
			  arg->start = 0;
			  arg->store = store;
			  Env::Default()->StartThread(method, arg);
			  tx2.Begin();	  
			  
	//		  arg->start = 1;
	//		while (arg->start <2) ;
			 
			 DBROTX::Iterator iter(&tx2, 0);
			 //DBTX::Iterator iter(&tx2, 0);
			 iter.Seek(1); 
			 uint64_t key = 1;
			 uint64_t m = 0;
			 uint64_t *r; uint64_t res;
			 while (iter.Valid()) {
			    m = iter.Key();
			    r = iter.Value();
				
			   	if (m == FLAGS_txs - 3) {

					arg->start = 1;
					while (arg->start <2) ;

			   	}
				
			    if (m % 10 == 0 ) {
				  c1 = true;
				  break;
			    }
			    
			   
			    else if (m != key)  { 
					
			  	  c2 = true;
				  break;
			    }
			    else if (*r != 3) {
				  res = *r;
			  	  c3 = true;
				  break;			 			  
			    }
			    key++;
			    if  (m % 10 == 9) key++;
	//			if (m==99) printf("at end\n");
			    iter.Next();
	//			if (m==99) printf("next\n");
			 }
			 tx2.End();
			
			int end = FLAGS_txs -1 ;
			if (end % 10 ==0) end--;
			if (c1 ) {
				printf("Key %d not inserted but found\n", m);
				return;
			}
			else if (c2)  {
			  	printf("Key %d inserted but not found\n", key);
				return;
			}
			else if (c3) {
			  	printf("Key %d get %d instead of 3\n", m, res);
				return;			 			  
			}
			else if (m!=end) {
				printf("Iterate to %d should be %d\n", m, end);
				return;
			}

			tx2.Begin();
			 
			 iter.Seek(FLAGS_txs - 1); 
			 key = FLAGS_txs - 1;
			 m = 0;
			 
			 while (iter.Valid()) {
			    m = iter.Key();
			    r = iter.Value();
			 //   printf("get %d\n",m);
			    if (m % 10 == 0 ) {
				  c1 = true;
				  break;
			    }			    
			    else if (m != key)  {
				//	printf("get %d %d\n",m,key);
			  	  c2 = true;
				  break;
			    }
			    else if (*r != 3 && *r != 4) {
			  	  c3 = true;
				  res = *r;
				  break;			 			  
			    }
			    key--;
			    if  (m % 10 == 1) key--;
			    iter.Prev();
			 }
			 tx2.End();
			end = 1 ;
			
			if (c1 ) {
				printf("Prev Key %d not inserted but found\n", m);
				return;
			}
			else if (c2)  {
			  	printf("Prev Key %d inserted but not found\n", key);
				return;
			}
			else if (c3) {
			  	printf("Prev Key %d get %d instead of 3\n", m, res);
				return;			 			  
			}
			else if (m!=end) {
				printf("Prev Iterate to %d should be %d\n", m, end);
				return;
			}



			
			printf("RangeTest pass!\n");	
			return;
		}
		
		
		if (name == Slice("rwiter")) {
			assert(FLAGS_txs >= 34);
			ThreadArg *arg = new ThreadArg();
			arg->start = 0;
			arg->store = store;
			Env::Default()->StartThread(method, arg);

			uint64_t *value;
			DBTX rotx(store);
			rotx.Begin();
			rotx.Get(0, 10, &value);
			rotx.End();
			
			DBTX tx2(store);
			bool b = false;
			
			tx2.Begin();
			DBTX::Iterator iter(&tx2, 0);
			iter.SeekToFirst();
			
			for (int i = 0; i < 30; i++) {
				iter.Next();
			}
			arg->start = 1;

			printf("1\n");
			while (arg->start != 2);
			while (iter.Valid())
				iter.Next();
			b = tx2.End();
			if (b) {
				printf("Modify a key, should abort!\n");
				return;
			}

			
			tx2.Begin();
			iter.SeekToFirst();
			for (int i = 0; i < 30; i++) {
				iter.Next();
			}
			arg->start = 3;
			printf("3\n");
			while (arg->start != 4);
			while (iter.Valid())
				iter.Next();
			b = tx2.End();
			if (b) {
				printf("Insert a key, should abort!\n");
				return;
			}
			printf("RwIterTest pass!\n");
			return;
				
		}
		
		if (name == Slice("counter") ) {
			store->ThreadLocalInit(FLAGS_threads);
			
			leveldb::DBTX tx( store);
			bool b =false;
			while (b==false) {
			tx.Begin();
			

			uint64_t *value = new uint64_t();
			*value = 0;
			
			tx.Add(0, 1, value, 8);				
			delete value;									
			b = tx.End();
			
			//if (b==true)printf("%d\n", i);
			}
			//printf("init \n");
		}
		else if (name == Slice("nocycle") || name == Slice("nocycle_readonly")) {
			store->ThreadLocalInit(0);
			leveldb::DBTX tx( store);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			for (int i=0; i<num; i++) {

			  uint64_t *value = new uint64_t();
			  *value = 1;
			
			  tx.Add(0, i, value, 8);				
			  delete value;
			}									
			b = tx.End();
			
			//if (b==true)printf("%d\n", i);
			}
		}
	    else if (name == Slice("delete") || name == Slice("bigdelete") || name == Slice("secdelete")) {
			store->ThreadLocalInit(FLAGS_threads);
			leveldb::DBTX tx( store);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			for (int i=0; i<10; i++) {
			  uint64_t *value = new uint64_t();
			  *value = 1;
			  if ( i == 3 ||i == 6 || i == 4)//) 
			  	tx.Add(0, 0, i, 2, value, 8);
			  else tx.Add(0, i , value, 8);
			  delete value;
			}									
			b = tx.End();
			
			//if (b==true)printf("%d\n", i);
			}

			//store->tables[0]->PrintStore();

		}
		if (name == Slice("bigdelete")) {
			leveldb::DBTX tx(store);
			for (int i=20; i<100; i+=10) {
				bool b = false;
				while (b == false) {
					tx.Begin();
					for (int j = 0; j<10; j++) {
					    uint64_t *value = new uint64_t();
					    *value = 1;			
					    tx.Add(0, i+j, value, 8);
						delete value;
					}									
					b = tx.End();
						
				}
			}
			//store->tables[0]->PrintStore();
		}

		if (name == Slice("freedelete")) {
			store->ThreadLocalInit(FLAGS_threads);
			leveldb::DBTX tx(store);
			bool b = false;
			while (!b) {
				tx.Begin();
				for (int i=0; i<30; i++) {
					uint64_t *value = new uint64_t();
					*value = 5;
					tx.Add(0, i, value, 8);
					delete value;
				}
				b = tx.End();
			}	
		}
		
		SharedState shared;
		shared.total = num;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.num_phase = 0;
		shared.start = false;
		shared.fail = false;
		 ThreadArg* arg = new ThreadArg[num];
		 for (int i = 0; i < num; i++) {	 	
		 	arg[i].thread = new ThreadState(i);
			
			arg[i].store = store;
			
			arg[i].thread->shared = &shared;
//			printf("Out tid %lx\n", &arg[i]);
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
		 }else if (name == Slice("equal")) printf("EqualTest pass!\n");
		 else if (name == Slice("equalrange")) printf("EqualRangeTest pass!\n");
		 else if (name == Slice("nocycle")) printf("NocycleTest pass!\n");
		 else if (name == Slice("nocycle_readonly")) printf("NocycleReadonlyTest pass!\n");		 
		 else if (name == Slice("delete")) printf("DeleteTest pass!\n");
		 else if (name == Slice("bigdelete")) printf("BigDeleteTest pass!\n");
		 else if (name == Slice("secdelete")) printf("SecDeleteTest pass!\n");
		 else if (name == Slice("freedelete")) printf("FreeDeleteTest pass!\n");
		 else if (name == Slice("counter")) {
		 	
		 	
		 	leveldb::DBTX tx(store);
			bool b =false; int result;
			//printf("verify\n");
			while (b==false) {
			tx.Begin();			
			
			uint64_t* k = new uint64_t();
			*k = 1;
			
			uint64_t *str;
			tx.Get(0, *k, &str);
			result = *str;
			//printf("result %d\n",result);
			b = tx.End();
//			store->PrintList();
			}
			if (result != (FLAGS_txs*num)) {
				printf("Get %d instead of %d from the counter fail!\n",result,FLAGS_txs*num);
				
			}
			else printf("CounterTest pass!\n");
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
			printf("Benchmarks : \nequal\t Each tx write (KeyA, x) (KeyB, x) , check get(KeyA)==get(KeyB) in other transactions\ncounter\t badcount\n");
			printf("nocycle(nocycle_readonly)\t n threads, each tx write (tid,1) ((tid+1) %n,2) , never have all keys' value are the same\n");
			printf("delete\t write or delete 2 keys in a tx, check both keys exist or both not exist\n");
			printf("rwiter\t insert or modify a key during iterating, should abort\n");
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
	  else if (name == leveldb::Slice("equalrange")){
        method = &leveldb::Benchmark::EqualRangeTest;
      }	  
	  else if (name == leveldb::Slice("counter")) {
	  	method = &leveldb::Benchmark::CounterTest;
	  }
	  else if (name == leveldb::Slice("nocycle")) {
	  	method = &leveldb::Benchmark::NocycleTest;
	  }
	  else if (name == leveldb::Slice("nocycle_readonly")) {
	  	method = &leveldb::Benchmark::NocycleReadonlyTest;
	  }
	  else if (name == leveldb::Slice("delete")) {
	  	method = &leveldb::Benchmark::DeleteTest;
	  }
	  else if (name == leveldb::Slice("readonly") || name == leveldb::Slice("range")) {
	  	method = &leveldb::Benchmark::Company;
	  }
	  else if (name == leveldb::Slice("rwiter")) {
	  	method = &leveldb::Benchmark::InsertNode;
	  }
	  else if (name == leveldb::Slice("bigdelete")) {
	  	method = &leveldb::Benchmark::BigdeleteTest;
	  }
	  else if (name == leveldb::Slice("secdelete")) {
	  	method = &leveldb::Benchmark::SecDeleteTest;
	  }
	  else if (name == leveldb::Slice("freedelete")) {
	  	method = &leveldb::Benchmark::FreeDeleteTest;
	  }
	 //leveldb::DBTables *store = new leveldb::DBTables();
	 leveldb::DBTables *store = new leveldb::DBTables(1);
	 store->InitEpoch(FLAGS_threads+1);
	 store->RCUInit(FLAGS_threads+1);
	 store->PBufInit(FLAGS_threads+1);
	 store->AddTable(0, BTREE, IBTREE);
	  
	 leveldb::Benchmark *benchmark = new leveldb::Benchmark(store);
	  printf("run %s\n", name.ToString().c_str());
	  benchmark->Run(method, name);

	  store->Sync();
	  delete store;
    }
	

	
  	return 0;
}
	
	

