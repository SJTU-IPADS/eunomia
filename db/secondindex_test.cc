// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtx.h"
#include "db/dbrotx.h"
#include "memstore/memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_uint64bplustree.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "db/dbtables.h"


static const char* FLAGS_benchmarks = "simple,interrupt,itersimple,roitersimple";

	

namespace leveldb {

typedef uint64_t Key;


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

	static void InsertNode(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		DBTables *store = arg->store;
		
		while (arg->start == 0) ;
		
		leveldb::DBTX tx( store);
		bool b =false;
			 
		while (!b) {
			tx.Begin();					  		
		  	uint64_t *value = new uint64_t();
		  	*value = 1;			
		  	tx.Add(0, 0, 4, 1, value);				
			b = tx.End();						  
		}			  
		arg->start = 2;
		printf("2\n");
		while (arg->start == 2) ;

		b = false;
		while (!b) {
			tx.Begin();					  		
		  	uint64_t *value = new uint64_t();
		  	*value = 4;			
		  	tx.Add(0, 0, 1, 4, value);				
			b = tx.End();						  
		}			  
		arg->start = 4;	
		printf("4\n");
	}	


	
	void Run(void (*method)(void* arg), Slice name ) {
		printf("%s start\n", name.ToString().c_str());				 		

		if (name == Slice("simple")) {
			DBTX tx(store);
			bool b = false;
			while (!b) {
				tx.Begin();
				uint64_t *v1 = new uint64_t();
				*v1 = 1;
				tx.Add(0, 0, 1, 1, v1);
				uint64_t *v2 = new uint64_t();
				*v2 = 1;
				tx.Add(0, 0, 2, 1, v2);
				printf("B\n");
				b = tx.End();
			}
			printf("Step 1\n");
			b = false;
			while (!b) {
				tx.Begin();
				DBTX::KeyValues* kvs = tx.GetByIndex(0, 1);
				if (kvs->num != 2)  
					printf("[1.1] Should get 2 records for sec key 1, get %d\n", kvs->num);
				if ((kvs->keys[0]+kvs->keys[1]) != 3) 
					printf("[1.2] Should get pri key 1,2 get %d %d\n", kvs->keys[0], kvs->keys[1]);
				uint64_t *v1 = new uint64_t();
				*v1 = 2;
				tx.Add(0, 0, 1, 2, v1);
				b = tx.End();
			}
			printf("Step 2\n");
			b = false;
			while  (!b) {
				tx.Begin();
				DBTX::KeyValues* kvs = tx.GetByIndex(0, 1);
				if (kvs->num != 1) {
					printf("[2.1] Should get 1 , get %d\n", kvs->num);
					tx.PrintKVS(kvs);
				}
				kvs = tx.GetByIndex(0, 2);
				if (kvs->num != 1) 
					printf("[2.2] Should get 1 , get %d\n", kvs->num);
				b = tx.End();
			}
			printf("Step 3\n");
			return;
		}

		if (name == Slice("interrupt")) {
			bool b = false;
			DBTX tx(store);
			ThreadArg *arg = new ThreadArg();
			arg->start = 0;
			arg->store = store;
			Env::Default()->StartThread(method, arg);
			while (!b) {
				tx.Begin();
				uint64_t *v1 = new uint64_t();
				*v1 = 1;
				tx.Add(0, 0, 1, 1, v1);
				uint64_t *v2 = new uint64_t();
				*v2 = 1;
				tx.Add(0, 0, 2, 1, v2);
				b = tx.End();
			}
			
			b = false;
			tx.Begin();
			DBTX::KeyValues* kvs = tx.GetByIndex(0, 1);
			arg->start = 1;
			while (arg->start == 1);
			b = tx.End();
			if (b) 
				printf("[3.1] Add node, should abort\n");
			
			b = false;				
			tx.Begin();
			kvs = tx.GetByIndex(0, 1);
			arg->start = 3;
			while (arg->start == 3);
			b = tx.End();
			if (b) 
				printf("[3.2] Delete node, should abort\n");
			
			
		}
		if (name == Slice("itersimple")) {
		
			ThreadArg *arg = new ThreadArg();
			arg->start = 0;
			arg->store = store;
			
			bool b = false;
			DBTX tx(store);
			while (!b) {
				tx.Begin();
				int pk = 0;
				// construct the memstore and secondary index
				//i is the secondary index
				for(int i = 1; i <= 10; i++) {
					//j is the primary key
					for(int j = 1; j <= i; j++) {
						uint64_t *v = new uint64_t();
						++pk;
						*v = i * pk;
						tx.Add(0, 0, pk, i, v);
					}
				}
				b = tx.End();
			}
			
			b = false;
			//Test step 1. normal verify
			while (!b) {
				tx.Begin();
				DBTX::SecondaryIndexIterator* siter = 
					new DBTX::SecondaryIndexIterator(&tx, 0);

				siter->SeekToFirst();
				while(siter->Valid()) {
					uint64_t key = siter->Key();
					DBTX::KeyValues* kvs = siter->Value();
					if(key != kvs->num) {
						printf("the number of value for key %ld is wrong[%d]\n", key, kvs->num);
						break;
					}

					for(int k = 0; k < kvs->num; k++) {
						if(*kvs->values[k] != (kvs->keys[k] * key)) {
							printf("Wrong Value %ld\n", *kvs->values[k]);
						}
					}
					siter->Next();
				}
				b = tx.End();
			}	


			b = false;
			//Test step 2. seek verify
			while (!b) {
				tx.Begin();
				DBTX::SecondaryIndexIterator* siter = 
					new DBTX::SecondaryIndexIterator(&tx, 0);

				siter->Seek(5);
				while(siter->Valid()) {
					uint64_t key = siter->Key();
					printf("key %ld\n", key);
					DBTX::KeyValues* kvs = siter->Value();
					if(key != kvs->num) {
						printf("the number of value for key %ld is wrong[%d]\n", key, kvs->num);
						break;
					}

					for(int k = 0; k < kvs->num; k++) {
						if(*kvs->values[k] != (kvs->keys[k] * key)) {
							printf("Wrong Value %ld\n", *kvs->values[k]);
						}
					}
					siter->Next();
				}
				b = tx.End();
			}	
			
		}

		if (name == Slice("roitersimple")) {
		
			ThreadArg *arg = new ThreadArg();
			arg->start = 0;
			arg->store = store;
			
			bool b = false;
			DBTX tx(store);
			while (!b) {
				tx.Begin();
				int pk = 0;
				// construct the memstore and secondary index
				//i is the secondary index
				for(int i = 1; i <= 10; i++) {
					//j is the primary key
					for(int j = 1; j <= i; j++) {
						uint64_t *v = new uint64_t();
						++pk;
						*v = i * pk;
						tx.Add(0, 0, pk, i, v);
					}
				}
				b = tx.End();
			}

			DBROTX rotx(store);
			b = false;
			//Test step 1 normal verify
			while (!b) {
				rotx.Begin();
				DBROTX::SecondaryIndexIterator* siter = 
					new DBROTX::SecondaryIndexIterator(&rotx, 0);

				siter->SeekToFirst();
				while(siter->Valid()) {
					uint64_t key = siter->Key();
					DBROTX::KeyValues* kvs = siter->Value();
					if(key != kvs->num) {
						printf("the number of value for key %ld is wrong[%d]\n", key, kvs->num);
						break;
					}

					for(int k = 0; k < kvs->num; k++) {
						if(*kvs->values[k] != (kvs->keys[k] * key)) {
							printf("Wrong Value %ld\n", *kvs->values[k]);
						}
					}
					siter->Next();
				}
				b = rotx.End();
			}
			
		    //Test step 2. write some other tuples on the new snapshot
			b = false;
			while (!b) {
				tx.Begin();
				int pk = 0;
				// construct the memstore and secondary index
				//i is the secondary index
				for(int i = 1; i <= 10; i++) {
					//j is the primary key
					for(int j = 1; j <= i; j++) {
						uint64_t *v = new uint64_t();
						++pk;
						*v = pk * pk;
						tx.Add(0,pk, v);
					}
				}
				b = tx.End();
			}

			//step 3. verify the latest value
			b = false;

			while (!b) {
				rotx.Begin();
				DBROTX::SecondaryIndexIterator* siter = 
					new DBROTX::SecondaryIndexIterator(&rotx, 0);

				siter->SeekToFirst();
				while(siter->Valid()) {
					uint64_t key = siter->Key();
					DBROTX::KeyValues* kvs = siter->Value();
					if(key != kvs->num) {
						printf("the number of value for key %ld is wrong[%d]\n", key, kvs->num);
						break;
					}

					for(int k = 0; k < kvs->num; k++) {
						if(*kvs->values[k] != (kvs->keys[k] * kvs->keys[k])) {
							printf("Wrong Value %ld\n", *kvs->values[k]);
						}
					}
					siter->Next();
				}
				b = rotx.End();
			}


			//step 4. verify the old value
			b = false;
			while (!b) {
				rotx.Begin();
				rotx.oldsnapshot = 0;
				DBROTX::SecondaryIndexIterator* siter = 
					new DBROTX::SecondaryIndexIterator(&rotx, 0);

				siter->SeekToFirst();
				while(siter->Valid()) {
					uint64_t key = siter->Key();
					DBROTX::KeyValues* kvs = siter->Value();
					if(key != kvs->num) {
						printf("the number of value for key %ld is wrong[%d]\n", key, kvs->num);
						break;
					}

					for(int k = 0; k < kvs->num; k++) {
						if(*kvs->values[k] != (kvs->keys[k] * key)) {
							printf("Wrong Value %ld\n", *kvs->values[k]);
						}
					}
					siter->Next();
				}
				b = rotx.End();
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
			return 0;
	 	 }
		 if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
		   FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
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
	  if (name == leveldb::Slice("simple")){
        method = NULL;
      }
	  else if (name == leveldb::Slice("interrupt")){
        method = &leveldb::Benchmark::InsertNode;
      }

	 leveldb::DBTables *store = new leveldb::DBTables(1);
	 store->AddTable(0, BTREE, IBTREE);
	
	  
	 leveldb::Benchmark *benchmark = new leveldb::Benchmark(store);

	  benchmark->Run(method, name);

	  delete store;
    }
	

	
  	return 0;
}
	
	

