// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtransaction.h"
#include "db/hashtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/rtm.h"
#include "util/hash.h"
#include "db/txleveldb.h"
#include "db/txskiplist.h"
#include "txdb.h"



namespace leveldb {

  DBTransaction::ReadSet::ReadSet() {

	max_length = 1024; //first allocate 1024 numbers
  	elems = 0;
	
	seqs = new RSSeqPair[max_length];
  }

  DBTransaction::ReadSet::~ReadSet() {
	delete[] seqs;	
  }

void  DBTransaction::ReadSet::Resize() {
  	
	max_length = max_length * 2;

	RSSeqPair *ns = new RSSeqPair[max_length];
	Data** nk = new Data*[max_length];

	for(int i = 0; i < elems; i++) {
		ns[i] = seqs[i];
	}

	delete[] seqs;

	seqs = ns;
  }
  
  void DBTransaction::ReadSet::Add(uint64_t hash, uint64_t oldeseq, uint64_t *ptr)
  {
	if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
	assert(elems <= max_length);
	
	if(elems == max_length)
		Resize();

	int cur = elems;
	elems++;

	seqs[cur].seq = ptr;
	seqs[cur].oldseq = oldeseq;
	seqs[cur].hash = hash;
  }

  bool DBTransaction::ReadSet::Validate(HashTable* ht) {

	//This function should be protected by rtm or mutex
	
	for(int i = 0; i < elems; i++) {

		if(seqs[i].seq != NULL 
			&& seqs[i].oldseq != *seqs[i].seq)
			return false;
		//printf("[%lx Get Validate] old seq %ld seq %ld\n", pthread_self(), seqs[i].oldseq, *seqs[i].seq);
		if(seqs[i].seq == NULL) {
			
			//doesn't read any thing
			uint64_t curseq = 0; //Here must initialized as 0
			
			//TODO: we can just use the hash to find the key
			bool found = ht->GetMaxWithHash(seqs[i].hash, &curseq);

			if(seqs[i].oldseq != curseq) {
				assert(found);
				return false;
			}
		}
	}

	return true;
  }

  void DBTransaction::ReadSet::Print()
  {
	for(int i = 0; i < elems; i++) {
		printf("Key[%d] ", i);
		if(seqs[i].seq != NULL) {
			printf("Old Seq %lu Cur Seq %lu Seq Addr 0x%p ", 
				seqs[i].oldseq, *seqs[i].seq, seqs[i].seq);
		}
	}
  }


  void DBTransaction::ReadSet::Reset() 
  {
	elems = 0;
  }
  
  DBTransaction::WriteSet::WriteSet() {

	max_length = 1024; //first allocate 1024 numbers
  	elems = 0;

	seqs = new WSSeqPair[max_length];
	kvs = new WSKV[max_length];

/*
	for(int i = 0; i < 64; i++) {
		cacheset[i] = 0;
		for(int j = 0; j < 8; j++) {
			cacheaddr[i][j] = 0;
			cachetypes[i][j] = 0;
		}
	}
*/
  }

  DBTransaction::WriteSet::~WriteSet() {
/*
	for(int index = 0; index < 64; index++) {

	 	printf("Cache Set [%d] count %d\n", index, cacheset[index]);
		for(int i = 0; i < 8; i++) {
			printf("[%lx] ",  cacheaddr[index][i]);
		}
		printf("\n");
	}
	*/
	delete[] seqs;
	for(int i = 0; i < elems; i++) {
		free(kvs[i].key);
		free(kvs[i].val);
	}
	delete[] kvs;
  }

void  DBTransaction::WriteSet::Resize() {
  	
	max_length = max_length * 2;

	WSSeqPair* nss = new WSSeqPair[max_length];
	WSKV* nkv = new WSKV[max_length];
	
	for(int i = 0; i < elems; i++) {
		nss[i] = seqs[i];
		nkv[i] = kvs[i];
	}

	delete[] seqs;
	delete[] kvs;

	seqs = nss;
	kvs = nkv;
  }

  void DBTransaction::WriteSet::Add(ValueType type, const Slice& key, 
  											const Slice& val, uint64_t *seqptr)
  {

	assert(elems <= max_length);
	
	if(elems == max_length)
		Resize();

	int cur = elems;
	elems++;
	
	Data* kp = reinterpret_cast<Data*>(
    	malloc(sizeof(Data)-1 + key.size()));
    kp->length = key.size();
    memcpy(kp->contents, key.data(), key.size());
	kvs[cur].key = kp;

	Data* vp = reinterpret_cast<Data*>(
    	malloc(sizeof(Data)-1 + val.size()));	
	vp->length = val.size();
	memcpy(vp->contents, val.data(), val.size());
	kvs[cur].val= vp;
	
	kvs[cur].type = type;

	seqs[cur].seqptr = seqptr;
	seqs[cur].wseq = 0;

	//TouchAddr((uint64_t)seqptr, 3);
		
  }

   void DBTransaction::WriteSet::TouchAddr(uint64_t addr, int type)
  {
  	
  	 uint64_t caddr = addr >> 12;
     int index = (int)((addr>>6)&0x3f);

//printf("addr %lx, paddr %lx, index %d\n", addr, caddr, index);
	 for(int i = 0; i < 8; i++) {

		//printf("set %d , index %d, paddr %lx\n", i, index, cacheaddr[index][i]);
		
	 	if(cacheaddr[index][i] == caddr) {
			//printf("!!!!!\n");
			return;

		}
	 }
	 
	 cacheset[index]++;
	 static int count = 0;
	 if( cacheset[index] > 8) {
	 	count++;
	 	printf("Cache Set [%d] Conflict type %d\n", index ,type );
		for(int i = 0; i < 8; i++) { 
			printf("[%lu][%lu] ", cachetypes[index][i], cacheaddr[index][i]);
		}
		printf(" %d \n", count);
	 }

	 for(int i = 0; i < 8; i++) {
	 	if(cacheaddr[index][i] == 0) {
	 	  	cacheaddr[index][i] = caddr;
			cachetypes[index][i] = type;
				//		printf("XXX\n");
			return;
	 	}
	 }

  }

  
  void DBTransaction::WriteSet::UpdateGlobalSeqs(HashTable* ht) {

	//This function should be protected by rtm or mutex
	
	for(int i = 0; i < elems; i++) {
		seqs[i].wseq = *seqs[i].seqptr;
		seqs[i].wseq++;
		*seqs[i].seqptr = seqs[i].wseq;

	//	TouchAddr((uint64_t)&seqs[i].wseq, 1);
	//	TouchAddr((uint64_t)&seqs[i].seqptr, 2);
	//	TouchAddr((uint64_t)seqs[i].seqptr, 3);
		
	}

  }

  bool DBTransaction::WriteSet::Lookup(const Slice& key, ValueType* type, Slice* val) 
  {
	  for(int i = 0; i < elems; i++) {
		if(kvs[i].key->Getslice()== key) {
			*type = kvs[i].type;
			*val = kvs[i].val->Getslice();
			return true;
		}
	  }

	  return false;
  }

  void DBTransaction::WriteSet::Commit(TXDB *txdb) 
  {
	//commit the local write set into the memory storage
	//should holde the mutex of memstore	
	for(int i = 0; i < elems; i++) {
		//printf("[%lx Commit] Insert Seq %ld Key %s\n", pthread_self(), 
			//seqs[i].wseq, kvs[i].key->Getslice());
		txdb->Put(kvs[i].key->Getslice(),kvs[i].val->Getslice(), seqs[i].wseq);
	}
  }

  void DBTransaction::WriteSet::Print()
  {
	for(int i = 0; i < elems; i++) {
		/*
		printf("Key[%d] ", i);
		if(seqs[i].seq != NULL) {
			printf("Old Seq %ld Cur Seq %ld Seq Addr 0x%lx ", 
				seqs[i].oldseq, *seqs[i].seq, seqs[i].seq);
		}

		printf("key %s  ", keys[i]->Getslice());
		printf("hash %ld\n", hashes[i]);
		*/
	}
  }

  void DBTransaction::WriteSet::Reset() 
  {
	elems = 0;

	/*
	for(int i = 0; i < 64; i++) {
		cacheset[i] = 0;
		for(int j = 0; j < 8; j++) {
			cacheaddr[i][j] = 0;
			cachetypes[i][j] = 0;
		}
	}
   */
  }
  
  DBTransaction::DBTransaction(HashTable* ht, TXDB* db, port::Mutex* mutex)
  {
	//get the globle store and versions passed by the parameter
	storemutex = mutex;
	latestseq_ = ht;
	txdb_ = db;

	readset = new ReadSet();
	writeset = new WriteSet();

	count = 0;
  }
  
  DBTransaction::~DBTransaction()
  {
	//clear all the data
	delete readset;
	delete writeset;
		
  }

  void DBTransaction::Begin()
  {
	//reset the local read set and write set
	readset->Reset();
	writeset->Reset();
	
  }

  bool DBTransaction::Abort()
  {
	return false;
  }
  
  bool DBTransaction::End()
  {
	if( !Validation())
		return false;

	GlobalCommit();
	return true;
  }
  void DBTransaction::Add(ValueType type, Slice& key, Slice& value)
  {
  	//Get the seq addr from the hashtable
	HashTable::Node* node = latestseq_->GetNodeWithInsert(key);
	
	//write the key value into local buffer	
	writeset->Add(type, key, value, &node->seq);
  }

  //FIXME: value should use slice instead of string !!!
  bool DBTransaction::Get(const Slice& key, Slice* value, Status* s)
  {
  	//step 1. First check if the <k,v> is in the write set
  	
	Slice val;
	ValueType type;
	if(writeset->Lookup(key, &type, value)) {
		//Found
		switch (type) {
          case kTypeValue: {
          	//value->assign(val.data(), val.size());
          	return true;
          }
          case kTypeDeletion:
          	*s = Status::NotFound(Slice());
          	return true;
      	}	
	}
	

	//step 2.  Read the <k,v> from the in memory store

	uint64_t seq = 0;

	HashTable::Node* node = latestseq_->GetNode(key);

	if ( NULL == node) {
		//even not found, still need to put the k into read set to avoid concurrent insertion
		readset->Add(MurmurHash64A(key.data(), key.size(), 0), seq, (uint64_t *)0);
		
		return false;
	}

	seq = node->seq;

	//This is an empty node (garbage)
	if(seq == 0)
		return false;

	Status res;
	//may be not found, should wait for a while
	int count = 0;
	do{
		
		res = txdb_->Get(key, value, seq);

		count++;
		if (count > 1000) {
			printf("Not found seq %lu  key %s \n", seq, key.data());
			TXSkiplist* sl = (TXSkiplist*)txdb_;
//			sl->DumpTXSkiplist();
			return false;
		}
		
	}while(res.IsNotFound());

	// step 3. put into the read set
	readset->Add(node->hash, seq, &node->seq);
	
	return true;
  }

  bool DBTransaction::Validation() {

	//writeset->PrintHashTable();	
	RTMScope rtm(&rtmProf);
	//MutexLock mu(storemutex);
	
	//step 1. check if the seq has been changed (any one change the value after reading)
	if( !readset->Validate(latestseq_))
		return false;
	
	//step 2.  update the the seq set 
	//can't use the iterator because the cur node may be deleted 
	writeset->UpdateGlobalSeqs(latestseq_);
	
	return true;
	
  }


  
  void DBTransaction::GlobalCommit() {
	//commit the local write set into the memory storage		

	writeset->Commit(txdb_);
  }

}  // namespace leveldb

/*
int main()
{
	leveldb::ValueType t = leveldb::kTypeValue;
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;

	const char* path = "/root/tmp";
	
	
    leveldb::Status s = leveldb::DB::Open(options, path, &db);

	leveldb::HashTable seqs;	
	leveldb::port::Mutex mutex;
	leveldb::TXDB* txdb = new leveldb::TXLeveldb(db);
	
	leveldb::DBTransaction tx(&seqs, txdb, &mutex);	
	tx.Begin();
	
    for(int i = 0; i < 10; i++) {
		char* key = new char[100];
        snprintf(key, sizeof(key), "%d", i);
		leveldb::Slice *k = new leveldb::Slice(key);
		leveldb::Slice *v = new leveldb::Slice(key);
		printf("Insert %s ", *k);
		printf(" Value %s\n", *v);
		tx.Add(t, *k, *v);
    }

	tx.End();


	tx.Begin();	

	for(int i = 0; i < 10; i++) {
		char key[100];
        snprintf(key, sizeof(key), "%d", i);
		leveldb::Slice k(key);
		std::string *str = new std::string();
		leveldb::Status s;
		tx.Get(k, str, &s);
		printf("Get %s ", k);
		printf(" Value %s\n", str->c_str());
    }

	tx.End();

    return 0;
 }

//For SkipList
int main()
{
	leveldb::ValueType t = leveldb::kTypeValue;
	leveldb::HashTable seqs;	
	leveldb::port::Mutex mutex;
	
	leveldb::Options options;
	leveldb::InternalKeyComparator cmp(options.comparator);

	 leveldb::TXSkiplist* txdb = new leveldb::TXSkiplist(cmp);
		
	leveldb::DBTransaction tx(&seqs, txdb, &mutex); 
	tx.Begin();
	
	for(int i = 0; i < 10; i++) {
		char* key = new char[100];
		snprintf(key, sizeof(key), "%d", i);
		leveldb::Slice *k = new leveldb::Slice(key);
		leveldb::Slice *v = new leveldb::Slice(key);
		printf("Insert %s ", *k);
		printf(" Value %s\n", *v);
		tx.Add(t, *k, *v);
	}

	tx.End();

	txdb->DumpTXSkiplist();


	tx.Begin(); 

	for(int i = 0; i < 10; i++) {
		char key[100];
		snprintf(key, sizeof(key), "%d", i);
		leveldb::Slice k(key);
		std::string *str = new std::string();
		leveldb::Status s;
		tx.Get(k, str, &s);
		printf("Get %s ", k);
		printf(" Value %s\n", str->c_str());
	}

	tx.End();
}

*/


