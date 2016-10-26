// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_TXMEMSTORETEMPLATE_H_
#define STORAGE_LEVELDB_DB_TXMEMSTORETEMPLATE_H_

#include <string>
#include "txdb.h"
#include "db/dbformat.h"
#include "util/arena.h"
#include "db/lockfreeSkiplist.h"


namespace leveldb {

template<typename Key, typename Value, class Comparator>
class TXMemStore{

 public:
 	
  TXMemStore(Comparator comparator);
  ~TXMemStore();

  Status Put(Key k, Value* val, uint64_t seq);
  Status Get(Key k, Value** val, uint64_t seq);
  Status Delete(Key k, uint64_t seq);
  Status GetMaxSeq(Key k, uint64_t* seq);
  void DumpTXMemStore();


  struct TableKey {
  	Key k;
	Value* v;
	uint64_t seq;
  };

  
  class TableKeyComparator : public Comparator {
  	
  public:
    const Comparator user_comparator_;
    explicit TableKeyComparator(const Comparator& c) : user_comparator_(c) { }
    virtual const char* Name() const {assert(0);}
    virtual int Compare(const Slice& a, const Slice& b) const {assert(0);}
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const {assert(0);}
    virtual void FindShortSuccessor(std::string* key) const {assert(0);}
 
  
	int Compare(const TableKey& a, const TableKey& b) const 
	{
		int r = user_comparator_(a.k, b.k);
		if(r == 0) {
		  if(a.seq > b.seq)
		    return -1;
		  else if(a.seq < b.seq)
		    return 1;
		  else
		    return 0;
		}
		return r;
	}

	int operator()(const TableKey& a, const TableKey& b) const 
	{
		int r = user_comparator_(a.k, b.k);
		if(r == 0) {
		  if(a.seq > b.seq)
		    return -1;
		  else if(a.seq < b.seq)
		    return 1;
		  else
		    return 0;
		}
		return r;
			
	}
	
  };

  typedef LockfreeSkipList<TableKey, TableKeyComparator> Table;

public:
  

  TableKeyComparator comparator_;
  //FIXME: this should be thread safe
  Arena arena_;
  Table table_;

 //for debug
  port::Mutex mutex_;

};

template<typename Key, typename Value, class Comparator>
TXMemStore<Key, Value, Comparator>::TXMemStore(Comparator cmp):
			comparator_(cmp), table_(comparator_, NULL) {}

template<typename Key, typename Value, class Comparator> 
TXMemStore<Key, Value, Comparator>::~TXMemStore(){}

template<typename Key, typename Value, class Comparator> 
Status TXMemStore<Key, Value, Comparator>::Put(Key k, Value* val, uint64_t seq)
{

  Table::ThreadLocalInit();

  TableKey key;
  key.k = k;
  key.v = val;
  key.seq = seq;

  table_.Insert(key);

  return Status::OK();
}

template<typename Key, typename Value, class Comparator> 
Status TXMemStore<Key, Value, Comparator>::Get(Key k, Value** val, uint64_t seq)
{

  Table::ThreadLocalInit();

//  printf("get key %ld seq %ld\n", k, seq);
  
  TableKey key;
  key.k = k;
  key.v = NULL;
  key.seq = seq;
   
  typename Table::Iterator iter(&table_);
  iter.Seek(key);
   
  if (iter.Valid()) {
	
	TableKey res = iter.key();
	Status s;
	if (comparator_.user_comparator_(k, res.k) == 0) {
				  
	  if(seq != res.seq)
	    return Status::NotFound(Slice());

      *val = res.v;
	  return Status::OK();

	}
  }
  return Status::NotFound(Slice());
}


template<typename Key, typename Value, class Comparator> 
Status TXMemStore<Key, Value, Comparator>::Delete(Key k, uint64_t seq)
{
  Table::ThreadLocalInit();
  assert(0);
  return Status::NotFound(Slice());
}

template<typename Key, typename Value, class Comparator> 
Status TXMemStore<Key, Value, Comparator>::GetMaxSeq(Key k, uint64_t* seq)
{
  Table::ThreadLocalInit();

  TableKey key;
  key.k = k;
  key.v = NULL;
  key.seq = (uint64_t)-1;
   
  typename Table::Iterator iter(&table_);
  iter.Seek(key);
	
  if (iter.Valid()) {
    TableKey res = iter.key();

  	if (comparator_.user_comparator_(k, res.k) == 0) {
	  *seq = res.seq;
  	  return Status::OK();
    }
  }
  return Status::NotFound(Slice());
}


template<typename Key, typename Value, class Comparator> 
void TXMemStore<Key, Value, Comparator>::DumpTXMemStore()
{
	
	  typename Table::Iterator iter(&table_);
	  iter.SeekToFirst();
	  while(iter.Valid()) {
		TableKey k = iter.key();
		mutex_.Lock();
		printf("Key %ld ", k.k);
		printf("Value %ld ", *k.v);
		printf("Seq %ld", k.seq);
		printf("\n");
		mutex_.Unlock();
		iter.Next();
	  }
}




}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
