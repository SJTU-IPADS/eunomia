// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_TXSKIPLIST_H_
#define STORAGE_LEVELDB_DB_TXSKIPLIST_H_

#include <string>
#include "txdb.h"
#include "db/dbformat.h"
#include "util/arena.h"
#include "db/lockfreeSkiplist.h"


namespace leveldb {

class InternalKeyComparator;
class Mutex;
class MemTableIterator;

class TXSkiplist : public TXDB {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit TXSkiplist(const InternalKeyComparator& comparator);
  ~TXSkiplist();


  Iterator* NewIterator();


  virtual Status Put(const Slice& key,
					   const Slice& value, uint64_t seq);
  
  //  virtual Status PutBatch(const Slice& key,
	//					 const Slice& value, uint64_t seq) = 0;
  
	virtual Status Get(const Slice& key,
					   Slice* value, uint64_t seq);
	
	virtual Status Delete(const Slice& key, uint64_t seq);
  
    Status GetMaxSeq(const Slice& key, uint64_t* seq);
    void DumpTXSkiplist();


  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };

  typedef LockfreeSkipList<const char*, KeyComparator> Table;

private:
  

  KeyComparator comparator_;
  //FIXME: this should be thread safe
  Arena arena_;
  Table table_;

 //for debug
  port::Mutex mutex_;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
