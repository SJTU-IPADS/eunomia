// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DBROTX_H
#define DBROTX_H

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "db/dbtables.h"
#include "db/dbtx.h"

#define TREE_TIME 0
#define RO_TIME_BKD 0

namespace leveldb {

class DBROTX {
 public:
	uint64_t treetime;
	uint64_t begintime, gettime, endtime, nexttime, prevtime, seektime;
	uint64_t begins, gets, ends, nexts, prevs, seeks;

	time_bkd ronext_time, roget_time;
	
	DBROTX (DBTables* store);
	~DBROTX();

	void Begin();
	bool Abort();
	bool End();
	
	bool Get(int tableid, uint64_t key, uint64_t** val);

	struct KeyValues {

	  int num;
	  uint64_t *keys;
	  uint64_t **values;
	  
	  KeyValues(int num)
	  {
	  	keys = new uint64_t[num];
		values = new uint64_t* [num];
	  }
	  ~KeyValues()
	  {
	  	delete[] keys;
		delete[] values;
	  }
	};
	
	class Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  explicit Iterator(DBROTX* rotx, int tableid);
	
	  // Returns true iff the iterator is positioned at a valid node.
	  bool Valid();
	
	  // Returns the key at the current position.
	  // REQUIRES: Valid()
	  uint64_t Key();

	  uint64_t* Value();
	
	  // Advances to the next position.
	  // REQUIRES: Valid()
	  void Next();
	
	  // Advances to the previous position.
	  // REQUIRES: Valid()
	  void Prev();
	
	  // Advance to the first entry with a key >= target
	  void Seek(uint64_t key);
	
	  // Position at the first entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToFirst();
	
	  // Position at the last entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToLast();
	
	 private:
	  DBROTX* rotx_;
	  Memstore::MemNode  *cur_;
	  Memstore::Iterator *iter_;
	  uint64_t *val_;
	  // Intentionally copyable
	};


	class SecondaryIndexIterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  explicit SecondaryIndexIterator(DBROTX* rotx, int tableid);
	
	  // Returns true iff the iterator is positioned at a valid node.
	  bool Valid();
	
	  // Returns the key at the current position.
	  // REQUIRES: Valid()
	  uint64_t Key();

	  KeyValues* Value();
	
	  // Advances to the next position.
	  // REQUIRES: Valid()
	  void Next();
	
	  // Advances to the previous position.
	  // REQUIRES: Valid()
	  void Prev();
	
	  // Advance to the first entry with a key >= target
	  void Seek(uint64_t key);
	
	  // Position at the first entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToFirst();
	
	  // Position at the last entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToLast();
	
	 private:
	  DBROTX* rotx_;
	  SecondIndex* index_;
	  SecondIndex::SecondNode* cur_;
	  SecondIndex::Iterator *iter_;
	  KeyValues* val_;
	  // Intentionally copyable
	};
	
public:
	inline bool ScanMemNode(Memstore::MemNode* n, uint64_t** val);
	inline bool GetValueOnSnapshot(Memstore::MemNode* n, uint64_t** val);
	inline bool GetValueOnSnapshotByIndex(SecondIndex::SecondNode* n, KeyValues* kvs);
	
	uint64_t oldsnapshot;
	DBTables *txdb_ ;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
