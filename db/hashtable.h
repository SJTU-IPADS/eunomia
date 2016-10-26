// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_HASHTABLE_H_
#define STORAGE_LEVELDB_INCLUDE_HASHTABLE_H_

#include <stdint.h>
#include "leveldb/slice.h"
#include <stdlib.h>
#include "util/rtm_arena.h"
#include "util/rtm_arena.h"
#include "port/port.h"

namespace leveldb {

class HashTable {
 public:
  
  struct Node 
  {
//	  uint64_t *seqaddr;
	  uint64_t seq;
	  uint64_t hash;	  // Hash of key(); used for fast sharding and comparisons
	  Node* next;  
	  
	  uint32_t key_length;
	  char key_contents[1];
	  
	  Slice Getkey() const {
		return Slice(key_contents, key_length);
	}
  };


  struct Head 
  {
	  Node* h;
	  //port::SpinLock* spinlock;
  };

  struct SeqNumber
  {
	  uint64_t seq;
//	  char padding[56];
  };


  HashTable();

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~HashTable();

  bool GetMaxWithHash(uint64_t hash, uint64_t *seq_ptr);
  
  void UpdateWithHash(uint64_t hash, uint64_t seq);
  
  
  Node* GetNode(const Slice& key);
  Node* GetNodeWithInsert(const Slice& key);
  Node* Insert(const Slice& key, uint64_t seq);
  
  void PrintHashTable();

  uint64_t HashSlice(const Slice& s);
  
  private:
  	

  int length_;
  int elems_;
  Head* list_;
  
 // Node** list_;

  int seqIndex;
  SeqNumber* seqs;
  
  RTMArena* arena_;    // Arena used for allocations of nodes
  
  
  void Resize();
  Node* NewNode(const Slice & key);
  

 };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CACHE_H_
