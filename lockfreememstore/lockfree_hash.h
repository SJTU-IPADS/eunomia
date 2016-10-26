#ifndef LOCKFREEHASH_H
#define LOCKFREEHASH_H

#include <stdlib.h>
#include <stdio.h>

#include <iostream>
#include "port/atomic.h"
#include "memstore/memstore.h"
#include "memstore/memstore_hash.h"

namespace leveldb {

class LockfreeHashTable: public Memstore {

public:	
	struct HashNode {
		HashNode* next;
		uint64_t key;
		Memstore::MemNode memnode;
		
		
	};

	struct Head{
		HashNode *h;
	};

	static __thread HashNode *dummynode_;
	
	int length;
	Head *lists;
	
	LockfreeHashTable(){
		length = HASHLENGTH;
		lists = new Head[length];
		for (int i=0; i<length; i++)
			lists[i].h = NULL;
		
	}
	
	~LockfreeHashTable(){
	}
	
	inline void ThreadLocalInit() {
		if(dummynode_ == NULL) {
			dummynode_ = new HashNode();
		}
	}
	
	inline Memstore::InsertResult GetWithInsert(uint64_t key) {

		ThreadLocalInit();
		
		uint64_t hash = MemstoreHashTable::GetHash(key);
		
		HashNode* prev = (HashNode *)&lists[hash];

retry:
		HashNode* cur = prev->next;
		
		while(cur != NULL && cur->key < key) {
			prev = cur;
			cur = cur->next;
		}

		if(cur != NULL && cur->key == key)
			return {&cur->memnode,false};

		dummynode_->key = key;
		dummynode_->next = cur;

		uint64_t oldv = atomic_cmpxchg64(
			(uint64_t *)&prev->next, (uint64_t)cur, (uint64_t)dummynode_);

		//Concurrent insertion happened
		if(oldv != (uint64_t)cur) {
			cur = prev->next;
			goto retry;
		}
			
		
		Memstore::MemNode* res = &dummynode_->memnode;
		dummynode_ = NULL;
		return {res,false};
	}	

	

	inline MemNode* Put(uint64_t key, uint64_t* val) {

		Memstore::MemNode* node = GetWithInsert(key).node;
		node->value = val;
		return node;
	}

	inline Memstore::MemNode* Get(uint64_t key) {
		
		uint64_t hash = MemstoreHashTable::GetHash(key);
		
		HashNode* cur = lists[hash].h;

		while(cur != NULL && cur->key < key) {
			cur = cur->next;
		}

		if(cur != NULL && cur->key == key)
			return &cur->memnode;

		return NULL;
	}

	void PrintStore() {
		int count = 0;
		for (int i=0; i<length; i++) {
			
			if (lists[i].h != NULL)  {
				printf("Hash %d :\t", i);
				HashNode *n = lists[i].h;
				while (n!= NULL) {
					count++;
					printf("%ld \t", n->key);
					n = n->next;
				}
			}
			printf("\n");
		}
		
		printf("Total %d\n" , count);
		
	}

	
	
	Memstore::Iterator* GetIterator() { return NULL; }
	class Iterator: public Memstore::Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  Iterator(){}
	  // Returns true iff the iterator is positioned at a valid node.
	  bool Valid(){ return false; }

	  // Returns the key at the current position.
	  // REQUIRES: Valid()
	  MemNode* CurNode() { return NULL; }

	  
	  uint64_t Key() { return -1; }

	  // Advances to the next position.
	  // REQUIRES: Valid()
	  bool Next() { return false; }

	  // Advances to the previous position.
	  // REQUIRES: Valid()
	  bool Prev() { return false; }

	  // Advance to the first entry with a key >= target
	  void Seek(uint64_t key) {}

	  void SeekPrev(uint64_t key) {}

	  // Position at the first entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToFirst() {}

	  // Position at the last entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToLast() {}

	  uint64_t* GetLink() {return NULL; }

	  uint64_t GetLinkTarget() { return -1; }

	};
};
}

#endif
