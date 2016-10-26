#ifndef MEMSTOREHASH_H
#define MEMSTOREHASH_H

#include <stdlib.h>
#include <iostream>

#include "util/rtmScope.h"
#include "util/rtm.h"
#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"
#include "memstore.h"

#define HASH_LOCK 0
#define HASHLENGTH 256*1024*1024

namespace leveldb {

class MemstoreHashTable: public Memstore {

public:
	struct HashNode {
		//The first field should be next
		HashNode* next;
		uint64_t key;
		Memstore::MemNode memnode;
	};

	struct Head {
		HashNode *h;
		//char padding[56];
	};

	int length;
	char padding0[64];
	Head *lists;
	char padding1[64];
	RTMProfile prof;
	char padding2[64];
	SpinLock rtmlock;
	char padding3[64];
	port::SpinLock slock;
	char padding4[64];
	static __thread HashNode *dummynode_;

	MemstoreHashTable() {
		length = HASHLENGTH;
		lists = new Head[length];
		for(int i = 0; i < length; i++)
			lists[i].h = NULL;
	}

	~MemstoreHashTable() {
//		printf("=========HashTable========\n");
//		PrintStore();
		prof.reportAbortStatus();
	}

	inline void ThreadLocalInit() {
		if(dummynode_ == NULL) {
			dummynode_ = new HashNode();
		}
	}

	static inline uint64_t MurmurHash64A(uint64_t key, unsigned int seed)  {

		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;
		uint64_t h = seed ^ (8 * m);
		const uint64_t * data = &key;
		const uint64_t * end = data + 1;

		while(data != end)  {
			uint64_t k = *data++;
			k *= m;
			k ^= k >> r;
			k *= m;
			h ^= k;
			h *= m;
		}

		const unsigned char * data2 = (const unsigned char*)data;

		switch(8 & 7)   {
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

	static inline uint64_t GetHash(uint64_t key) {
		return MurmurHash64A(key, 0xdeadbeef) & (HASHLENGTH - 1);
		//return key % HASHLENGTH ;
	}

	inline MemNode* Put(uint64_t key, uint64_t* val) {
		ThreadLocalInit();

		Memstore::MemNode* res = GetWithInsert(key).node;

		res->value = val;

		return res;
	}

	inline InsertResult GetWithInsert(uint64_t key) {
		ThreadLocalInit();

		MemNode* mn = Insert_rtm(key);

		return {mn,false};
	}
	inline Memstore::MemNode* GetForRead(uint64_t key) {
		ThreadLocalInit();

		MemNode* mn = Insert_rtm(key);

		return mn;
	}

	inline Memstore::MemNode* Get(uint64_t key) {

		uint64_t hash = GetHash(key);

		HashNode* cur = lists[hash].h;

		while(cur != NULL && cur->key < key) {
			cur = cur->next;
		}

		if(cur != NULL && cur->key == key)
			return &cur->memnode;

		return NULL;
	}

	inline Memstore::MemNode* Insert_rtm(uint64_t key) {
		uint64_t hash = GetHash(key);

#if HASH_LOCK
		MutexSpinLock lock(&slock);
#else
		RTMScope begtx(&prof, 1, 1, &rtmlock);
//		RTMArenaScope begtx(&rtmlock, &prof, NULL);
#endif

		HashNode* prev = (HashNode *)&lists[hash];
		HashNode* cur = prev->next;

		while(cur != NULL && cur->key < key) {
			prev = cur;
			cur = cur->next;
		}

		if(cur != NULL && cur->key == key)
			return &cur->memnode;


		prev->next = dummynode_;
		dummynode_->next = cur;

		dummynode_->key = key;

		Memstore::MemNode* res = &dummynode_->memnode;
		dummynode_ = NULL;

		return res;
	}

	void PrintStore() {
		int total = 0;
		for(int i = 0; i < length; i++) {
			int count = 0;
			if(lists[i].h != NULL)  {
				//printf("Hash %ld :\t", i);
				HashNode *n = lists[i].h;
				while(n != NULL) {
					count++;
					//printf("%ld \t", n->key);
					n = n->next;
					total++;
				}
				if(count > 10) printf(" %d\n" , count);
				//printf("\n");
			}
		}

		printf("Total Count %d\n", total);
	}

	Memstore::Iterator* GetIterator() {
		return NULL;
	}
	class Iterator: public Memstore::Iterator {
	public:
		// Initialize an iterator over the specified list.
		// The returned iterator is not valid.
		Iterator() {}
		// Returns true iff the iterator is positioned at a valid node.
		bool Valid() {
			return false;
		}

		// Returns the key at the current position.
		// REQUIRES: Valid()
		MemNode* CurNode() {
			return NULL;
		}

		uint64_t Key() {
			return -1;
		}

		// Advances to the next position.
		// REQUIRES: Valid()
		bool Next() {
			return false;
		}

		// Advances to the previous position.
		// REQUIRES: Valid()
		bool Prev() {
			return false;
		}

		// Advance to the first entry with a key >= target
		void Seek(uint64_t key) {}

		void SeekPrev(uint64_t key) {}

		// Position at the first entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToFirst() {}

		// Position at the last entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToLast() {}

		uint64_t* GetLink() {
			return NULL;
		}

		uint64_t GetLinkTarget() {
			return -1;
		}

	};
};
}

#endif
