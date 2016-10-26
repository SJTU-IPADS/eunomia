#ifndef MEMSTORE_H_
#define MEMSTORE_H_
#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <new>
#include "db/objpool.h"
#include "util/txprofile.h"
#include "util/rtm.h"

class Memstore {
public:
	struct MemNode {
		uint64_t seq;
		uint64_t counter;
		uint64_t* value; //pointer of the real value. 1: logically delete 2: Node is removed from memstore
		MemNode* oldVersions;
		int gcRef;
		//char padding[16];
		MemNode() {
			counter = 0;
			seq = 0;
			value = NULL;
			oldVersions = NULL;
			gcRef = 0;
		}

		//For debuging
		void Print() {
#if 0
			printf("Mem Addr %lx, Counter %ld, Seq %ld, Value Addr %lx, Old Addr %lx\nå",
				   this, counter, seq, value, oldVersions);
#endif
		}
	};

	struct InsertResult {
		MemNode* node;
		bool hasNewNode;
	};

	class Iterator {
	public:
		// Initialize an iterator over the specified list.
		// The returned iterator is not valid.
		Iterator() {}

		virtual bool Valid() = 0;

		// Returns the key at the current position.
		// REQUIRES: Valid()
		virtual MemNode* CurNode() = 0;

		virtual uint64_t Key() = 0;

		// Advances to the next position.
		// REQUIRES: Valid()
		virtual bool Next() = 0;

		// Advances to the previous position.
		// REQUIRES: Valid()
		virtual bool Prev() = 0;

		// Advance to the first entry with a key >= target
		virtual void Seek(uint64_t key) = 0;

		virtual void SeekPrev(uint64_t key) = 0;

		// Position at the first entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		virtual void SeekToFirst() = 0;

		// Position at the last entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		virtual void SeekToLast() = 0;

		virtual uint64_t* GetLink() = 0;

		virtual uint64_t GetLinkTarget() = 0;

	};

public:
	unsigned thread_num;
	Memstore() {};
	virtual ~Memstore() {
		//printf("~Memstore()\n");
	};

	//Only for initialization
	virtual Memstore::Iterator* GetIterator() = 0;

	virtual MemNode* Put(uint64_t k, uint64_t* val) = 0;

	virtual MemNode* Get(uint64_t key) = 0;

	virtual InsertResult GetWithInsert(uint64_t key) = 0;

	virtual MemNode* GetForRead(uint64_t key) = 0;

	virtual MemNode* GetWithDelete(uint64_t key) {
		assert(0);
	}

	virtual void PrintStore() {
		assert(0);
	}

	virtual void ThreadLocalInit() {
		assert(0);
	}

	static MemNode* GetMemNode() {
		//char* mn = (char *)malloc(sizeof(OBJPool::Obj) + sizeof(Memstore::MemNode));
		char* mn = (char *)malloc(64);

		//if(((uint64_t)mn & 63) != 0)
		//printf("MemNode Not Cache Assignment addr %lx obj size %d memnode size %d\n",
		//mn, sizeof(OBJPool::Obj), sizeof(Memstore::MemNode));
		mn += sizeof(OBJPool::Obj);

		return new(mn) Memstore::MemNode();
	}

	virtual void transfer_para(RTMPara&) {
		printf("[Alex]transfer_para\n");
	}
	virtual void set_thread_num(int i) {
		thread_num = i;
	}
};
#endif
