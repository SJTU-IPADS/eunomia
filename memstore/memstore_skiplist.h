#ifndef MEMSTORESKIPLIST_H
#define MEMSTORESKIPLIST_H


#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <malloc.h>
#include "port/port.h"
#include "port/port_posix.h"
#include "port/atomic.h"
#include "util/arena.h"
#include "util/spinlock.h"

#include "util/random.h"
#include "port/port_posix.h"
#include "memstore/memstore.h"

namespace leveldb {

class MemStoreSkipList: public Memstore {

 public:

  uint64_t snapshot; // the counter for current snapshot
  
  struct Node
  {
	uint64_t key;
	int height;
	MemNode memVal;
	SpinLock lock;	
	Node* next_[1];
  };


  class Iterator: public Memstore::Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(){};
    Iterator(MemStoreSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();

    // Returns the key at the current position.
    // REQUIRES: Valid()
    MemNode* CurNode();

	
	uint64_t Key();

    // Advances to the next position.
    // REQUIRES: Valid()
    bool Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    bool Prev();

    // Advance to the first entry with a key >= target
    void Seek(uint64_t key);

	void SeekPrev(uint64_t key);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

	uint64_t* GetLink();

	uint64_t GetLinkTarget();

   private:
    MemStoreSkipList* list_;
    Node* node_;
	Node* prev_;
	uint64_t snapshot_;
    // Intentionally copyable
  };

 public:

  explicit MemStoreSkipList();
  ~MemStoreSkipList();

  //Only for initialization
  MemNode* Put(uint64_t k, uint64_t* val);
  MemNode* GetWithInsertFineGrainLock(uint64_t key);
  MemNode* GetWithInsertLockFree(uint64_t key);
  MemNode* GetWithInsertRTM(uint64_t key);

  InsertResult GetWithInsert(uint64_t key);
  MemNode* GetForRead(uint64_t key);
  MemNode* Get(uint64_t key);

  Memstore::Iterator* GetIterator();
  
  void PrintStore();

  static Node* NewNode(uint64_t key, int height);
 
  inline void FreeNode(Node* n);
  
  inline uint32_t RandomHeight();

   
  inline Node* FindGreaterOrEqual(uint64_t key, Node** prev,Node** succs );
  inline Node* FindGreaterOrEqual(uint64_t key, Node** prev);

  inline Node* FindGreaterOrEqualProfile(uint64_t key, Node** prev);

  inline Node* FindLessThan(uint64_t key);
  
  inline Node* GetHead(){return head_;}
  
  void ThreadLocalInit();
  
  private:
  	
  	char padding1[64];
  	enum { kMaxHeight = 16 };
	uint32_t max_height_;
	char padding2[64];
	Node* head_;
	char padding3[64];
	
	uint64_t tcount;
	uint64_t ocount;
	uint64_t nnum;
	
	static __thread Random* rnd_;
	static __thread Node* dummy_;
	static __thread bool localinit_;
  
};

}  // namespace leveldb

#endif
