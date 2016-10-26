// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the LockfreeSkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the LockfreeSkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the LockfreeSkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...


#ifndef LOCKFREESKIPLIST_H
#define LOCKFREESKIPLIST_H


#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <malloc.h>
#include "port/port.h"
#include "port/atomic.h"
#include "util/arena.h"
#include "util/random.h"
#include "port/port_posix.h"

namespace leveldb {

class Arena;
#define LocalCursor 0


template<typename Key, class Comparator>
class LockfreeSkipList {
 public:
  struct Node;

  Node* tmp;

 public:
  // Create a new LockfreeSkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.

  explicit LockfreeSkipList(Comparator cmp, Arena* arena);
  ~LockfreeSkipList();

  static void ThreadLocalInit();
  static void ForceThreadLocalClear();
  static void GlobalClear();

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  size_t Insert(const Key& key);
  size_t FineGrainLockInsert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key);

  void PrintList() const;
  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const LockfreeSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const LockfreeSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:

  //Thread local variable for scalability purpose
  // Read/written only by Insert().
  static __thread Random* rnd_;
  static __thread bool localinit_;
  static __thread Arena* arena_;    // Arena used for allocations of nodes
  static std::vector<Arena*> arenas_collector;
  static port::SpinLock ac_lock; // protect the arenas_collector
  
  //For Profiling
  uint64_t retryCount;
  
  enum { kMaxHeight = 12 };

  // Immutable after construction
  Comparator const compare_;

  static __thread Node** curPreds_;
  static __thread Node* curNode_;
  
  Node* head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  inline uint64_t CAS64(uint64_t *p, uint64_t old, uint64_t newv)
  {
    uint64_t out;
    __asm__ __volatile__( "lock; cmpxchgq %2,%1"
                          : "=a" (out), "+m" (*p)
                          : "q" (newv), "0" (old)
                          : "cc");
    return out;
  }



  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  LockfreeSkipList(const LockfreeSkipList&);
  void operator=(const LockfreeSkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct LockfreeSkipList<Key,Comparator>::Node {

  explicit Node(const Key& k) : key(k) { }

  Key key;
  int height;
  port::SpinLock lock;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 public:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  //port::AtomicPointer next_[kMaxHeight];
  port::AtomicPointer next_[1];
};

template<typename Key, class Comparator>
typename LockfreeSkipList<Key,Comparator>::Node*
LockfreeSkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  //char* mem = arena_->AllocateAligned(sizeof(Node));
  char* mem = new char[(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1))];
//  assert((mem & (64 -1)) == 0);
  Node* n = new (mem) Node(key);
  n->height = height;
  return n;
}


template<typename Key, class Comparator>
inline LockfreeSkipList<Key,Comparator>::Iterator::Iterator(const LockfreeSkipList* list) {
  list_ = list;
  node_ = NULL;
}

template<typename Key, class Comparator>
inline bool LockfreeSkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

template<typename Key, class Comparator>
inline const Key& LockfreeSkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}



template<typename Key, class Comparator>
inline void LockfreeSkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void LockfreeSkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
inline void LockfreeSkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

template<typename Key, class Comparator>
inline void LockfreeSkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void LockfreeSkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
int LockfreeSkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_->Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template<typename Key, class Comparator>
bool LockfreeSkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

template<typename Key, class Comparator>
typename LockfreeSkipList<Key,Comparator>::Node* LockfreeSkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {

  Node* x = head_;
  int level = GetMaxHeight() - 1;

  while (true) {
    Node* next = x->Next(level);
	
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) 
	  	prev[level] = x;
	  
#if LocalCursor	
	    curPreds_[level] = x;
#endif

      if (level == 0) {
	  	curNode_ = next;
        return next;
      } else {
        // Switch to next list
       	level--;
		
#if LocalCursor	
		if(KeyIsAfterNode(key, curPreds_[level]) 
			&& KeyIsAfterNode(curPreds_[level]->key, x))
			x = curPreds_[level];
#endif	

      }
    }
  }
}

template<typename Key, class Comparator>
typename LockfreeSkipList<Key,Comparator>::Node*
LockfreeSkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
typename LockfreeSkipList<Key,Comparator>::Node* LockfreeSkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
LockfreeSkipList<Key,Comparator>::LockfreeSkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      retryCount(0),
      max_height_(reinterpret_cast<void*>(1)) {

  //The thread local arena may be not intitialized, so we use the system allocator
  head_ = reinterpret_cast<Node*>(malloc(sizeof(Node) + sizeof(port::AtomicPointer) * (kMaxHeight - 1)));
 
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);
  }
  tmp == NULL;
}


template<typename Key, class Comparator>
LockfreeSkipList<Key,Comparator>::~LockfreeSkipList()
{
  //FIXME: should not call static member clear function in the local clear construction
  //GlobalClear();
  
  delete head_;
}

template<typename Key, class Comparator>
__thread Random* LockfreeSkipList<Key,Comparator>::rnd_ = NULL;

template<typename Key, class Comparator>
__thread bool LockfreeSkipList<Key,Comparator>::localinit_ = false;

template<typename Key, class Comparator>
__thread Arena* LockfreeSkipList<Key,Comparator>::arena_ = NULL;

template<typename Key, class Comparator>
__thread typename LockfreeSkipList<Key,Comparator>::Node* 
LockfreeSkipList<Key,Comparator>::curNode_ = NULL;


template<typename Key, class Comparator>
__thread typename LockfreeSkipList<Key,Comparator>::Node** 
LockfreeSkipList<Key,Comparator>::curPreds_ = NULL;


template<typename Key, class Comparator>
std::vector<Arena*> LockfreeSkipList<Key,Comparator>::arenas_collector;

template<typename Key, class Comparator>
port::SpinLock LockfreeSkipList<Key,Comparator>::ac_lock;

template<typename Key, class Comparator>
void LockfreeSkipList<Key,Comparator>::ThreadLocalInit() {

	if(localinit_ == false) {
		rnd_ = new Random(0xdeadbeef);
		arena_ = new Arena();
		//printf("new arena_ %lx\n", arena_);
		ac_lock.Lock();
		arenas_collector.push_back(arena_);
		ac_lock.Unlock();
		localinit_ = true;
		curNode_ = NULL;
		curPreds_ = new Node*[kMaxHeight];
	}

}

template<typename Key, class Comparator>
void LockfreeSkipList<Key,Comparator>::ForceThreadLocalClear() {

	localinit_ = false;
}



template<typename Key, class Comparator>
void LockfreeSkipList<Key,Comparator>::GlobalClear() {

	for(int i = 0 ; i < arenas_collector.size(); i++) {
		//printf("delete arena %lx\n", arenas_collector[i]);
		delete arenas_collector[i];
	}
}


template<typename Key, class Comparator>
bool LockfreeSkipList<Key,Comparator>::Contains(const Key& key) {
  Node* x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}


	/*

template<typename Key, class Comparator>
bool LockfreeSkipList<Key,Comparator>::Contains(const Key& key) {

  size_t msize = 0;
  
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  int height = RandomHeight();


  //find the prevs and succs
  Node* x = FindGreaterOrEqual(key, preds);

  //assert(x == NULL || !Equal(key, x->key));

  Node* tmp = NewNode(key, height);

  msize = sizeof(Node);
  
  for (int i = 0; i < height; i++) {
		
		//We should first get succs[i] which is larger than the key
		succs[i] = preds[i]->NoBarrier_Next(i);
		while(KeyIsAfterNode(key, succs[i])) {		
			preds[i] = succs[i];
			succs[i] = preds[i]->NoBarrier_Next(i);			
		}
		
	
  }

  return tmp->key == x->key;
    
}
*/



template<typename Key, class Comparator>
void LockfreeSkipList<Key,Comparator>::PrintList() const {
	
	
	//Check every level
	printf(" Max Height %d\n", GetMaxHeight());

	Node* cur = head_;
		int count = 0;
		
		
	while(cur != NULL)
	{
		
		Key prev = cur->key;
		printf("key %ld height %ld\n", prev.k, cur->height);
		cur = cur->Next(0);
		
		count++;
	}
		
	/*
	for(int i = kMaxHeight - 1; i >= 0; i--) {	
		
		printf(" Check Layer %d\n", i);
		
		Node* cur = head_;
		int count = 0;
		
		
		while(cur != NULL)
		{
			
			Key prev = cur->key;
			cur = cur->Next(i);
			count++;
		}

		printf(" Layer %d Has %d Elements\n", i, count);
	}
	
	Iterator iter(this);
	iter.SeekToFirst();
	int count = 0;
	Key prev = NULL;
	Key min, max;
	while(iter.Valid()) {
		count++;
		prev = iter.key();
		iter.Next();
	}

	max = prev;
	printf("Last Key %lx\n", prev);
	printf("End\n Total %d elements\n", count);
	if(retryCount > 0)
		printf("End\n Retry %ld times\n", retryCount);
	/*
	//Just if the key is uint64_t
	int64_t step = (max - min)/10;
	step++;
	uint64_t keyDistribution[10][40];
	for(int i = 0; i < 10; i++)
		for(int j = 0; j < 40; j++)
			keyDistribution[i][j] = 0;
	
	iter.SeekToFirst();
	while(iter.Valid()) {
		
		Key k = iter.key();
		int index = k & 0xffff;
		assert(index < 40);
		int range = (k - min)/step;
		assert(range < 10);
		keyDistribution[range][index]++;
		
		iter.Next();
	}

	printf("Ranges\t\t");
	for(int i = 0; i < 10; i++)
		printf("[%ld -- %ld] ", min + step * i, min + step * (i + 1));
	
	printf("\n");

	for(int i = 0; i < 40; i++) {
		printf("Thread[%d] ", i);
		for(int j = 0; j < 10; j++)
			printf("[%ld] ",keyDistribution[j][i]);
		printf("\n");
	}
	*/
	
}


template<typename Key, class Comparator>
size_t LockfreeSkipList<Key,Comparator>::FineGrainLockInsert(const Key& key) {

  size_t msize = 0;
  
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  int height = RandomHeight();


  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      preds[i] = head_;
    }
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }
  

  int highestLocked = -1;

  
  while(true) {
 	//find the prevs and succs
	Node* x = FindGreaterOrEqual(key, preds);
	for (int i = 0; i < height; i++) {
		succs[i] = preds[i]->NoBarrier_Next(i);
    	while(KeyIsAfterNode(key, succs[i])) {		
			preds[i] = succs[i];
			succs[i] = preds[i]->NoBarrier_Next(i);			
		}
	}
	assert(x == NULL || !Equal(key, x->key));

	//lock every prevs to insert the new node
	Node *pred, *succ;
	bool valid = true;
	Node *prevpred = NULL;
	
	for (int i = 0; i < height && valid; i++) {
		pred = preds[i];
		succ = succs[i];
		if(pred != prevpred) {
			pred->lock.Lock();
			highestLocked = i;
			prevpred = pred;
		}
		//check if its succ has been modified
		valid = (succ == pred->Next(i));			
	}

	if(!valid)
	{	
		prevpred = NULL;
		for (int i = 0; i < highestLocked + 1; i++) {
			pred = preds[i];
			succ = succs[i];
			if(pred != prevpred) {
				pred->lock.Unlock();
				prevpred = pred;
			}
		}
		
		continue;

	}

	x = NewNode(key, height);
	
 	for (int i = 0; i < height; i++) {
    	x->NoBarrier_SetNext(i, preds[i]->NoBarrier_Next(i));
    	preds[i]->SetNext(i, x);
  	}

	prevpred = NULL;
	for (int i = 0; i < height; i++) {
		pred = preds[i];
		succ = succs[i];
		if(pred != prevpred) {
			pred->lock.Unlock();
			prevpred = pred;
		}
	}

	return true;
	
    	
 }

}


template<typename Key, class Comparator>
size_t LockfreeSkipList<Key,Comparator>::Insert(const Key& key) {


  size_t msize = 0;
  
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  int height = RandomHeight();

  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      preds[i] = head_;
    }
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }


  //find the prevs and succs
  Node* x = FindGreaterOrEqual(key, preds);

  if(x != NULL && Equal(key, x->key))
	return 0;

  
  assert(x == NULL || !Equal(key, x->key));

//  printf("put key %ld height %ld \n", key.k, height);
  x = NewNode(key, height);

  
  msize = sizeof(Node);

  uint64_t retry = 0;
  
  for (int i = 0; i < height; i++) {
	Node *succ = NULL;

	while(true) {
		
		//We should first get succs[i] which is larger than the key
		succs[i] = preds[i]->NoBarrier_Next(i);
    	while(KeyIsAfterNode(key, succs[i])) {
			
			preds[i] = succs[i];
			succs[i] = preds[i]->NoBarrier_Next(i);			
		}
		
		if((succs[i]!= NULL) && Equal(key, succs[i]->key))
			return 0;
		
		x->NoBarrier_SetNext(i, preds[i]->NoBarrier_Next(i));
		
		succ = (Node*)CAS64((uint64_t*)&preds[i]->next_[i].rep_, (uint64_t)succs[i], (uint64_t)x);

		if(succ == succs[i])
		{	
	//		assert(compare_(preds[i]->key, key) < 0);
	//		assert(succs[i] == NULL || compare_(key, succs[i]->key) < 0);
			//printf("Insert %dth Node %lx Addr %lx Height %d\n", gcount, x->key, x, height);
	//		if(tmp != NULL)
		//		printf("     %dth Node %lx Addr %lx Height %d\n", 41, tmp->key, tmp, height);
			
			/*
			if(succs[i] != NULL)
				printf("%lx ---> %lx ---> %lx\n", preds[i]->key, key, succs[i]->key);
			else
				printf("%lx ---> %lx ---> NULL\n", preds[i]->key, key);
			
			
			if(succs[i] != NULL && preds[i]->key != NULL && succs[i]->key != NULL)
				printf("%ld ---> %ld ---> %ld\n", DecodeFixed32(preds[i]->key), 
				DecodeFixed32(key), DecodeFixed32(succs[i]->key));
			else if(succs[i] == NULL && preds[i]->key != NULL)
				printf("%ld ---> %ld ---> NULL \n", DecodeFixed64(preds[i]->key), 
				DecodeFixed64(key));
			else if(succs[i] == NULL && preds[i]->key == NULL)
				printf("Head ---> %ld ---> NULL \n", DecodeFixed64(key));
			else if(succs[i] != NULL && preds[i]->key == NULL)
				printf("Head  ---> %ld ---> %ld \n",
				DecodeFixed64(key), DecodeFixed64(succs[i]->key));
			*/
			break;
		}
	//	retry++;
		//printf("Retry %d\n", retry);
	 }
	
  }

  //atomic_add64(&retryCount, retry);

  return msize;
    
}


}  // namespace leveldb

#endif
