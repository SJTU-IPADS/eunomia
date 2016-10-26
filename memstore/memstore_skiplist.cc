#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <malloc.h>
#include "port/port.h"
#include "port/atomic.h"
#include "util/arena.h"
#include "util/random.h"
#include "util/rtm.h"
#include "util/mutexlock.h"
#include "db/dbtx.h"
#include "port/port_posix.h"
#include "memstore_skiplist.h"


#define SKIPLISTGLOBALLOCK 0
#define SKIPLISTRTM 0
#define SKIPLISTLOCKFREE 0
#define NODEPROFILE 0
#define SKIPLISTFINEGRAINLOCK 1
namespace leveldb {


__thread Random* MemStoreSkipList::rnd_;
__thread bool MemStoreSkipList::localinit_ = false;
__thread MemStoreSkipList::Node* MemStoreSkipList::dummy_ = NULL;

inline void MemoryBarrier() {
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  __asm__ __volatile__("" : : : "memory");
}

MemStoreSkipList::MemStoreSkipList()
{
	printf("MemStoreSkipList\n");
    ThreadLocalInit();
	max_height_ = 1;

	head_ = reinterpret_cast<Node*>(malloc(sizeof(Node) + sizeof(void *) * (kMaxHeight - 1)));
 
  	for (int i = 0; i < kMaxHeight; i++) {
    	head_->next_[i] = NULL;
  	}

	snapshot = 1;

#if NODEPROFILE
	tcount = 0;
	ocount = 0;
	nnum = 0;
#endif
}

MemStoreSkipList::~MemStoreSkipList()
{
#if NODEPROFILE
	printf("total touched %ld\n", tcount);
	printf("other table touched %ld\n", ocount);
	printf("total node number to found %ld\n", nnum);
#endif
}



void MemStoreSkipList::ThreadLocalInit()
{
	if(localinit_ == false) {
		rnd_ = new Random(0xdeadbeef);
		localinit_ = true;
		int height = RandomHeight();

 	 	dummy_ = NewNode(0, height);
	}

}


MemStoreSkipList::Node* MemStoreSkipList::NewNode(uint64_t key, int height)
{
 
  Node* n = reinterpret_cast<Node*>(malloc(
	  			sizeof(Node) + sizeof(void *) * (height - 1)));

  n->key = key;
  n->height = height;
  n->memVal.counter = 0;
  n->memVal.seq = 0;
  n->memVal.value = NULL;
  n->memVal.oldVersions = NULL;
  n->next_[0] = NULL;
  return n;
}


inline void MemStoreSkipList::FreeNode(Node* n)
{
  free(n); 
}



MemStoreSkipList::Iterator::Iterator(MemStoreSkipList* list)
{
	list_ = list;
	node_ = NULL;
	prev_ = NULL;
}

uint64_t* MemStoreSkipList::Iterator::GetLink()
{
	if(prev_ == NULL)
		return NULL;
	else
		return (uint64_t*)(&prev_->next_[0]);
}

uint64_t MemStoreSkipList::Iterator::GetLinkTarget()
{
	return (uint64_t)node_;
}


// Returns true iff the iterator is positioned at a valid node.
bool MemStoreSkipList::Iterator::Valid()
{
	return node_ != NULL;
}

// Advances to the next position.
// REQUIRES: Valid()
bool MemStoreSkipList::Iterator::Next()
{
	//get next different key
	prev_ = node_;
	node_ = node_->next_[0];
	return true;
}

// Advances to the previous position.
// REQUIRES: Valid()
bool MemStoreSkipList::Iterator::Prev()
{
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());

  //FIXME: This function doesn't support link information
  
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
	node_ = NULL;
  }
  return true;
}

uint64_t MemStoreSkipList::Iterator::Key()
{
	return node_->key;
}

Memstore::MemNode* MemStoreSkipList::Iterator::CurNode()
{
	return (Memstore::MemNode*)&node_->memVal;
}

// Advance to the first entry with a key >= target
void MemStoreSkipList::Iterator::Seek(uint64_t key)
{
	prev_ = list_->FindLessThan(key);
	/*node_ = prev_->next_[0];
	while(node_->key < key) {
		prev_ = node_;
		node_ = prev_->next_[0];
	}*/
	node_ = list_->FindGreaterOrEqual(key, NULL);
}

void MemStoreSkipList::Iterator::SeekPrev(uint64_t key)
{
	node_ = list_->FindLessThan(key);
	if(node_ == NULL)
		node_ = list_->head_;
	else
		prev_ = list_->FindLessThan(node_->key);
}


// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void MemStoreSkipList::Iterator::SeekToFirst()
{
	prev_ = list_->head_;
	node_ = list_->head_->next_[0];
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void MemStoreSkipList::Iterator::SeekToLast()
{
	//TODO
	assert(0);
}
	

inline uint32_t MemStoreSkipList::RandomHeight() 
{
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  uint32_t height = 1;
  while (height < kMaxHeight && ((rnd_->Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

inline MemStoreSkipList::Node* MemStoreSkipList::FindLessThan(uint64_t key)
{
  Node* x = head_;
  int level = max_height_ - 1;
  while (true) {
    assert(x == head_ || x->key < key);
    Node* next = x->next_[level];
    if (next == NULL || next->key >= key) {
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


inline MemStoreSkipList::Node* MemStoreSkipList::FindGreaterOrEqualProfile(uint64_t key, Node** prev)
{

  Node* x = head_;
  int level = max_height_ - 1;

  atomic_inc64(&nnum);
  
  while (true) {
    Node* next = x->next_[level];

	atomic_inc64(&tcount);
	
	if(next != NULL && (key>>50) != (next->key>>50))
		atomic_inc64(&ocount);
	
    if (next != NULL && key > next->key) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) 
	  	prev[level] = x;
	  
      if (level == 0) {
        return next;
      } else {
       	level--;
      }
    }
  }
}

inline MemStoreSkipList::Node* MemStoreSkipList::FindGreaterOrEqual(uint64_t key, Node** prev)
{

  Node* x = head_;
  int level = max_height_ - 1;

  while (true) {
    Node* next = x->next_[level];
	
    if (next != NULL && key > next->key) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) 
	  	prev[level] = x;
	  
      if (level == 0) {
        return next;
      } else {
       	level--;
      }
    }
  }
}
inline MemStoreSkipList::Node* MemStoreSkipList::FindGreaterOrEqual(uint64_t key, Node** prev, Node** succs)
{
  Node* x = head_;
  int level = kMaxHeight - 1;
  while (true) {
  	MemoryBarrier();
    Node* next = x->next_[level];
    if (next != NULL && key > next->key) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
	  if (succs != NULL) succs[level] = next;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}


Memstore::MemNode* MemStoreSkipList::Put(uint64_t k,uint64_t * val)
{
	
	Memstore::MemNode* n = GetWithInsert(k).node;
	n->value = val;
	n->seq = 1;

	return n;
}
 

Memstore::MemNode* MemStoreSkipList::Get(uint64_t key)
{
	Node* x = FindGreaterOrEqual(key, NULL);

  	if(x != NULL && key == x->key ) {
		
		return (Memstore::MemNode*)&x->memVal;
		
  	}

	return NULL;
}

Memstore::InsertResult  MemStoreSkipList::GetWithInsert(uint64_t key) 
{
	ThreadLocalInit();
#if SKIPLISTLOCKFREE
	Memstore::MemNode* x = GetWithInsertLockFree(key);
#elif SKIPLISTRTM
	Memstore::MemNode* x = GetWithInsertRTM(key);
#elif SKIPLISTFINEGRAINLOCK
	Memstore::MemNode* x = GetWithInsertFineGrainLock(key);
#endif

	if(dummy_ == NULL) {
		 int height = RandomHeight();
  		 dummy_ = NewNode(key, height);
	}

	return {x,false};
}
Memstore::MemNode*  MemStoreSkipList::GetForRead(uint64_t key) 
{
	ThreadLocalInit();
#if SKIPLISTLOCKFREE
	Memstore::MemNode* x = GetWithInsertLockFree(key);
#elif SKIPLISTRTM
	Memstore::MemNode* x = GetWithInsertRTM(key);
#elif SKIPLISTFINEGRAINLOCK
	Memstore::MemNode* x = GetWithInsertFineGrainLock(key);
#endif

	if(dummy_ == NULL) {
		 int height = RandomHeight();
  		 dummy_ = NewNode(key, height);
	}

	return x;
}

Memstore::MemNode* MemStoreSkipList::GetWithInsertFineGrainLock(uint64_t key)
{
	Node* preds[kMaxHeight];
	Node* succs[kMaxHeight];
	int height = RandomHeight();
	
	int highestLocked = -1;

	while(true) {
		//find the prevs and succs
		Node* x = FindGreaterOrEqual(key, preds, succs);
	
		if(x!= NULL && x->key == key)
			return (Memstore::MemNode*)&x->memVal;

		
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
			MemoryBarrier();
			valid = (succ == pred->next_[i]);			
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
			 x->next_[i] = preds[i]->next_[i];
			 MemoryBarrier();
			 preds[i]->next_[i] =  x;
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
		 
		 return (Memstore::MemNode*)&x->memVal;
	}
}


Memstore::MemNode*  MemStoreSkipList::GetWithInsertRTM(uint64_t key)
{

  
  Node* preds[kMaxHeight];
 
  Node* x;

  {

	#if SKIPLISTGLOBALLOCK
		//MutexLock lock(&DBTX::storemutex);
		DBTX::slock.Lock();
	#elif SKIPLISTRTM
		RTMScope rtm(NULL, 15, 15, NULL);
	#endif

    //find the prevs and succs
    x = FindGreaterOrEqual(key, preds);
  
    if(x != NULL && key == x->key ) {

#if SKIPLISTGLOBALLOCK
			  //MutexLock lock(&DBTX::storemutex);
			  DBTX::slock.Unlock();
#endif

  	  goto found;
    }

	 x = dummy_;
	 dummy_ = NULL;

	 x->key = key;
	 
  	 int height = x->height;
	 
     if (height > max_height_){
      for (int i = max_height_; i < height; i++) {
        preds[i] = head_;
      }
      max_height_ = height;
    }
    	
    
    for (int i = 0; i < height; i++) {
  		x->next_[i] = preds[i]->next_[i];
  		preds[i]->next_[i] = x;	
    }

	
	
#if SKIPLISTGLOBALLOCK
				  //MutexLock lock(&DBTX::storemutex);
	DBTX::slock.Unlock();
#endif

    return (Memstore::MemNode*)&x->memVal;
	
  }

found:
  
  return (Memstore::MemNode*)&x->memVal;
}

Memstore::MemNode* MemStoreSkipList::GetWithInsertLockFree(uint64_t key)
{

  
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  Node* x;

  //find the prevs and succs
 #if NODEPROFILE
  x = FindGreaterOrEqualProfile(key, preds);
 #else
  x = FindGreaterOrEqual(key, preds);
 #endif

  if(x != NULL && key == x->key ) {
	//FIXME: Only search the node at level 1
//	while(x->next_[0] != NULL && x->next_[0]->key == key)
	//	x = x->next_[0];

	return (Memstore::MemNode*)(&x->memVal);
  }

  
  int height = RandomHeight();
	
	uint32_t maxh = 0; 
  
	//We need to update the max_height_ atomically
	while (true) {
	  
	  maxh = max_height_;
	  
	  if (height > maxh) {
	  
		uint32_t oldv = atomic_cmpxchg32((uint32_t*)&max_height_, maxh, height);
		if(oldv == maxh)
		  break;
		
  
	  } else
	  {
		break; 
	  }
	}
	
	for (int i = maxh; i < height; i++) {
		preds[i] = head_;
	}

//	printf("put key %ld height %ld \n", key.k, height);
  x = NewNode(key, height);
  
  for (int i = 0; i < height; i++) {
	Node *succ = NULL;

	while(true) {
		
		//We should first get succs[i] which is larger than the key
		succs[i] = preds[i]->next_[i];
		while(succs[i] != NULL && key > succs[i]->key) {
			preds[i] = succs[i];
			succs[i] = preds[i]->next_[i]; 		
		}
		
		if((succs[i]!= NULL) && key == succs[i]->key) {
			assert( i == 0);
			FreeNode(x);
			return (Memstore::MemNode*)&succs[i]->memVal;
		}
		
		x->next_[i] = preds[i]->next_[i];
		
		succ = (Node*)atomic_cmpxchg64((uint64_t*)&preds[i]->next_[i], (uint64_t)succs[i], (uint64_t)x);

		if(succ == succs[i])
		{	
			break;
		}
	//	retry++;
		//printf("Retry %d\n", retry);
	 }
	
  }
  //atomic_add64(&retryCount, retry);

  return (Memstore::MemNode*)(&x->memVal);
	
}



Memstore::Iterator* MemStoreSkipList::GetIterator()
{
	return new MemStoreSkipList::Iterator(this);
}

void MemStoreSkipList::PrintStore(){

	printf("------Store-------\n");
	
	Node* cur = head_;
		
	while(cur != NULL)
	{
		
		//Key prev = cur->key;
		cur = cur->next_[0];
		if(cur != NULL) {
			printf("key %lu value addr %p\n", 	cur->key, &cur->memVal);

		}
	}
		
	/*

	

	printf(" Max Height %d\n", max_height_);

	Node* cur = head_;
	int count = 0;	
		
	for(int i = kMaxHeight - 1; i >= 0; i--) {	
		
		//printf(" Check Layer %d\n", i);
		
		Node* cur = head_;
		int count = 0;
		
		
		while(cur != NULL)
		{
			
			//Key prev = cur->key;
			cur = cur->next_[i];
			count++;

			//if( i == 0 && cur != NULL)
				//printf("key %lx\n", cur->key);
		}

		printf(" Layer %d Has %d Elements\n", i, count);
	}
*/
}


}
