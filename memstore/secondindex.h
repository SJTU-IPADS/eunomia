#ifndef SECONDINDEX_H_
#define SECONDINDEX_H_

#include "memstore.h"
class SecondIndex {

 public:
 	SecondIndex(){}
	virtual ~SecondIndex(){}
	
 public:
  struct MemNodeWrapper
  {
	uint64_t key;
	Memstore::MemNode *memnode;
	MemNodeWrapper *next;
	int valid;

	MemNodeWrapper()
	{
		valid = 0;
		memnode = NULL;
	}
  };
  
  struct SecondNode
  {
	uint64_t seq;
	MemNodeWrapper* head;

	SecondNode()
	{
		seq = 0;
		head = NULL;
	}
  };


  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator() {}

	virtual bool Valid() = 0;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual SecondNode* CurNode() = 0;

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


  //Only for initialization

  virtual SecondIndex::Iterator* GetIterator() = 0;

  virtual void Put(uint64_t seckey, uint64_t prikey, Memstore::MemNode* memnode){}
  virtual SecondNode* RoGet(uint64_t key) = 0;
  virtual SecondNode* Get(uint64_t key) = 0;
  
  virtual MemNodeWrapper* GetWithInsert(uint64_t seckey, uint64_t prikey, uint64_t **secseq) = 0;
  
  virtual void PrintStore() { assert(0); }
  
  virtual void ThreadLocalInit() { assert(0); }

  
  
};


#endif
