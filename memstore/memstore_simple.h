#ifndef MEMSTORESIMPLE_H_
#define MEMSTORESIMPLE_H_
#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include "memstore.h"


class SimpleMemstore: public Memstore {

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

  Memstore::MemNode* simplestore;
  SimpleMemstore();
  ~SimpleMemstore();

	
  //Only for initialization

  virtual Memstore::Iterator* GetIterator(){return NULL;};
  
  virtual MemNode* Put(uint64_t k, uint64_t* val);

  virtual MemNode* Get(uint64_t key);
  
  virtual InsertResult GetWithInsert(uint64_t key);
  virtual MemNode* GetForRead(uint64_t key);
  
  virtual MemNode* GetWithDelete(uint64_t key) {assert(0);}
  virtual void PrintStore() { assert(0); }
  
  virtual void ThreadLocalInit() { assert(0); }
  
  
};


#endif
