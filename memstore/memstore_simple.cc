#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include "memstore_simple.h"

#define STORESIZE 4096

SimpleMemstore::SimpleMemstore()
{
	simplestore = new MemNode[STORESIZE];
}

SimpleMemstore::~SimpleMemstore()
{
	delete[] simplestore;
}



Memstore::MemNode* SimpleMemstore::Put(uint64_t k, uint64_t* val)
{
	assert(k < STORESIZE);
	simplestore[k].value = val;
	return &simplestore[k];
}

Memstore::MemNode* SimpleMemstore::Get(uint64_t k)
{
	assert(k < STORESIZE);
	return &simplestore[k];
}


Memstore::InsertResult SimpleMemstore::GetWithInsert(uint64_t k)
{
	assert(k < STORESIZE);
	return {&simplestore[k],false};
}

Memstore::MemNode* SimpleMemstore::GetForRead(uint64_t k)
{
	assert(k < STORESIZE);
	return &simplestore[k];
}



