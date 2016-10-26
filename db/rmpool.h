#ifndef RMPool_H
#define RMPool_H

#include "db/epoch.h"
#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "memstore/memstore.h"
#include "db/dbtables.h"

namespace leveldb {
class DBTables;

class RMPool {

public:

static RTMProfile *rtmProf;

struct RMObj {

	int tableid;
	uint64_t key;
	Memstore::MemNode* node;
	uint64_t seq;

	RMObj* next;
	
	RMObj(int t, uint64_t k, Memstore::MemNode* mn, uint64_t sequence) 
	{
		tableid = t;
		key = k;
		node = mn;
		seq = sequence;
	}

	~RMObj() 
	{
		//TODO
	}
};


private:
	RMObj* rmlist_;
	int elems;
	DBTables *store;
	
public:
	RMPool(DBTables *st);
	
	~RMPool();
	
	void AddRMObj(int tableid, uint64_t key, uint64_t seq, Memstore::MemNode* node);

	void RemoveAll();

	bool Remove(RMObj* o);

	int GCElems();
	void Print();
	
};

}
#endif
