#ifndef NODEBUF_H
#define NODEBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "gcqueue.h"
#include "memstore/memstore.h"

class NodeBuf {

private:
	int qsize;
	int head;
	int tail;
	GCElement** buffer;
	int elems;
	//For profiling

	int hit;
	int miss;
	
public:

	
	NodeBuf();
	
	~NodeBuf();
	
	void AddGCElement(GCElement* gce);

	Memstore::MemNode* GetMemNode();

	void Print();
	
};


#endif
