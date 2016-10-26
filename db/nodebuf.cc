#include "nodebuf.h"

NodeBuf::NodeBuf()
{
	//the default size of the gc queue is 64
	qsize = 1024;
	head = tail = 0;
	buffer = new GCElement*[qsize];
	elems = 0;

	hit = 0;
	miss = 0;
}

NodeBuf::~NodeBuf()
{
	while(head != tail) {
		delete buffer[head];
		head = (head + 1) % qsize;
	}

	delete[] buffer;
}


void NodeBuf::AddGCElement(GCElement* gce)
{
	//the queue is empty
	elems++;

	//If the buffer is full just free the elements
	if((tail + 1) % qsize == head) {
		delete gce;
		return;
	}
	
	buffer[tail] = gce;
	tail = (tail + 1) % qsize;
	assert(tail != head); 
}


Memstore::MemNode* NodeBuf::GetMemNode()
{	

	//if the buffer is empy, just allocate a new memory node
	if(head == tail) {

		return Memstore::GetMemNode();
	}


	
	int last = (tail - 1) % qsize;
	GCElement* gce = buffer[last];
	
	assert(gce->len >= 1);
	Memstore::MemNode*  res = (Memstore::MemNode*)gce->gcarray[gce->len - 1];

	gce->len--;
	
	if(gce->len == 0) {
		delete gce;
		tail = last;	
	}

	res->seq = 0;
	res->value = 0;
	res->counter = 0;
	res->oldVersions = NULL;
	
	return res;
}


void NodeBuf::Print()
{
	printf("Node Buffer Hit %d Miss %d\n", hit, miss);
}

