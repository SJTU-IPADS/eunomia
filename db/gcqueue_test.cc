#include "port/atomic.h"
#include "gcqueue.h"
#include <assert.h>

GCQueue::GCQueue()
{
	//the default size of the gc queue is 64
	qsize = 64;
	head = tail = 0;
	queue = new GCElement*[qsize];
}
	
GCQueue::~GCQueue()
{
	while(head != tail) {
		delete queue[head];
		head = (head + 1) % qsize;
	}

	delete[] queue;
}

void GCQueue::AddGCElement(Epoch* e, uint64_t** arr, int len)
{
	//the queue is empty
	queue[tail] = new GCElement(e, arr, len);
	tail += (tail + 1) % qsize;
	assert(tail != head);
	if(tail == head) {
		printf("ERROR: GCQUEUE Over Flow \n");
		exit(1);
	}
}

void GCQueue::GC(Epoch* current)
{
	while(head != tail && queue[head]->epoch->Compare(current) < 0) {
		delete queue[head];
		head = (head + 1) % qsize;
	}
}