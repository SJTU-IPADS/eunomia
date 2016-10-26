#include "port/atomic.h"
#include "gcqueue.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "nodebuf.h"

GCQueue::GCQueue()
{
	//the default size of the gc queue is 64
	qsize = 1024;
	head = tail = 0;
	queue = new GCElement*[qsize];
	need_del = 0;
	actual_del = 0;
	elems = 0;
	
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
	elems++;
	queue[tail] = new GCElement(e, arr, len);
	
	if((tail + 1) % qsize == head) {
		printf("ERROR: GCQUEUE Over Flow %d\n", elems);
		//printf("Cur \n");
		//e->Print();
		//printf("Queue \n");
		//Print();
		exit(1);
	}
	tail = (tail + 1) % qsize;
	assert(tail != head);
	
#if GCTEST
	need_del++;
#endif	
}

void GCQueue::GC(Epoch* current)
{	
	while(head != tail && queue[head]->epoch->Compare(current) < 0) {
		delete queue[head];
		head = (head + 1) % qsize;
		elems--;
#if GCTEST
		actual_del++;
#endif
	}
	
}

void GCQueue::GC(Epoch* current, NodeBuf* buf)
{	
	while(head != tail && queue[head]->epoch->Compare(current) < 0) {
		buf->AddGCElement(queue[head]);
		head = (head + 1) % qsize;
		elems--;
#if GCTEST
		actual_del++;
#endif
	}
	
}

void GCQueue::Print()
{
	int index = head;
	while(index != tail) {
		
		queue[index]->epoch->Print();
		
		index = (index + 1) % qsize;
	}
}
