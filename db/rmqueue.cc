#include "port/atomic.h"
#include "rmqueue.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "util/rtm.h"
#include "db/dbtx.h"

namespace leveldb {

RTMProfile *RMQueue::rtmProf = NULL;

RMQueue::RMQueue(DBTables *st)
{
	//the default size of the gc queue is 64
	qsize = 1024;
	head = tail = 0;
	queue = new RMArray*[qsize];
	need_del = 0;
	actual_del = 0;
	elems = 0;
	store = st;
	if(rtmProf == NULL)
		rtmProf = new RTMProfile();
}
	
RMQueue::~RMQueue()
{

	printf("RMQueue\n");
	while(head != tail) {
		delete queue[head];
		head = (head + 1) % qsize;
	}

	delete[] queue;

}

void RMQueue::AddRMArray(Epoch* e, uint64_t** arr, int len)
{
	//the queue is empty
	elems++;
	queue[tail] = new RMArray(e, arr, len);
	
	if((tail + 1) % qsize == head) {
		printf("ERROR: RMQueue Over Flow %d\n", elems);
		printf("Cur \n");
		e->Print();
		printf("Queue \n");
		Print();
		exit(1);
	}
	tail = (tail + 1) % qsize;
	assert(tail != head);
	
#if RMTEST
	need_del++;
#endif	
}

void RMQueue::Remove(Epoch* current)
{	
	while(head != tail && queue[head]->epoch->Compare(current) < 0) {
		
		//remove the nodes from the data structure
		for(int i = 0; i < queue[head]->len; i++) {
			
			RMElement* mn = (RMElement*)queue[head]->rmarray[i];
			
			{
#if GLOBALOCK
  				SpinLockScope spinlock(&DBTX::slock);
#else
				RTMScope rtm(rtmProf);
#endif
				//printf("RMQueue Remove %lx node %lx\n", mn->node);
				//Check if this node has been modified
				//printf("%ld %ld\n",mn->node->seq,mn->seq);
				if(mn->node->value == (uint64_t *)1 && mn->node->seq == mn->seq) {

				//	printf("[%ld] remove node %lx seq %ld\n", 
				//		pthread_self(), mn->node, mn->node->seq);
					
					//Physically removed
					mn->node->value = (uint64_t *)2;
					Memstore::MemNode* n = store->tables[mn->tableid]->GetWithDelete(mn->key);
					n->seq++;
					assert(n == mn->node);
				}
			
			}

			
		}
		
		head = (head + 1) % qsize;
		elems--;
#if RMTEST
		actual_del++;
#endif
	}
	
}

void RMQueue::Print()
{
	int index = head;
	while(index != tail) {
		
		queue[index]->epoch->Print();
		
		index = (index + 1) % qsize;
	}
}

}
