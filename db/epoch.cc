#include "port/atomic.h"
#include "epoch.h"
#include <assert.h>
#include <stdio.h>

#define ENDMASK ((uint64_t)1<<63)
#define BEGMASK ~((uint64_t)1<<63)

__thread int Epoch::tid = 0;

Epoch::Epoch(int thrs) {

	thrs_num = thrs;
	counters = new uint64_t[thrs];
	for(int i = 0; i < thrs_num; i++) {
		counters[i] = ENDMASK | 0;
		//printf("%lx %ld\n", counters[i], counters[i]);
	}
}

Epoch::Epoch(int thrs, uint64_t* cs) {
	thrs_num = thrs;
	counters = cs;
}

Epoch::~Epoch() {

	delete[] counters;
	
}

void Epoch::setTID(int i) {
	assert(tid < thrs_num);
	tid = i;	
}

Epoch* Epoch::getCurrentEpoch() {

	//printf("thrs_num %d\n", thrs_num);
	assert(thrs_num < 100);
	uint64_t * cs = new uint64_t[thrs_num];

	for(int i = 0; i < thrs_num; i++)
		cs[i] = (counters[i] & BEGMASK);
	
	return new Epoch(thrs_num, cs);
}


void Epoch::beginTX()
{
	counters[tid]++;
	counters[tid] &= BEGMASK;
}

void Epoch::endTX()
{
	counters[tid] |= ENDMASK;
}

//Compare with another epoch: 1: this > e  0: this == e   < 0: this < e
int Epoch::Compare(Epoch* e)
{
	int res  = 0;
	int tmp = 0;
	for(int i = 0; i < thrs_num; i++) {		
		 if (counters[i] == e->counters[i]) {
		
			return 0;
			
		} else if (counters[i] < e->counters[i]) {
			
			res++;
			
		} 

	}
	
	if(res == thrs_num) {
		return -1;
	} else if(res == 0) {
		return 1;
	}
	assert(0);
}


void Epoch::Print()
{
	printf("Epoch[%d] ", tid);
	for(int i = 0; i < thrs_num; i++) {
		printf("[%d]: %ld ", i, counters[i]);
	}
	printf("\n");
}

