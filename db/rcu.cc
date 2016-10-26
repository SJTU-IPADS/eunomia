#include "rcu.h"
#include <assert.h>
#include <stdio.h>
#include <pthread.h>
#include "port/atomic.h"

__thread int RCU::tid = 0;

RCU::RCU(int thrs)
{
	thrs_num = thrs;
	states = new State[thrs_num];
	sync = false;
}
	
RCU::~RCU()
{
	delete[] states;
}

void RCU::RegisterThread(int i)
{
	tid = i;
}
	
void RCU::WaitForGracePeriod()
{
	uint64_t * cur = GetStatesCopy();
	
	for(int i = 0; i < thrs_num; i++) {
		
		if(i == tid)
			continue;

		int w = 0;
		while (!states[i].Safe(cur[i])) {
			w++;
			cpu_relax();
			if(thrs_num > 8) {

				struct timespec t;
				t.tv_sec  = 0;
		     	t.tv_nsec = 5;

				if(w > 128 & w <= 1024) {
					pthread_yield();
				}
				else if(w > 1024) {
					nanosleep(&t, NULL);
					w = 0;
				}
			}
		}
		
	}
}

void RCU::BeginTX()
{
	states[tid].BeginTX();
}

void RCU::EndTX()
{
	states[tid].EndTX();
}

void RCU::Print()
{
	for(int i = 0; i < thrs_num; i++)
		printf("Core [%d] %ld ", i, states[i].counter);
	printf("\n");
}

uint64_t* RCU::GetStatesCopy()
{
	uint64_t * cs = new uint64_t[thrs_num];

	for(int i = 0; i < thrs_num; i++)
		cs[i] = states[i].counter & BEGMASK;

	return cs;
}
