#include "tbuf.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <sys/uio.h>
#include <limits.h>

volatile bool TBuf::sync_ = false;

TBuf::TBuf(int thr, char* lpath)
{

	buflen = thr;

	flushbufs = new LocalPBuf*[thr];
	
	frozenbufs = new LocalPBuf*[thr];
	
	for(int i = 0; i < thr; i++)
		frozenbufs[i] = NULL;
	
		frozennum = 0;
	

	freebufs = NULL;
		
	logpath = lpath;
	logf = new Log(logpath, true);
	
	localsn = new uint64_t[thr];
	for(int i = 0; i < thr; i++)
		localsn[i] = 0;
	
	safe_sn = 0;
	
	//Create Serialization Thread
	pthread_create(&write_id, NULL, loggerThread, (void *)this);
}

TBuf::~TBuf()
{
	
}


void TBuf::Sync()
{
	sync_ = true;

}

void TBuf::WaitSyncFinish()
{
	pthread_join(write_id, NULL);
}


void TBuf::PublishLocalBuffer(int tid, LocalPBuf* lbuf)
{
	frozenlock.Lock();
	lbuf->next = frozenbufs[tid];
	frozenbufs[tid] = lbuf;
	frozennum++;
	frozenlock.Unlock();

}

LocalPBuf* TBuf::GetFreeBuf()
{
	
	freelock.Lock();
	LocalPBuf* lbuf = freebufs;
	if(lbuf != NULL) 
		freebufs = freebufs->next;
	freelock.Unlock();
	
	if(lbuf != NULL) 
		lbuf->Reset();
	
	return lbuf;

}


void* TBuf::loggerThread(void * arg)
{
#if 0	
	   cpu_set_t  mask;
	  CPU_ZERO(&mask);
	  CPU_SET(7 , &mask);
	sched_setaffinity(0, sizeof(mask), &mask);
#endif

	TBuf* tb = (TBuf*) arg;
	uint64_t res = 0;
	while(true) {

		if(sync_) {
	//		uint64_t ss = rdtsc();
			res += tb->Writer();
	//		write_time += (rdtsc()-ss);
			printf("Total Write %ld Bytes\n", res);
			break;
		}
#if 1		  	
		struct timespec t;
		t.tv_sec  = 0;
		t.tv_nsec = 1000000;
		nanosleep(&t, NULL);
#endif		
		res += tb->Writer();
	
	}
}

int TBuf::Writer()
{
	int bytes = 0;
	
	int num = 0;
	
	frozenlock.Lock();
	
	for(int i = 0; i < buflen; i++) {
		if(frozenbufs[i] == NULL)
			continue;
		
		flushbufs[i] = frozenbufs[i];
		localsn[i] = flushbufs[i]->GetSN();
		frozenbufs[i] = NULL;
	}
	
	num = frozennum;
	
	frozennum = 0;
	frozenlock.Unlock();	

	//init iovec
	struct iovec iovs[IOV_MAX];
	//memset(iovs, 0, num * sizeof(struct iovec));
	
	int idx = 0;

	LocalPBuf* lsbufs = NULL;
	LocalPBuf* tail = NULL;

	for(int i = 0; i < buflen; i++) {

		if(flushbufs[i] == NULL)
			continue;
			

		LocalPBuf* cur = flushbufs[i];
		flushbufs[i] = NULL;
		
		if(lsbufs == NULL)
			lsbufs = cur;

		if(tail != NULL)
			tail->next = cur;
		
		tail = cur;

		uint64_t cursn = cur->GetSN();
		
		while(cur != NULL) {

			assert(cursn >= cur->GetSN());
			assert(idx < num);
			
			if(cur->cur > 0) {	
				iovs[idx].iov_base = cur->buf;
				iovs[idx].iov_len = cur->cur;
				idx++;

				if(idx == IOV_MAX) {
					bytes += writev(logf->fd, &iovs[0], idx);
					if(bytes == -1) 
						perror("Write Log ERROR");
					idx = 0;
				}
			
			
			//bytes += cur->cur;
			
			
			}	
			tail = cur;
			cur =  cur->next;
		}
	
	}

//	printf("Flush idx %d num  %d\n", idx , num);
	if(idx > 0) {
		bytes += writev(logf->fd, &iovs[0], idx);
		if(bytes == -1) {
			for(int i = 0; i < idx; i++)
				printf("LOG ERROR: Write Bufer %p %lu \n", iovs[i].iov_base, iovs[i].iov_len);
			printf("MAX %d cur %d", IOV_MAX, idx);
			perror("");
			exit(1);
		}
	}

	fdatasync(logf->fd);
	
	if(tail != NULL) {
		freelock.Lock();
		tail->next = freebufs;
		freebufs = lsbufs;
		freelock.Unlock();
	}


	
	uint64_t minsn = 0;
	for(int i = 0; i < buflen; i++) {
		if(localsn[i] == 0)
			continue;
		
		if(minsn == 0 || minsn > localsn[i]) {
			minsn = localsn[i];
		}
	}
	uint64_t old = safe_sn;
	safe_sn = minsn > 0 ? minsn - 1: 0;
	if (old > safe_sn) printf("TBuf %ld %ld\n",old, safe_sn);
	return bytes;
}

void TBuf::Print()
{
	
}



