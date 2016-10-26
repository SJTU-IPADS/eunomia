#ifndef TBUF_H
#define TBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "util/spinlock.h"
#include "log.h"
#include "lbuf.h"

class TBuf {

	static volatile bool sync_;
	char *logpath;
	
	pthread_t write_id;

	int buflen;

	SpinLock frozenlock;
	LocalPBuf** frozenbufs;
	int frozennum;

	
	SpinLock freelock;
	LocalPBuf* freebufs;

	LocalPBuf** flushbufs;

	
	uint64_t* localsn;

	Log* logf;

	//Only update in the logger thread
	volatile uint64_t safe_sn;


public:

	TBuf(int thr, char *logpath);
	
	~TBuf();

	static void* loggerThread(void *arg);
	
	void PublishLocalBuffer(int tid, LocalPBuf* lbuf);
	
	LocalPBuf* GetFreeBuf();
	
	void Sync();

	void WaitSyncFinish();
	int Writer();
		
	void Print();

	uint64_t GetSafeSN() {return safe_sn;};
	
};


#endif
