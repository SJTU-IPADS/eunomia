#ifndef PBUF_H
#define PBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "util/spinlock.h"
#include "log.h"
#include "lbuf.h"
#include "tbuf.h"


#define g_max_lag_epochs 8096
struct persist_stats {
  // how many txns this thread has persisted in total
  uint64_t ntxns_persisted_;

  // sum of all latencies (divid by ntxns_persisted_ to get avg latency in
  // us) for *persisted* txns (is conservative)
  uint64_t latency_numer_;

  // per last g_max_lag_epochs information
  struct per_epoch_stats {
	uint64_t ntxns_;
	uint64_t earliest_start_us_;

	per_epoch_stats() : ntxns_(0), earliest_start_us_(0) {}
  } d_[g_max_lag_epochs];

  persist_stats() :
	ntxns_persisted_(0), latency_numer_(0) {}
};

class PBuf {
	
static __thread int tid_;
static volatile bool sync_;

int buflen;
LocalPBuf** lbuf;
//Only update in the logger thread
volatile uint64_t safe_sn;
public:
persist_stats* g_persist_stats; 
	
volatile uint64_t last_safe_sn;


TBuf* tbufs[2];


	PBuf(int thr);
	
	~PBuf();

	static void* loggerThread(void * arg);
	
	void RegisterThread(int tid);

	void RecordTX(uint64_t sn, int recnum);

	void WriteRecord(int tabid, uint64_t key, 
		uint64_t seqno, uint64_t* value, int vlen);
	
	void FrozeLocalBuffer(int idx);

	void FrozeAllBuffer();

	void Sync();

	void Writer();
		
	void Print();

	uint64_t GetSafeSN() {
		uint64_t old = safe_sn;		
		if (buflen > 1) {
			uint64_t s0 = tbufs[0]->GetSafeSN();
			uint64_t s1 = tbufs[1]->GetSafeSN();
			safe_sn = s0 < s1? s0: s1;
		}
		else 
			safe_sn = tbufs[0]->GetSafeSN();
		
		if (old > safe_sn) printf("PBuf old %ld  safe %ld  t0 %ld t1 %ld\n",old, safe_sn,
			tbufs[0]->GetSafeSN(),tbufs[1]->GetSafeSN() );
		return safe_sn;
	};


};


#endif
