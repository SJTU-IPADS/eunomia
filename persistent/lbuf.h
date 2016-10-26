#ifndef LBUF_H
#define LBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include "log.h"

#include <sys/time.h>
#include <time.h>


#define BSIZE 128*1024*1024 //64MB


class LocalPBuf {

public:
	
	uint64_t sn;
	int cur;
	char *buf;
	int txns;
	uint64_t earliest_start_us_;
	LocalPBuf* next;
		
	LocalPBuf() {

		next = NULL;
		sn = 0;
		cur = 0;
		txns = 0;
		earliest_start_us_ = 0;
		buf = new char[BSIZE];
	}

	int EmptySlotNum() {
		return BSIZE - cur;
	}

	uint64_t GetSN() {
		return sn;
	}

	void SetSN(uint64_t s) {
		sn = s;
		if (txns == 0) 
			earliest_start_us_ = cur_usec();
	}

	inline uint8_t *
	write_uvint64(uint8_t *buf, uint64_t value)
	{
	  while (value > 0x7F) {
	    *buf++ = (((uint8_t) value) & 0x7F) | 0x80;
	    value >>= 7;
	  }
	  *buf++ = ((uint8_t) value) & 0x7F;
	  return buf;
	}

	void PutRecord(int tabid, uint64_t key, 
		uint64_t seqno, uint64_t* value, int vlen)
	{
			//memcpy(&buf[cur], (char *)&tabid, sizeof(int));
			//cur += sizeof(int);
			
			memcpy(&buf[cur], (char *)&key, sizeof(uint64_t));
			cur += sizeof(uint64_t);

			uint8_t temp[8];
			uint8_t *rtemp = write_uvint64(temp, seqno);
			int size = (uint64_t)rtemp- (uint64_t)temp;
			memcpy(&buf[cur], temp, size);
			cur += size;
			
//			memcpy(&buf[cur], (char *)&seqno, sizeof(uint64_t));
//			cur += sizeof(uint64_t);

			if(value == NULL) {
				uint64_t nulval = -1;
				memcpy(&buf[cur], (char *)&nulval, sizeof(uint64_t));
				cur += sizeof(uint64_t);
			} else {
				memcpy(&buf[cur], (char *)&value, vlen);
				cur += vlen;
			}
			
			assert(cur < BSIZE);
	}

	void Reset() {
		
		earliest_start_us_ = 0;
		txns = 0;
		cur = 0;
		sn = 0;
		next = NULL;
	}

	int Serialize(Log* lf) {

		lf->writeLog(buf, cur);
		return cur;
	}
	static inline uint64_t
	cur_usec()
	{
  		struct timeval tv;
		  gettimeofday(&tv, 0);
	  return ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
	}
};

#endif
