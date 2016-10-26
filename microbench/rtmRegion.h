// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef RTMRegion_H_
#define RTMRegion_H_

#include <immintrin.h>
#include <sys/time.h>

struct RTMRegionProfile {
	int abort;
	int succ;
	int conflict;
	int capacity;
	int zero;
	int nest;
	RTMRegionProfile(): 
		abort(0), succ(0), conflict(0), capacity(0), zero(0), nest(0){}
	
	void ReportProfile()
	{
		printf("Avg Abort %.5f [Conflict %.5f : Capacity %.5f Zero: %.5f Nest: %.5f] \n",
				abort/(double)succ, conflict/(double)succ, 
				capacity/(double)succ, zero/(double)succ,
				nest/(double)succ);
		printf("Succ %d\n", succ);
	}

	void Reset(){
		abort = 0;
		succ = 0;
		conflict = 0;
		capacity = 0;
		zero = 0;
		nest = 0;
	}
};


#define MAXRETRY 100000

class RTMRegion {

 public:
 	
 RTMRegionProfile* prof;
 int abort;
 int conflict;
 int capacity;
 int zero;
 int nest;
 
 inline RTMRegion(RTMRegionProfile *p) {
 	
#ifdef PROF
	abort = 0;
	conflict = 0;
	capacity = 0;
	zero = 0;
	prof = p;
#endif

	while(true) {
	    unsigned stat;
	 	stat = _xbegin();
		
		if(stat == _XBEGIN_STARTED) {		  
		  return;
			
		} else {

#ifdef PROF
		  abort++;

		  if(stat & _XABORT_NESTED)
			nest++;
		  
		  if(stat & _XABORT_CONFLICT)
			conflict++;
		  
		  if(stat & _XABORT_CAPACITY) { 
			capacity++;
		  }
		  
		  if(stat == 0)
		  	zero++;

		  if(abort > MAXRETRY)
		  	return;
#endif
		}
	}
	
  }

  void Abort() {
  	
  	  _xabort(0x1);
	
  }
inline  ~RTMRegion() {  
	if(_xtest())
	  _xend ();

#ifdef PROF
if(prof != NULL) {
	  prof->abort += abort;
	  prof->capacity += capacity;
	  prof->conflict += conflict;
	  prof->zero += zero;
	  prof->nest += nest;
	  prof->succ++;
	  }
#endif
 }

};

#endif  // STORAGE_LEVELDB_UTIL_RTM_H_
