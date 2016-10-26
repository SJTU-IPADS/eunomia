#ifndef RCU_H
#define RCU_H

#include <stdint.h>

#define ENDMASK ((uint64_t)1<<63)
#define BEGMASK ~((uint64_t)1<<63)

class RCU {

	static __thread int tid;

	struct State {
		
		//should exclusively occupy a cache line
		volatile uint64_t counter;
		char padding[56];
		
		State(): counter(ENDMASK) {}
		
		inline void BeginTX() 
		{
			counter++;
			counter &= BEGMASK;
		}
		
		inline void EndTX() 
		{
			counter |= ENDMASK;
		}

		inline bool Safe(uint64_t c) 
		{
			return counter > c;
		}
	};
	
public:
	volatile bool sync;
	State* states;
	int thrs_num;
	

	RCU(int thrs);
	
	~RCU();

	void RegisterThread(int i);
	
	void WaitForGracePeriod();

	void BeginTX();

	void EndTX();

	void Print();

private:
	uint64_t *GetStatesCopy();
	
};


#endif