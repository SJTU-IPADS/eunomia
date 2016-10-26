#ifndef LOCKREGION_H
#define LOCKREGION_H

#include <stdint.h>

typedef unsigned char  uint8_t;
typedef unsigned long uint64_t;

#define cpu_relax() asm volatile("pause\n": : :"memory")

inline void atomic_inc64(uint64_t *p) {
	asm volatile(
		"lock;incq %0"
		: "+m"(*p)
		:
		: "cc");
}

inline void atomic_add64(uint64_t* addr, uint64_t val) {
	asm volatile(
		"lock;addq %1, %0"
		: "+m"(*addr)
		: "a"(val)
		: "cc");
}

inline uint8_t xchg8(uint8_t *addr, uint8_t val) {
	asm volatile(
		"xchgb %0,%1"
		:"=r"(val)
		:"m"(*addr), "0"(val)
		:"memory");

	return val;
}



/* The counter should be initialized to be 0. */
class LockRegion  {

public:
	uint8_t *lock;
	inline LockRegion(uint8_t *l) {
		lock = l;
		while(1) {
			if(!xchg8(lock, 1)) return;

			while(*lock) cpu_relax();
		}
	}

	inline ~LockRegion() {
		*lock = 0;
	}
};

#endif /* _RWLOCK_H */
