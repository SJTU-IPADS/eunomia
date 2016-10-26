/*conflict control module*/
//#include <arch/x86/include/asm/bitops.h>
#include <stdint.h>
#include "port/atomic.h"
#define ISBIT(bits, index) ((bits >> index) & 0x1)
#define SETBIT(bits, index) (bits | (1UL << index))
#define UNSETBIT(bits, index) (bits & ((-1) - (1UL << index)))
#define SEEDCOUNT 6
#define LEN 5
#define SIZE 64
#define LEAF_THRESHOLD 12
class CCM {
public:
	/*
	 *We adopt a byte to represent a bit in lock_bits, since there is no support to
	 *flip a bit atomically
	 */
	/*read lock bit*/
	volatile uint16_t read_lock_bits[64];
	/*write lock bit*/
	volatile uint16_t write_lock_bits[64];
	/*mark bits to check whether the key is in the leaf node*/
	uint64_t mark_bits;
	/*operation num*/
	uint64_t operation;
	/*whether lock phase should be skipped*/
	bool skip_lock = false;
	/*record conflict count in RTM*/
	struct Counter {
		unsigned conflict_count;
		unsigned mark_count;
	} counter;
public:
	CCM() {}

	/*lock operation*/
	inline void read_Lock(unsigned int index) {
		//read_lock_bits = SETBIT(read_lock_bits, index);

		while(1) {
			if(!xchg16((uint16_t *)&read_lock_bits[index], 1)) return;

			while(read_lock_bits[index]) cpu_relax();
		}

	}

	inline void read_Unlock(unsigned int index) {
		barrier();
		read_lock_bits[index] = 0;
		//read_lock_bits = UNSETBIT(read_lock_bits, index);
	}

	inline void write_Lock(unsigned int index) {
		//write_lock_bits = SETBIT(write_lock_bits, index);
		while(1) {
			if(!xchg16((uint16_t *)&write_lock_bits[index], 1)) return;

			while(write_lock_bits[index]) cpu_relax();
		}
	}

	inline void write_Unlock(unsigned int index) {
		barrier();
		write_lock_bits[index] = 0;
		//write_lock_bits = UNSETBIT(write_lock_bits, index);
	}

	inline uint16_t IsReadLocked(unsigned int index) {
		//return ISBIT(read_lock_bits, index);
		return read_lock_bits[index];
	}

	inline uint16_t IsWriteLocked(unsigned int index) {
		//return ISBIT(write_lock_bits, index);
		return write_lock_bits[index];
	}

	inline void set_mark_bits(uint64_t key, unsigned int slot) {
		if(!ISBIT(key, slot)) {
			counter.mark_count++;
		}
		add_mark_bit(slot);
	}

	inline void unset_mark_bits(uint64_t key) {
		unsigned slot = getIndex(key);
		if(ISBIT(key, slot)) {
			counter.mark_count--;
		}
		delete_mark_bit(slot);
	}

	inline bool isfound(uint64_t key) {
		return ((mark_bits >> getIndex(key)) & 0x1);
	}

	inline void clear_conflict() {
		counter.conflict_count = 0;
	}

	inline void add_conflict_num(unsigned add_num) {
		//printf("num:%d\n", add_num);
		counter.conflict_count += add_num;
	}

	inline void add_operation_num() {
		operation++;
	}

	/*set all mark bits to 0*/
	inline void clearMarkCount() {
		mark_bits = 0;
		counter.mark_count = 0;
	}

    /*judge if a leaf node is under high conflict rate*/
	inline bool is_conflict() {
		//printf("operation:%ld, conflict:%d, rate:%f\n", operation, counter.conflict_count, (counter.conflict_count + 0.0) / (operation + 0.0));
		if(operation > 100) {
			return ((counter.conflict_count + 0.0) / (operation + 0.0)) > 0.002;
		} else
			return !skip_lock;
	}

	inline double get_conflict() {
		//printf("operation:%ld, conflict:%d, rate:%f\n", operation, counter.conflict_count, (counter.conflict_count + 0.0) / (operation + 0.0));
		//printf("operation:%ld, conflict:%d, rate:%f\n", operation, counter.conflict_count, (counter.conflict_count + 0.0) / (operation + 0.0));
		if(operation > 10) {
			return ((counter.conflict_count + 0.0) / (operation + 0.0));
		} else
			return 0.5;
	}

	/*get the slot index of the key in mark bit*/
	unsigned int getIndex(uint64_t key) {
		int Ret[SEEDCOUNT];
		int temp = 0;
		GenerateHashValue(key, LEN, Ret, SEEDCOUNT);
		for(int i = 0; i < SEEDCOUNT; i++)
			temp += Ret[i];
		return (temp % 64 + 64) % 64;
	}

	/*if most bits in mark_bits are set, it should skip the query process*/
	inline bool skipBF() {
		return counter.mark_count >= LEAF_THRESHOLD;
	}

	inline uint64_t getOperation() {
		return operation;
	}

private:
	/*set one of mark bit to 1*/
	inline void add_mark_bit(unsigned int index) {
		mark_bits = SETBIT(mark_bits, index);
	}

	/*unset mark bit*/
	inline void delete_mark_bit(unsigned int index) {
		mark_bits = UNSETBIT(mark_bits, index);
	}

	/*generate a hash value*/
	int GenerateHashValue(uint64_t key, unsigned int len, int* Ret, int retLen)  {
		if(retLen > SEEDCOUNT) {
			return -1;
		}
		for(int i = 0; i < retLen; i++) {
			Ret[i] = GenerateHashValue(key, len, i);
			//printf("%d:%d\n", i, Ret[i]);
		}
		return retLen;
	}

	/*use hash functions to generate a hash value*/
	int GenerateHashValue(uint64_t str, unsigned int len, int hasFunIndex) {
		switch(hasFunIndex) {
		case 0:
			return RSHash(str, len);
		case 1:
			return JSHash(str, len);
		default:
			return RSHash(str, len);
		}
	}
	/*hash function 1*/
	unsigned int RSHash(uint64_t str, unsigned int len) {
		unsigned int b    = 378551;
		unsigned int a    = 63689;
		unsigned int hash = 0;
		unsigned int i    = 0;
		for(i = 0; i < len; str++, i++) {
			hash = hash * a + str;
			a = a * b;
		}
		return hash;
	}
	/*hash function 2*/
	unsigned int JSHash(uint64_t str, unsigned int len) {
		unsigned int hash = 1315423911;
		unsigned int i    = 0;
		for(i = 0; i < len; str++, i++) {
			hash ^= ((hash << 5) + str + (hash >> 2));
		}
		return hash;
	}
};
