#ifndef _BLOOM_MURMURHASH2
#define _BLOOM_MURMURHASH2

#include <stdint.h>
unsigned int murmurhash2(const uint64_t * key, int len, const unsigned int seed);

#endif
