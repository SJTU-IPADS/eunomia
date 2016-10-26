#ifndef BLOOMFILTER_H
#define	BLOOMFILTER_H

#include <stdlib.h>
#include <stdint.h>

//typedef struct BloomFilter BloomFilter;

struct BloomFilter {
	size_t		m;
	size_t		k;
	size_t		size;
	uint64_t		*bits;
	size_t		bits_length;
	size_t 		bits_size;
};


/* A callback function for exactly determining if the string is contained.
 * For a "pure" probabilistic Bloom filter, use NULL. For our application,
 * we must be able to determine exact containment.
*/


extern BloomFilter *	bloom_filter_new(size_t filter_size, size_t num_hashes);
extern BloomFilter *	bloom_filter_new_with_probability(float prob, size_t num_elements);
extern size_t		bloom_filter_num_bits(const BloomFilter *bf);
extern size_t		bloom_filter_num_hashes(const BloomFilter *bf);
extern size_t		bloom_filter_size(const BloomFilter *bf);
extern void		bloom_filter_insert(BloomFilter *bf, const uint64_t *string);
extern bool		bloom_filter_contains(const BloomFilter *bf, const uint64_t *string);
extern void		bloom_filter_destroy(BloomFilter *bf);
extern void bloom_filter_flush(BloomFilter* bf);

#endif	/* BLOOMFILTER_H */
