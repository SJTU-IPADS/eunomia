#ifndef _STATISTICS_UTIL_H_
#define _STATISTICS_UTIL_H_

struct table_prof_s{
	uint64_t inner_local_access;
	uint64_t inner_remote_access;
	uint64_t leaf_local_access;
	uint64_t leaf_remote_access;
	uint64_t buffer_local_access;
};

static table_prof_s table_prof;

#endif
