#include "lockfree_hash.h"

namespace leveldb {
	__thread LockfreeHashTable::HashNode *LockfreeHashTable::dummynode_ = NULL;
}
