#ifndef DBTABLES_H
#define DBTABLES_H

#include "memstore/memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_eunotree.h"
#include "memstore/memstore_cuckoohash.h"
#include "memstore/memstore_hash.h"
#include "memstore/secondindex.h"
#include "memstore/memstore_stringbplustree.h"
#include "memstore/memstore_uint64bplustree.h"
#include "memstore/secondindex_uint64bplustree.h"
#include "memstore/secondindex_stringbplustree.h"
#include "db/epoch.h"
#include "db/gcqueue.h"
#include "db/rmqueue.h"
#include "db/nodebuf.h"
#include "db/objpool.h"
#include "db/rcu.h"
#include "db/rmpool.h"
#include "persistent/pbuf.h"

#define USESECONDINDEX 0

namespace leveldb {

class RMQueue;
class RMPool;
class DBTX;

#define NONE 0
#define BTREE 1
#define HASH 2
#define CUCKOO 6
#define SKIPLIST 3
#define IBTREE 4
#define SBTREE 5
#define EUNO_BTREE 6

//GC when the number of gc objects reach GCThreshold
//XXX FIXME: this is critical to the performance,
//larger means less gc times and higher performance
#define GCThreshold 100000

//GC when the number of rm objects reach RMThreshold
#define RMThreshold 100

class DBTables {
	struct TableSchema {
		int klen;
		int vlen; //We didn't support varible length of value
	};

public:
	static __thread GCQueue* nodeGCQueue;
	static __thread GCQueue* valueGCQueue;
	static __thread RMQueue* rmqueue;
	static __thread NodeBuf* nodebuffer;


	static __thread OBJPool* valuesPool;
	static __thread OBJPool* memnodesPool;
	static __thread uint64_t gcnum;

	static __thread RMPool* rmPool;

	volatile uint64_t snapshot; // the counter for current snapshot: 1 is the first snapshot
	int	threads;

	uint64_t dist_last_id[64][10];

	int number;
	Memstore **tables;
	TableSchema* schemas;
	SecondIndex ** secondIndexes;
	int *types;
	int *indextypes;
	int next;
	int nextindex;
	Epoch* epoch;
	RCU* rcu;
	PBuf* pbuf_;

	static Memstore::MemNode* bugnode;

	DBTables();
	DBTables(int n);

	//n: tables number, thr: threads number
	DBTables(int n, int thrs);

	~DBTables();

	void TupleInsert(int tabid, uint64_t key, uint64_t *val, int len);

	void ThreadLocalInit(int tid);
	int AddTable(int tableid, int index_type, int secondary_index_type);

	void AddSchema(int tableid, int kl, int vl);

	//For Epoch
	void InitEpoch(int thr_num);
	void EpochTXBegin();
	void EpochTXEnd();

	void AddDeletedNodes(uint64_t **nodes, int len);
	void GCDeletedNodes();
	void AddDeletedValues(uint64_t **nodes, int len);
	void GCDeletedValues();
	void AddRemoveNodes(uint64_t **nodes, int len);
	void RemoveNodes();

	//For RCU
	void RCUInit(int thr_num);
	void RCUTXBegin();
	void RCUTXEnd();

	void AddDeletedValue(int tableid, uint64_t* value, uint64_t sn);
	uint64_t*GetEmptyValue(int tableid);

	void AddDeletedNode(int tableid, uint64_t *node);

	void AddRemoveNode(int tableid, uint64_t key, uint64_t seq, Memstore::MemNode* value);

	Memstore::MemNode* GetMemNode(int tableid);
	void GC();

	void DelayRemove();


	//For Perisistence
	void PBufInit(int thrs);
	void Sync();
	void WriteUpdateRecords();

	//An independent thread updates the snapshot number periodically
	static void* SnapshotUpdateThread(void * arg);
	static void* PersistentInfoThread(void * arg);
	static void DEBUGGC();

};

}
#endif
