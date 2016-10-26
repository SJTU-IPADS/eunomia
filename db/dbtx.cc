// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBTRANSACTIONTEMPLATE_H_
#define STORAGE_LEVELDB_DB_DBTRANSACTIONTEMPLATE_H_

#include <string>
#include <time.h>
#include "leveldb/db.h"
#include "util/rtm.h"
#include "silo_benchmark/tpcc.h"

#include "db/dbformat.h"
#include "db/dbtx.h"
#include "silo_benchmark/util.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "util/numa_util.h"

namespace leveldb {

port::Mutex DBTX::storemutex;

SpinLock DBTX::slock;

__thread DBTX::WriteSet* DBTX::writeset = NULL;
__thread DBTX::ReadSet* DBTX::readset = NULL;
__thread DELSet* DBTX::deleteset =
	NULL;

__thread bool DBTX::localinit = false;
__thread DBTX::BufferNode* DBTX::buffer = NULL;

#define MAXSIZE 1024 // the initial read/write set size

void DBTX::ThreadLocalInit() {
	if(false == localinit) {
		//printf("%ld localinit AA\n",pthread_self());
		readset = new ReadSet();
		writeset = new WriteSet();
		deleteset = new DELSet();
#if BUFFERNODE
		assert(txdb_->number > 0);
		buffer = new BufferNode[txdb_->number];
#endif
		localinit = true;
		//	printf("%ld localinit BB\n", pthread_self());
	}
}

DBTX::ReadSet::ReadSet() {
	max_length = MAXSIZE;
	elems = 0;
	seqs = new RSSeqPair[max_length];

	rangeElems = 0;
	nexts = new RSSuccPair[max_length];

	//pretouch to avoid page fault in the rtm
	for(int i = 0; i < max_length; i++) {
		seqs[i].seq = 0;
		seqs[i].seqptr = NULL;
		nexts[i].next = 0;
		nexts[i].nextptr = NULL;
	}
}

DBTX::ReadSet::~ReadSet() {
	delete[] seqs;
	delete[] nexts;
}

inline void DBTX::ReadSet::Reset() {
	elems = 0;
	rangeElems = 0;
}

void DBTX::ReadSet::Resize() {
	printf("Read Set Resize\n");
	max_length = max_length * 2;

	RSSeqPair *ns = new RSSeqPair[max_length];
	for(int i = 0; i < elems; i++) {
		ns[i] = seqs[i];
	}
	delete[] seqs;
	seqs = ns;


	RSSuccPair* nts = new RSSuccPair[max_length];
	for(int i = 0; i < rangeElems; i++) {
		nts[i] = nexts[i];
	}
	delete[] nexts;
	nexts = nts;
}

inline void DBTX::ReadSet::AddNext(uint64_t *ptr, uint64_t value) {
//if (max_length < rangeElems) printf("ELEMS %d MAX %d\n", rangeElems, max_length);
	assert(rangeElems <= max_length);

	if(rangeElems == max_length) {
		//printf("AddNext\n");
		Resize();
	}

	int cur = rangeElems;
	rangeElems++;

	nexts[cur].next = value;
	nexts[cur].nextptr = ptr;
}

inline void DBTX::ReadSet::Add(uint64_t *ptr, int label) {
	if(max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
	assert(elems <= max_length);

	if(elems == max_length)
		Resize();

	int cur = elems;
	elems++;

	seqs[cur].seq = *ptr;
	seqs[cur].seqptr = ptr;
	seqs[cur].label = label;
}

inline int DBTX::ReadSet::Validate() {
	//This function should be protected by rtm or mutex
	//Check if any tuple read has been modified
	for(int i = 0; i < elems; i++) {
		assert(seqs[i].seqptr != NULL);
		if(seqs[i].seq != *seqs[i].seqptr) {
			return seqs[i].label;
		}
	}

	//Check if any tuple has been inserted in the range
	for(int i = 0; i < rangeElems; i++) {
		assert(nexts[i].nextptr != NULL);
		if(nexts[i].next != *nexts[i].nextptr) {
			return 0;
		}
	}
	return NO_CONFLICT;
}

void DBTX::ReadSet::Print() {
	for(int i = 0; i < elems; i++) {
		printf("Key[%d] ", i);
		printf(
			"Old Seq %lu Cur Seq %lu Seq Addr 0x%p ",
			seqs[i].seq, *seqs[i].seqptr, seqs[i].seqptr);
	}
}

DBTX::WriteSet::WriteSet() {
	max_length = MAXSIZE; //first allocate 1024 numbers
	elems = 0;
#if USESECONDINDEX
	cursindex = 0;
#endif

	dbtx_ = NULL;

	commitSN = 0;

	kvs = new WSKV[max_length];
#if USESECONDINDEX
	sindexes = new WSSEC[max_length];
#endif

	for(int i = 0; i < max_length; i++) {
		kvs[i].key = 0;
		kvs[i].val = NULL;
		kvs[i].node = NULL;
		kvs[i].dummy = NULL;
		kvs[i].commitseq = 0;
		kvs[i].commitval = NULL;
	}

#if CACHESIM
	for(int i = 0; i < 64; i++) {
		cacheset[i] = 0;
		for(int j = 0; j < 8; j++) {
			cacheaddr[i][j] = 0;
			cachetypes[i][j] = 0;
		}
	}
#endif
}

DBTX::WriteSet::~WriteSet() {
	//FIXME: Do nothing here
}

void DBTX::WriteSet::Resize() {
	printf("Write Set Resize\n");
	max_length = max_length * 2;

	//resize the wskv
	WSKV* nkv = new WSKV[max_length];

	for(int i = 0; i < elems; i++) {
		nkv[i] = kvs[i];
	}

	//delete[] kvs;

	kvs = nkv;

	//FIXME: didn't resize the secondary index array
}

void DBTX::WriteSet::Clear() {

	for(int i = 0; i < elems; i++) {
#if COPY_WHEN_ADD
		if(kvs[i].val != LOGICALDELETE)
			delete writeset->kvs[i].val;
#endif
		if(kvs[i].dummy != NULL)
			delete kvs[i].dummy;
	}

	elems = 0;
	commitSN = 0;
}

void DBTX::WriteSet::Reset() {
	elems = 0;
	commitSN = 0;

#if USESECONDINDEX
	cursindex = 0;
#endif
#if CACHESIM
	for(int i = 0; i < 64; i++) {
		cacheset[i] = 0;
		for(int j = 0; j < 8; j++) {
			cacheaddr[i][j] = 0;
			cachetypes[i][j] = 0;
		}
	}
#endif
}

void DBTX::WriteSet::TouchAddr(uint64_t addr, int type) {

	uint64_t caddr = addr >> 12;
	int index = (int)((addr >> 6) & 0x3f);

	for(int i = 0; i < 8; i++) {

		if(cacheaddr[index][i] == caddr)
			return;

	}

	cacheset[index]++;
	static int count = 0;
	if(cacheset[index] > 8) {
		count++;
		printf("Cache Set [%d] Conflict type %d\n", index , type);
		for(int i = 0; i < 8; i++) {
			printf("[%lu][%lu] ", cachetypes[index][i], cacheaddr[index][i]);
		}
		printf(" %d \n", count);
	}

	for(int i = 0; i < 8; i++) {
		if(cacheaddr[index][i] == 0) {
			cacheaddr[index][i] = caddr;
			cachetypes[index][i] = type;
			return;
		}
	}
}

void DBTX::WriteSet::Add(int tableid, uint64_t key, uint64_t* val, Memstore::MemNode* node) {
	assert(elems <= max_length);
	//printf("WriteSet ADD %lx\n", val);
	if(elems == max_length) {
		Resize();
	}
	int cur = elems;
	elems++;

	kvs[cur].tableid = tableid;
	kvs[cur].key = key;
	kvs[cur].val = val;
	kvs[cur].node = node;
	kvs[cur].dummy = NULL;
}

inline void DBTX::WriteSet::Add(uint64_t *seq, SecondIndex::MemNodeWrapper* mnw, Memstore::MemNode* node) {
#if USESECONDINDEX

	if(cursindex >= max_length) {
		//FIXME: No resize support!
		printf("Error: sindex overflow\n");
	}

	sindexes[cursindex].seq = seq;
	sindexes[cursindex].sindex = mnw;
	sindexes[cursindex].memnode = node;

	cursindex++;
#endif
}

inline bool DBTX::WriteSet::Lookup(int tableid, uint64_t key, uint64_t** val) {
	bool res = false;
	for(int i = 0; i < elems; i++) {
		if(kvs[i].tableid == tableid && kvs[i].key == key) {
			*val = kvs[i].val;
			res = true;
		}
	}
	return res;
}

inline void DBTX::WriteSet::UpdateSecondaryIndex() {
	assert(0);
}

inline void DBTX::WriteSet::SetDBTX(DBTX* dbtx) {
	dbtx_ = dbtx;
}

//gcounter should be added into the rtm readset
inline void DBTX::WriteSet::Write(uint64_t gcounter) {
#if PERSISTENT
	commitSN = gcounter;
#endif
	for(int i = 0; i < elems; i++) {
#if GLOBALOCK
		assert(kvs[i].node->counter <= gcounter);
#endif
		if(kvs[i].node->counter == 0)
			kvs[i].node->counter = gcounter;

		//If counter of the node is equal to the global counter, then just change the value pointer
		if(kvs[i].node->counter == gcounter) {

#if DEBUG_PRINT
			printf("[%ld] write %ld, node %lx, seq %ld, old value %ld new value %ld\n",
				   pthread_self(), kvs[i].key, kvs[i].node,
				   kvs[i].node->seq, (uint64_t)kvs[i].node->value, kvs[i].val);
#endif

			if(kvs[i].val == LOGICALDELETE  && kvs[i].node->value != LOGICALDELETE) {

#if DEBUG_PRINT
				printf("[%ld] Put %ld, node %lx, seq %ld, old value %ld into Delete Set\n",
					   pthread_self(), kvs[i].key, kvs[i].node,
					   kvs[i].node->seq, (uint64_t)kvs[i].node->value);
#endif
				assert(dbtx_ != NULL);
				dbtx_->deleteset->Add(kvs[i].tableid, kvs[i].key, kvs[i].node, true);

			}

			uint64_t* oldval = kvs[i].node->value;
			kvs[i].node->value = kvs[i].val; //overwrite to the original position
			kvs[i].commitval = kvs[i].val;

			kvs[i].val = oldval;

			kvs[i].node->seq++;
			kvs[i].commitseq = kvs[i].node->seq;

		} else if(kvs[i].node->counter < gcounter) {

#if DEBUG_PRINT
			printf("[%ld] write %ld, node %lx, seq %ld, old value %ld new value %ld\n",
				   pthread_self(), kvs[i].key, kvs[i].node,
				   kvs[i].node->seq, (uint64_t)kvs[i].node->value, kvs[i].val);
#endif

			//If global counter is updated, just update the counter and store a old copy into the dummy node
			//Clone the current value into dummy node

			//If is delete, put it into the delete set
			if(kvs[i].val == LOGICALDELETE 	&& kvs[i].node->value != LOGICALDELETE) {
#if DEBUG_PRINT
				printf("[%ld] Put %ld, node %lx, seq %ld, old value %ld into Delete Set\n",
					   pthread_self(), kvs[i].key, kvs[i].node,
					   kvs[i].node->seq, (uint64_t)kvs[i].node->value);
#endif
				//Invalidate secondary index when deletion
				dbtx_->deleteset->Add(kvs[i].tableid, kvs[i].key, kvs[i].node, true);
			}

			kvs[i].dummy = dbtx_->txdb_->GetMemNode(kvs[i].tableid);
			kvs[i].dummy->value = kvs[i].node->value;
			kvs[i].dummy->counter = kvs[i].node->counter;
			kvs[i].dummy->seq = kvs[i].node->seq; //need this ?
			kvs[i].dummy->oldVersions = kvs[i].node->oldVersions;

#if DEBUG_PRINT
			printf("Thread [%lx] ", pthread_self());
			kvs[i].dummy->Print();
#endif

			//update the current node
			kvs[i].node->oldVersions = kvs[i].dummy; //record the order versions
			kvs[i].node->counter = gcounter; //update to the current version number

			kvs[i].node->value = kvs[i].val;
			kvs[i].node->seq++;

			kvs[i].commitseq = kvs[i].node->seq;
			kvs[i].commitval = kvs[i].val;

			//Fix me: here we set the val in write set to be NULL
			kvs[i].val = NULL;
		} else {
			//Error Check: Shouldn't arrive here
			while(_xtest()) _xend();
			assert(0);
		}
	}
}

//Check if any record in the write set has been remove from the memstore
inline bool DBTX::WriteSet::CheckWriteSet() {
	for(int i = 0; i < elems; i++) {
		//the node has been removed from the memstore
		if(kvs[i].node->value == HAVEREMOVED)
			return false;
	}
	return true;
}

//Collect old version records
inline void DBTX::WriteSet::CollectOldVersions(DBTables* tables) {
	for(int i = 0; i < elems; i++) {
		//First check if there is value replaced by the put operation
		if(DBTX::ValidateValue(kvs[i].val)) {
			tables->AddDeletedValue(kvs[i].tableid, kvs[i].val, commitSN);
		}

		//Then check if there is some old version records
		if(kvs[i].dummy != NULL) {

			tables->AddDeletedNode(kvs[i].tableid, (uint64_t *) kvs[i].dummy);

			if(DBTX::ValidateValue(kvs[i].dummy->value))
				tables->AddDeletedValue(kvs[i].tableid, kvs[i].dummy->value, commitSN);
		}
	}
}

inline uint64_t** DBTX::WriteSet::GetOldVersions(int* len) {
	*len = 0;
	int ovn = 0;

	if(elems == 0) {
		return NULL;
	}

	//First get the number of values needed to be deleted
	for(int i = 0; i < elems; i++) {
		if(kvs[i].dummy != NULL)
			ovn++;
	}

	assert(ovn <= elems);

	if(ovn == 0)
		return NULL;

	uint64_t** arr = new uint64_t*[ovn];
	*len = ovn;

	int count = 0;
	for(int i = 0; i < elems; i++) {

		if(kvs[i].dummy != NULL) {
			assert(kvs[i].dummy->counter > 0);
			//FIXME: only delete the dummy, the value won't be deleted
			arr[count] = (uint64_t *)kvs[i].dummy;
			count++;
		}

	}
	assert(count == ovn);

	return arr;
}

inline uint64_t** DBTX::WriteSet::GetDeletedValues(int* len) {
	*len = 0;
	int dvn = 0;

	if(elems == 0) {
		return NULL;
	}

	//First get the number of values needed to be deleted
	for(int i = 0; i < elems; i++) {
		if(kvs[i].val == (uint64_t *)NULL || !DBTX::ValidateValue(kvs[i].val)) {
			kvs[i].val = NULL;
		} else {
			dvn++;
		}
	}

	if(dvn > elems) {
		printf("len %d elems %d\n", *len, elems);
	}

	assert(dvn <= elems);

	if(dvn == 0)
		return NULL;

	uint64_t** arr = new uint64_t*[dvn];
	*len = dvn;

	int count = 0;
	for(int i = 0; i < elems; i++) {

		if(kvs[i].val != (uint64_t *)NULL) {
			arr[count] = kvs[i].val;
			count++;
		}

	}
	assert(count == dvn);

	return arr;
}


void DBTX::WriteSet::Print() {
	for(int i = 0; i < elems; i++) {
		/*
		printf("Key[%d] ", i);
		if(seqs[i].seq != NULL) {
			printf(

		"Old Seq %ld Cur Seq %ld Seq Addr 0x%lx ",
				seqs[i].oldseq, *seqs[i].seq, seqs[i].seq);
		}

		printf("key %s  ", keys[i]->Getslice());
		printf("hash %ld\n", hashes[i]);
		*/
	}
}

DBTX::DBTX(DBTables* store) {
	//printf("DBTX\n");
	txdb_ = store;
	count = 0;
#if END_TIME
	other_time = validate_time = write_time = end_time = 0;
	ends = end_elems = end_try_times;
#endif
	
#if ABORT_REASON
	read_invalid = write_invalid = other_invalid = 0;
#endif
	
#if SET_TIME
	begintime=addtime=gettime=endtime=aborttime=iterprevtime=iternexttime=iterseektime=iterseektofirsttime=0;
	begins=adds=gets=ends=nexts=prevs=seeks=seektofirsts=0;
#endif

#if TREE_TIME
	treetime = 0;
#endif

#if ORLI_BKD
	tree_time=set_time=total_time=buffer_time=orli_inserts=0;
#endif

	abort = false;
#if PROFILEBUFFERNODE
	bufferGet = 0;
	bufferMiss = 0;
	bufferHit = 0;
#endif
	searchTime = 0;
	traverseTime = 0;
	traverseCount = 0;
	for(int i = 0; i < TABLE_NUM; i++){
		local_access[i] = remote_access[i] = 0;
	}

#if ABORT_REASON
	for(int i = 0; i < TOTAL_GETS; i++){
		abort_reason_txns[i] = 0;
	}
#endif
}

DBTX::~DBTX() {
	//printf("~DBTX\n");
//#if END_TIME
//	printf("end_time = %lu, validate_phase = %lu, write_phase = %lu, other_phase = %lu\n", end_time, validate_time, write_time, other_time);
//#endif
	//rtmProf.reportAbortStatus();
#if ORLI_BKD
	printf("%lu, %lu, %lu, %lu, %lu\n", tree_time, set_time, buffer_time, total_time, orli_inserts);
#endif
	
#if ABORT_REASON
	for(int i = 1; i < TOTAL_GETS; i++){
		if(abort_reason_txns[i]!=0){
			printf("Abort reason[%d] = %d\n", i, abort_reason_txns[i]);
		}
	}
#endif

#if NUMA_DUMP
	for(int i = 0; i < TABLE_NUM; i++){
		printf("Table[%d] local_access: %d remote_access: %d\n",i, local_access[i], 
			remote_access[i]);
	}
#endif
#if RW_TIME_BKD
	printf("RWGET, %ld, ", gets);
	get_time.display();
	printf("RWADD, %ld, ", adds);
	add_time.display();
	printf("RWNEXT, %ld, ", nexts);
	next_time.display();
	printf("RWEND, %ld, ", ends);
	end_time.display();
#endif
#if TREE_TIME 
	printf("RW tree_time = %lf sec\n", (double)treetime/MILLION);
#endif
#if SET_TIME
/*
	printf("total begin_time = %ld(%ld)\n", begintime, begins);
	printf("total get_time = %ld(%ld)\n", gettime, gets);
	printf("total add_time = %ld(%ld)\n", addtime, adds);
	printf("total end_time = %ld(%ld)\n", endtime, ends);
	printf("total next_time = %ld(%ld)\n", iternexttime, nexts);
	printf("total prev_time = %ld(%ld)\n", iterprevtime, prevs);
	printf("total seek_time = %ld(%ld)\n", iterseektime, seeks);
*/
	printf("%ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld\n", 
		begintime, gettime, addtime, endtime, iternexttime, iterprevtime, iterseektime,
		begins, gets, adds, ends, nexts, prevs, seeks);

#endif

	//clear all the data
}

void DBTX::Begin() {
//reset the local read set and write set
	//txdb_->ThreadLocalInit();
#if SET_TIME
		util::timer t;
#endif

	ThreadLocalInit();
	readset->Reset();
	writeset->Reset();
	deleteset->Reset();

	txdb_->RCUTXBegin();

#if SET_TIME
	atomic_inc64(&begins);
	atomic_add64(&begintime, t.lap());
#endif

#if DEBUG_PRINT
	printf("[%ld] TX Begin\n", pthread_self());
#endif
}

bool DBTX::Abort() {
	//FIXME: clear all the garbage data
	readset->Reset();
	writeset->Reset();
	deleteset->Reset();

	abort = false;
	//Should not call End
	txdb_->RCUTXEnd();

	return abort;
}

bool DBTX::End() {
	//ends++;
	int dvlen;
	uint64_t **dvs;
	int ovlen;
	uint64_t **ovs;

	uint64_t **gcnodes;
	uint64_t **rmnodes;
	int read_valid_val = -1;
	
	if(abort) goto ABORT;
	//Phase 1. Validation & Commit
	{
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(&rtmProf, readset->elems, writeset->elems);
#endif

		read_valid_val = readset->Validate();
		bool read_valid = (read_valid_val == NO_CONFLICT);
		bool write_valid = writeset->CheckWriteSet();

		if(!(read_valid&&write_valid)) {
			goto ABORT;
		}

		writeset->SetDBTX(this);
		//step 2. update the the seq set
		//can't use the iterator because the cur node may be deleted
		writeset->Write(txdb_->snapshot);//snapshot is the counter for current snapshot

		//step 3. update the sencondary index
#if USESECONDINDEX
		writeset->UpdateSecondaryIndex();
#endif
	}
	
	//Put the objects into the object pool
	writeset->CollectOldVersions(txdb_);
	deleteset->GCRMNodes(txdb_);
	txdb_->RCUTXEnd();

#if PERSISTENT
	txdb_->WriteUpdateRecords();
#endif

#if RCUGC
	txdb_->GC();
	txdb_->DelayRemove();
#endif

	return true;

ABORT:
	
//#if END_TIME
//	t1.lap();
//#endif	
#if ABORT_REASON
	abort_reason_txns[read_valid_val]++;
#endif

	txdb_->RCUTXEnd();

#if RCUGC
	txdb_->GC();
	txdb_->DelayRemove();
#endif

	return false;
}

void DBTX::Add(int tableid, uint64_t key, uint64_t* val) {
#if RW_TIME_BKD
	util::timer total_time, tree_time, set_time;
	adds++;
#endif
	bool newNode = false;  

//  SpinLockScope spinlock(&slock);
retry:

	Memstore::MemNode* node;
	Memstore::InsertResult res;
#if PROFILEBUFFERNODE
	bufferGet++;
#endif
	
	//Get the seq addr from the hashtable
#if BUFFERNODE
	if(buffer[tableid].key == key
			&& buffer[tableid].node->value != HAVEREMOVED) {

	#if PROFILEBUFFERNODE
		bufferHit++;
	#endif
		node = buffer[tableid].node;
		assert(node != NULL);
	} else {

	#if PROFILEBUFFERNODE
		bufferMiss++;
	#endif
	#if RW_TIME_BKD
				tree_time.lap();
	#endif

		node = txdb_->tables[tableid]->GetWithInsert(key).node;//insert a new index
		//node = res.node;
		//newNode = res.hasNewNode;

		buffer[tableid].node = node;
		buffer[tableid].key = key;
	#if RW_TIME_BKD
		add_time.tree_time+=tree_time.lap();
	#endif

		//assert(node != NULL);
	}
#else
	node = txdb_->tables[tableid]->GetWithInsert(key).node;
	//node = res.node;
	//newNode = res.hasNewNode;
#endif

	if(node->value == HAVEREMOVED){
		goto retry;
	}
#if RW_TIME_BKD
		set_time.lap();
#endif
	writeset->Add(tableid, key, val, node);
#if RW_TIME_BKD
		add_time.set_time+=set_time.lap();
#endif

#if RW_TIME_BKD
		add_time.total_time+=total_time.lap();
#endif
}

uint64_t DBTX::Fetch_last_dist_id(uint warehouse_id, uint d){
	return __atomic_load_n(&txdb_->dist_last_id[warehouse_id-1][d-1] , __ATOMIC_ACQUIRE);
}

void DBTX::Store_last_dist_id(uint warehouse_id, uint d, uint64_t val){
	__atomic_store_n(&txdb_->dist_last_id[warehouse_id-1][d-1], val, __ATOMIC_RELEASE);
}


bool DBTX::Atomic_Fetch(int tableid, uint64_t key, uint64_t** val, uint64_t* orderline_id){
	bool newNode = false;
	//step 1. First check if the <k,v> is in the write set
	bool found = writeset->Lookup(tableid, key, val);
	if(found) {
		if((*val) == LOGICALDELETE)
			return false;
		return true;
	}
	//step 2.  Read the <k,v> from the in memory store
retry:		
	Memstore::MemNode* node;
	Memstore::InsertResult res;

	node = txdb_->tables[tableid]->GetWithInsert(key).node;

	if(node == NULL){
		return false;
	}
#if BUFFERNODE
	buffer[tableid].node = node;
	buffer[tableid].key = key;
//	assert(node != NULL);
#endif

	{
	//Guarantee
	#if GLOBALOCK
		SpinLockScope spinlock(&slock);
	#else
		RTMScope rtm(NULL);
	#endif
		if(node->value == HAVEREMOVED){
			goto retry;
		}
		//readset->Add(&node->seq, label);
	}
//district::value *v_d = (district::value *)d_value;
//atomic_v_d->d_next_o_id

	//if this node has been removed from the memstore
	if(ValidateValue(node->value)) {
		*val = node->value;
		district::value *v_d = (district::value *)(node->value);
		*orderline_id = __sync_fetch_and_add(&v_d->d_next_o_id,1);
		return true;
	} else {
		*val = NULL;
		return false;
	}
}

void DBTX::Add(int tableid, uint64_t key, uint64_t* val, int len) {
	//printf("[1]Add key = %lu\n", key);
retry:
	Memstore::MemNode* node;
	//Memstore::InsertResult res;
#if PROFILEBUFFERNODE
	bufferGet++;
#endif

	//Get the seq addr from the hashtable
#if BUFFERNODE
	if(buffer[tableid].key == key
			&& buffer[tableid].node->value != HAVEREMOVED) {

	#if PROFILEBUFFERNODE
		bufferHit++;
	#endif
		node = buffer[tableid].node;
		assert(node != NULL);
	} else {
	#if PROFILEBUFFERNODE
		bufferMiss++;
	#endif
		//uint64_t s_start = rdtsc();
		node = txdb_->tables[tableid]->GetWithInsert(key).node;
		//node = res.node;
		//newNode = res.hasNewNode;
	}
#else
	node = txdb_->tables[tableid]->GetWithInsert(key).node;
#endif

	if(node->value == HAVEREMOVED){
		goto retry;
	}

	char* value = reinterpret_cast<char *>(txdb_->GetEmptyValue(tableid));

	if(value == NULL) {
		value = (char *)malloc(sizeof(OBJPool::Obj) + len);
		value += sizeof(OBJPool::Obj);
	}
	memcpy(value, val, len);

	writeset->Add(tableid, key, (uint64_t *)value, node);
	//printf("[2]Add key = %lu\n", key);
}

void DBTX::Add_Label(int tableid, uint64_t key, uint64_t* val, int len) {
//  SpinLockScope spinlock(&slock);
/*
	if(tableid==6){
		printf("[%2d] insert key = %lu\n", sched_getcpu(), key);
	}
*/
#if ORLI_BKD
	util::timer total_timer, tree_timer, set_timer, buffer_timer;
	orli_inserts++;
#endif

retry:
	Memstore::MemNode* node;
	//Memstore::InsertResult res;
#if PROFILEBUFFERNODE
	bufferGet++;
#endif

	//Get the seq addr from the hashtable

#if BUFFERNODE

	#if ORLI_BKD
	buffer_timer.lap();
	#endif

	if(buffer[tableid].key == key
			&& buffer[tableid].node->value != HAVEREMOVED) {

	#if PROFILEBUFFERNODE
		bufferHit++;
	#endif
		node = buffer[tableid].node;
		assert(node != NULL);
	} else {
	#if PROFILEBUFFERNODE
		bufferMiss++;
	#endif
		//uint64_t s_start = rdtsc();

	#if ORLI_BKD
	buffer_time += buffer_timer.lap();
	#endif

	#if ORLI_BKD
		tree_timer.lap();
	#endif

		node = txdb_->tables[tableid]->GetWithInsert(key).node;

	#if ORLI_BKD
		tree_time+=tree_timer.lap();
	#endif
		//node = res.node;
		//newNode = res.hasNewNode;
	}
#else
	node = txdb_->tables[tableid]->GetWithInsert(key).node;
#endif

	if(node->value == HAVEREMOVED){
		goto retry;
	}
	
#if ORLI_BKD
	set_timer.lap();
#endif

	char* value = reinterpret_cast<char *>(txdb_->GetEmptyValue(tableid));

	if(value == NULL) {
		value = (char *)malloc(sizeof(OBJPool::Obj) + len);
		value += sizeof(OBJPool::Obj);
	}
	memcpy(value, val, len);

	writeset->Add(tableid, key, (uint64_t *)value, node);
	
#if ORLI_BKD
	set_time += set_timer.lap();	
#endif

#if ORLI_BKD
	total_time += total_timer.lap();
#endif

}


//Update a column which has a secondary key
void DBTX::Add(int tableid, int indextableid, uint64_t key, uint64_t seckey, uint64_t* new_val) {
#if RW_TIME_BKD
	util::timer total_time, tree_time, set_time;
	adds++;
#endif
	bool newNode = false;

retryA:
	uint64_t *seq;

#if PROFILEBUFFERNODE
	bufferGet++;
#endif

	Memstore::MemNode* node;
	//Memstore::InsertResult res;

#if BUFFERNODE
	//Get the seq addr from the hashtable
	if(buffer[tableid].key == key
			&& buffer[tableid].node->value != HAVEREMOVED) {

	#if PROFILEBUFFERNODE
		bufferHit++;
	#endif
		node = buffer[tableid].node;
		assert(node != NULL);
	} else {
	#if PROFILEBUFFERNODE
		bufferMiss++;
	#endif
#if RW_TIME_BKD
		tree_time.lap();
#endif
		node = txdb_->tables[tableid]->GetWithInsert(key).node;
		//node = res.node;
		//newNode = res.hasNewNode;
#if RW_TIME_BKD
		add_time.tree_time+=tree_time.lap();
#endif

		buffer[tableid].node = node;
		buffer[tableid].key = key;
		assert(node != NULL);
	}
#else
	node  = txdb_->tables[tableid]->GetWithInsert(key).node;
	//node = res.node;
	//newNode = res.hasNewNode;

	if(node->value == HAVEREMOVED)
		goto retryA;

#endif
#if RW_TIME_BKD
			tree_time.lap();
#endif

	//1. get the memnode wrapper of the secondary key
	SecondIndex::MemNodeWrapper* mw =
		txdb_->secondIndexes[indextableid]->GetWithInsert(seckey, key, &seq);
	//mw->memnode = node;
#if RW_TIME_BKD
			add_time.tree_time+=tree_time.lap();
#endif
#if RW_TIME_BKD
	set_time.lap();
#endif

	//3. add the seq number of the second node and the validation flag address into the write set
	writeset->Add(seq, mw, node);
#if RW_TIME_BKD
	add_time.set_time+=set_time.lap();
#endif
#if RW_TIME_BKD
	add_time.total_time+=total_time.lap();
#endif

}

//Update a column which has a secondary key
void DBTX::Add(int tableid, int indextableid, uint64_t key, uint64_t seckey, uint64_t* val, int len) {
#if RW_TIME_BKD
	util::timer total_time,tree_time, set_time;
	adds++;
#endif
	bool newNode = false;

retryA:
	uint64_t *seq;

#if PROFILEBUFFERNODE
	bufferGet++;
#endif

	Memstore::MemNode* node;
	//Memstore::InsertResult res;
#if BUFFERNODE
	//Get the seq addr from the hashtable
	if(buffer[tableid].key == key
			&& buffer[tableid].node->value != HAVEREMOVED) {
	#if PROFILEBUFFERNODE
		bufferHit++;
	#endif
		node = buffer[tableid].node;
		assert(node != NULL);
	} else {

	#if PROFILEBUFFERNODE
		bufferMiss++;
	#endif
	#if RW_TIME_BKD
		tree_time.lap();
	#endif
		node = txdb_->tables[tableid]->GetWithInsert(key).node;
		//node = res.node;
		//newNode = res.hasNewNode;
	#if RW_TIME_BKD
		add_time.tree_time+=tree_time.lap();
	#endif
		//assert(node != NULL);
	}
#else
		node = txdb_->tables[tableid]->GetWithInsert(key).node;
		//node = res.node;
		//newNode = res.hasNewNode;
	if(node->value == HAVEREMOVED)
		goto retryA;

#endif
#if RW_TIME_BKD
		tree_time.lap();
#endif

	//1. get the memnode wrapper of the secondary key
	SecondIndex::MemNodeWrapper* mw =
		txdb_->secondIndexes[indextableid]->GetWithInsert(seckey, key, &seq);
#if RW_TIME_BKD
		add_time.tree_time+=tree_time.lap();
#endif
	//mw->memnode = node;
	//2. add the record seq number into write set
#if RW_TIME_BKD
		set_time.lap();
#endif

	char* value = reinterpret_cast<char *>(txdb_->GetEmptyValue(tableid));

	if(value == NULL)
		value = new char[len];

	memcpy(value, val, len);


	//3. add the seq number of the second node and the validation flag address into the write set
		writeset->Add(tableid, key, (uint64_t *)value, node);
		writeset->Add(seq, mw, node);
#if RW_TIME_BKD
		add_time.set_time+=set_time.lap();
#endif

#if RW_TIME_BKD
	add_time.total_time+=total_time.lap();
#endif
}

void DBTX::Delete(int tableid, uint64_t key) {
	uint64_t *val;
	//Logically delete, set the value pointer to be NULL

	Add(tableid, key, (uint64_t *)LOGICALDELETE);
}

void DBTX::PrintKVS(KeyValues* kvs) {
	for(int i = 0; i < kvs->num; i++) {
		printf("KV[%d]: key %lu, value %p \n", i, kvs->keys[i], kvs->values[i]);
	}
}
int DBTX::ScanSecondNode(SecondIndex::SecondNode* sn, KeyValues* kvs) {
	//1.  put the secondary node seq into the readset
	readset->Add(&sn->seq);

	//KVS is NULL because there is no entry in the second node
	if(kvs == NULL)
		return 0;

	//2. get every record and put the record seq into the readset
	int i = 0;
	SecondIndex::MemNodeWrapper* mnw = sn->head;
	while(mnw != NULL) {
		if(mnw->valid == 1 && ValidateValue(mnw->memnode->value)) {
			kvs->keys[i] = mnw->key;
			kvs->values[i] = mnw->memnode->value;
			//put the record seq into the read set
			readset->Add(&mnw->memnode->seq);
			i++;
			//printf("%ld \t", kvs->keys[i]);
			//if (kvs->keys[i] == 0) printf("!!!\n");
		}
		mnw = mnw->next;
	}
	//printf("\n");
	kvs->num = i;
	return i;
}

DBTX::KeyValues* DBTX::GetByIndex(int indextableid, uint64_t seckey) {
	KeyValues* kvs = NULL;

	assert(txdb_->secondIndexes[indextableid] != NULL);
	SecondIndex::SecondNode* sn = txdb_->secondIndexes[indextableid]->Get(seckey);
	assert(sn != NULL);

retryGBI:
	if(kvs != NULL) delete kvs;

	//FIXME: the seq number maybe much larger than the real number of nodes
	uint64_t knum = sn->seq;

	if(knum != 0)
		kvs = new KeyValues(knum);

#if GLOBALOCK
	SpinLockScope spinlock(&slock);
#else
	RTMScope rtm(NULL);
#endif

	//FIXME: the number of node may be changed before the RTM acquired
	if(knum != sn->seq) {
		//while(_xtest())
		//	_xend();
		//printf("[GetByIndex] Error OCCURRED\n");
		goto retryGBI;
	}

	int i = ScanSecondNode(sn, kvs);
	/*	if (i == 0) {
			printf("Empty %ld\n",seckey);
			txdb_->secondIndexes[0]->PrintStore();
		}*/

	if(i > knum) {
		while(_xtest())
			_xend();
		printf("[GetByIndex] Error OCCURRED\n");
	}

	if(i == 0) {
		//printf("[%ld] Get tag2 %ld No Entry\n", pthread_self(), seckey);
		
		if(kvs != NULL)
			delete kvs;
		return NULL;
	}
	return kvs;

}

bool DBTX::ROGet(int tableid, uint64_t key, uint64_t** val, int label) {
	Memstore::MemNode* node;

	node = txdb_->tables[tableid]->GetWithInsert(key).node;

	if(node == NULL) {
		return false;
	}

	if(ValidateValue(node->value)) {
		*val = node->value;
		return true;
	} else {
		*val = NULL;
		return false;
	}
}

bool DBTX::Get(int tableid, uint64_t key, uint64_t** val, int label) {

	bool newNode = false;

	//step 1. First check if the <k,v> is in the write set

	bool found = writeset->Lookup(tableid, key, val);
		
	if(found) {
		if((*val) == LOGICALDELETE)
			return false;
		return true;
	}
	//step 2.  Read the <k,v> from the in memory store
retry:		

	Memstore::MemNode* node;
	Memstore::InsertResult res;

	node = txdb_->tables[tableid]->GetWithInsert(key).node;

	if(node == NULL){
		return false;
	}
	
	//printf("node != NULL\n");
	#if BUFFERNODE
	buffer[tableid].node = node;
	buffer[tableid].key = key;
//	assert(node != NULL);
	#endif

	{
	//Guarantee
		#if GLOBALOCK
		SpinLockScope spinlock(&slock);
		#else
		RTMScope rtm(NULL);
		#endif

		if(node->value == HAVEREMOVED){
			goto retry;
		}
		readset->Add(&node->seq, label);
	}

	//if this node has been removed from the memstore
	//printf("node->value = %x\n", node->value);
	if(ValidateValue(node->value)) {
		*val = node->value;
		return true;
	} else {
		*val = NULL;
		return false;
	}
}

DBTX::Iterator::Iterator(DBTX* tx, int tableid) {
	tx_ = tx;
	table_ = tx->txdb_->tables[tableid];
	iter_ = table_->GetIterator();
	cur_ = NULL;
	prev_link = NULL;
	tableid_ = tableid;
}

bool DBTX::Iterator::Valid() {
	return cur_ != NULL;
}

uint64_t DBTX::Iterator::Key() {
	return iter_->Key();
}

uint64_t* DBTX::Iterator::Value() {
#if BUFFERNODE
	tx_->buffer[tableid_].key = iter_->Key();
	tx_->buffer[tableid_].node = cur_;
#endif
	return val_;
}

void DBTX::Iterator::Next() {
	bool r = iter_->Next();

#if AGGRESSIVEDETECT
	if(!r) {
		tx_->abort = true;
		cur_ = NULL;
		return;
	}
#endif

	while(iter_->Valid()) {
		cur_ = iter_->CurNode();
		{
#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif
			val_ = cur_->value;

			if(prev_link != iter_->GetLink()) {
				tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
				prev_link = iter_->GetLink();
			}
			tx_->readset->Add(&cur_->seq);

			if(DBTX::ValidateValue(val_)) {
				return;
			}
		}

		iter_->Next();

	}
	cur_ = NULL;
}

void DBTX::Iterator::Prev() {

	bool b = iter_->Prev();

	if(!b) {
		tx_->abort = true;
		cur_ = NULL;

		return;
	}
	while(iter_->Valid()) {
		cur_ = iter_->CurNode();
		{

#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif
			val_ = cur_->value;

			tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());

			tx_->readset->Add(&cur_->seq);

			if(DBTX::ValidateValue(val_)) {

				return;
			}
		}

		iter_->Prev();

	}
	cur_ = NULL;

}

void DBTX::Iterator::Seek(uint64_t key) {
	//Should seek from the previous node and put it into the readset
	//printf("I Seek key = %lu\n", key);
	iter_->Seek(key);
	
	cur_ = iter_->CurNode();

	//No keys is equal or larger than key
	if(!iter_->Valid()) {
		assert(cur_ == NULL);
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(NULL);
#endif
		//put the previous node's next field into the readset

		printf("Not Valid!\n");
		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());

		return;
	}

	//Second, find the first key which value is not NULL
	while(iter_->Valid()) {
		{
#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif
//		  printf("before\n");
			//Avoid concurrently insertion
			tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
			//	  printf("end\n");
			//Avoid concurrently modification
			tx_->readset->Add(&cur_->seq);

			val_ = cur_->value;

			if(DBTX::ValidateValue(val_)) {
				return;
			}
		}
		iter_->Next();

		cur_ = iter_->CurNode();
	}
	cur_ = NULL;

}

void DBTX::Iterator::SeekProfiled(uint64_t key) {
	uint64_t start = tx_->rdtsc();
	//Should seek from the previous node and put it into the readset
	iter_->Seek(key);
	cur_ = iter_->CurNode();

	tx_->searchTime += tx_->rdtsc() - start;

	//No keys is equal or larger than key
	if(!iter_->Valid()) {
		assert(cur_ == NULL);
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(NULL);
#endif
		//put the previous node's next field into the readset

		printf("Not Valid!\n");
		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
		return;
	}

	start = tx_->rdtsc();
	//Second, find the first key which value is not NULL
	while(iter_->Valid()) {

		{
#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif
//		  printf("before\n");
			//Avoid concurrently insertion
			tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
			//	  printf("end\n");
			//Avoid concurrently modification
			tx_->readset->Add(&cur_->seq);

			val_ = cur_->value;

			if(DBTX::ValidateValue(val_)) {

				tx_->traverseTime += (tx_->rdtsc() - start);
				return;
			}
			tx_->traverseCount ++;

		}

		iter_->Next();
		cur_ = iter_->CurNode();
	}


	cur_ = NULL;
//	tx_->traverseTime += tx_->rdtsc() - start;
}


// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTX::Iterator::SeekToFirst() {
#if SET_TIME
		util::timer seektofirst_time;
#endif

	//Put the head into the read set first

	iter_->SeekToFirst();

	cur_ = iter_->CurNode();

	if(!iter_->Valid()) {
		assert(cur_ == NULL);
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(NULL);
#endif
		//put the previous node's next field into the readset

		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
#if SET_TIME
		atomic_inc64(&tx_->seektofirsts);
		atomic_add64(&tx_->iterseektofirsttime, seektofirst_time.lap());
#endif

		return;
	}

	while(iter_->Valid()) {

		{

#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif
			val_ = cur_->value;

			tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());

			tx_->readset->Add(&cur_->seq);

			if(DBTX::ValidateValue(val_)) {
#if SET_TIME
				atomic_inc64(&tx_->seektofirsts);
				atomic_add64(&tx_->iterseektofirsttime, seektofirst_time.lap());
#endif

				return;
			}
		}
		iter_->Next();
		cur_ = iter_->CurNode();
	}
	cur_ = NULL;
#if SET_TIME
	atomic_inc64(&tx_->seektofirsts);
	atomic_add64(&tx_->iterseektofirsttime, seektofirst_time.lap());
#endif

}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTX::Iterator::SeekToLast() {
	//TODO
	assert(0);
}

DBTX::SecondaryIndexIterator::SecondaryIndexIterator(DBTX* tx, int tableid) {
	tx_ = tx;
	index_ = tx->txdb_->secondIndexes[tableid];
	iter_ = index_->GetIterator();
	cur_ = NULL;
}

bool DBTX::SecondaryIndexIterator::Valid() {
	return cur_ != NULL;
}


uint64_t DBTX::SecondaryIndexIterator::Key() {
	return iter_->Key();
}

DBTX::KeyValues* DBTX::SecondaryIndexIterator::Value() {
	return val_;
}

void DBTX::SecondaryIndexIterator::Next() {
	bool r = iter_->Next();

#if AGGRESSIVEDETECT
	if(!r) {
		tx_->abort = true;
		cur_ = NULL;
		return;
	}
#endif

	while(iter_->Valid()) {

		cur_ = iter_->CurNode();

		uint64_t knum = cur_->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		{

#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif

			//FIXME: the number of node may be changed before the RTM acquired
			if(knum != cur_->seq) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Next] Error OCCURRED\n");
			}

			int i = tx_->ScanSecondNode(cur_, kvs);

			if(i > knum) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Next] Error OCCURRED\n");
			}

			if(i > 0) {
				val_ = kvs;
				return;
			}
		}

		delete kvs;
		iter_->Next();
	}

	cur_ = NULL;

}

void DBTX::SecondaryIndexIterator::Prev() {
	bool b = iter_->Prev();
	if(!b) {
		tx_->abort = true;
		cur_ = NULL;
		return;
	}


	while(iter_->Valid()) {

		cur_ = iter_->CurNode();

		uint64_t knum = cur_->seq;

		if(knum == 0) {
			iter_->Prev();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		{

#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif

			//FIXME: the number of node may be changed before the RTM acquired
			if(knum != cur_->seq) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Prev] Error OCCURRED\n");
			}

			int i = tx_->ScanSecondNode(cur_, kvs);

			if(i > knum) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Prev] Error OCCURRED\n");
			}

			if(i > 0) {
				val_ = kvs;
				return;
			}
		}

		delete kvs;
		iter_->Prev();
	}

	cur_ = NULL;
}

void DBTX::SecondaryIndexIterator::Seek(uint64_t key) {
	//Should seek from the previous node and put it into the readset
	iter_->Seek(key);
	cur_ = iter_->CurNode();
//	printf("%s\n", (char *)(iter_->Key()+4));

	//No keys is equal or larger than key
	if(!iter_->Valid() && cur_ != NULL) {
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(NULL);
#endif
		//put the previous node's next field into the readset
		readset->Add(&cur_->seq);
		return;
	}

	//Second, find the first key which value is not NULL
	while(iter_->Valid()) {

		cur_ = iter_->CurNode();

		uint64_t knum = cur_->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		{

#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif

			//FIXME: the number of node may be changed before the RTM acquired
			if(knum != cur_->seq) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Next] Error OCCURRED\n");
			}

			int i = tx_->ScanSecondNode(cur_, kvs);

			if(i > knum) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Next] Error OCCURRED\n");
			}

			if(i > 0) {
				val_ = kvs;
				return;
			}
		}

		delete kvs;
		iter_->Next();
	}


	cur_ = NULL;
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTX::SecondaryIndexIterator::SeekToFirst() {
	//Put the head into the read set first

	iter_->SeekToFirst();

	cur_ = iter_->CurNode();
	assert(cur_ != NULL);

	while(iter_->Valid()) {

		uint64_t knum = cur_->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);
		{

#if GLOBALOCK
			SpinLockScope spinlock(&slock);
#else
			RTMScope rtm(NULL);
#endif

			//FIXME: the number of node may be changed before the RTM acquired
			if(knum != cur_->seq) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Next] Error OCCURRED\n");
			}

			int i = tx_->ScanSecondNode(cur_, kvs);

			if(i > knum) {
				while(_xtest())
					_xend();
				printf("[SecondaryIndexIterator::Next] Error OCCURRED\n");
			}

			if(i > 0) {
				val_ = kvs;
				return;
			}
		}

		delete kvs;
		iter_->Next();
		cur_ = iter_->CurNode();
	}
	cur_ = NULL;

}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTX::SecondaryIndexIterator::SeekToLast() {
	//TODO
	assert(0);
}

}  // namespace leveldb
#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
