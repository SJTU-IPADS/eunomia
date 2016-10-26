// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"
#include "port/atomic.h"
#include "db/dbformat.h"
#include "db/dbrotx.h"
#include "db/dbtx.h"
#include "port/port_posix.h"
#include "port/atomic.h"
#include "silo_benchmark/util.h"

#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "db/txmemstore_template.h"

namespace leveldb {

DBROTX::DBROTX(DBTables* store) {
	treetime=0;
	begintime=gettime=endtime=nexttime=seektime=0;
	begins=gets=ends=nexts=seeks=0;
	txdb_ = store;
	oldsnapshot = 0;
// printf("READONLY TX!!!!\n");
}

DBROTX::~DBROTX()
{
	//clear all the data
#if TREE_TIME
	printf("RO total tree_time = %lf sec\n", (double)treetime/MILLION);
#endif

#if RO_TIME_BKD
	printf("ROGET, %ld, ", gets);
	roget_time.display();
	printf("RONEXT, %ld, ", nexts);
	ronext_time.display();
#endif

#if SET_TIME
	//printf("total rotx get_time = %ld(%ld)\n", gettime, gets);
	//printf("total rotx next_time = %ld(%ld)\n", nexttime, nexts);

	/*
	printf("total rotx begin_time = %ld(%ld)\n", begintime, begins);
	printf("total rotx get_time = %ld(%ld)\n", gettime, gets);
	printf("total rotx end_time = %ld(%ld)\n", endtime, ends);
	printf("total rotx next_time = %ld(%ld)\n", nexttime, nexts);
	printf("total rotx prev_time = %ld(%ld)\n", prevtime, prevs);
	printf("total rotx seek_time = %ld(%ld)\n", seektime, seeks);
	*/
	/*
	printf("%ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, %ld, ", 
	begintime, gettime, endtime, nexttime, prevtime, seektime,
	begins, gets, ends, nexts, prevs, seeks);
	*/
#endif
}

void DBROTX::Begin() {

//fetch and increase the global snapshot counter
	txdb_->RCUTXBegin();

#if GLOBALOCK
	SpinLockScope slock(&DBTX::slock);
#else
	RTMScope rtm(NULL);
#endif
	//oldsnapshot = atomic_fetch_and_add64(&(txdb_->snapshot), 1);
	oldsnapshot = txdb_->snapshot;
	txdb_->snapshot++;

	//printf("snapshot %ld\n", txdb_->snapshot);
}

bool DBROTX::Abort() {
	txdb_->RCUTXEnd();
	return false;
}

bool DBROTX::End() {

	txdb_->GCDeletedNodes();

	txdb_->RCUTXEnd();

#if RCUGC
	txdb_->GC();
	txdb_->DelayRemove();
#endif

	return true;
}

bool DBROTX::ScanMemNode(Memstore::MemNode* n, uint64_t** val) {
	if(n == NULL) {
		//printf("ScanMemNode: Scan NULL\n");
		return false;
	}
	if(n->counter <= oldsnapshot) {
		if(DBTX::ValidateValue(n->value)) {
			*val = n->value;
			return true;
		} else {
			return false;
		}
	}

	n = n->oldVersions;
	while(n != NULL && n->counter > oldsnapshot) {
		n = n->oldVersions; //get the older version (earlier than the current version)
	}

	if(n != NULL && n->counter <= oldsnapshot) {
		if(DBTX::ValidateValue(n->value)) {
			*val = n->value;
			return true;
		} else {
			return false;
		}
	}
	//printf("ScanMemNode: No snap \n");
	return false;
}


//This function should be executed atomically
inline bool DBROTX::GetValueOnSnapshot(Memstore::MemNode* n, uint64_t** val) {

#if GLOBALOCK
	SpinLockScope slock(&DBTX::slock);
#else
	RTMScope rtm(NULL);
#endif
	return ScanMemNode(n, val);
}

bool DBROTX::GetValueOnSnapshotByIndex(SecondIndex::SecondNode* sn, KeyValues* kvs) {

#if GLOBALOCK
	SpinLockScope slock(&DBTX::slock);
#else
	RTMScope rtm(NULL);
#endif

	SecondIndex::MemNodeWrapper* mnw = sn->head;
	int i = 0;
	while(mnw != NULL) {
		uint64_t *val = NULL;
		if(ScanMemNode(mnw->memnode, &val)) {
			kvs->keys[i] = mnw->key;
			kvs->values[i] = val;
			i++;
		}
		mnw = mnw->next;
	}

	if(i > 0) {
		kvs->num = i;
		return true;
	}

	return false;

}

bool DBROTX::Get(int tableid, uint64_t key, uint64_t** val) {
#if RO_TIME_BKD
	util::timer get_time,tree_time,ov_time;
	gets++;
#endif
	Memstore::MemNode* n = txdb_->tables[tableid]->Get(key);
#if RO_TIME_BKD
	roget_time.tree_time += tree_time.lap();
#endif
#if RO_TIME_BKD
	ov_time.lap();
#endif
	bool res = GetValueOnSnapshot(n, val);
#if RO_TIME_BKD
	roget_time.ov_time+= ov_time.lap();
#endif
#if RO_TIME_BKD
	roget_time.total_time+= get_time.lap();
#endif

	return res;
}

DBROTX::Iterator::Iterator(DBROTX* rotx, int tableid) {
	rotx_ = rotx;
	iter_ = rotx->txdb_->tables[tableid]->GetIterator();
	cur_ = NULL;
	val_ = NULL;
}

bool DBROTX::Iterator::Valid() {
	return cur_ != NULL;
}


uint64_t DBROTX::Iterator::Key() {
	return iter_->Key();
}

uint64_t* DBROTX::Iterator::Value() {
	//return cur_->value;
	return val_;
}

void DBROTX::Iterator::Next() {
#if RO_TIME_BKD
	util::timer tree_time, next_time, ov_time;
	rotx_->nexts++;
#endif
	iter_->Next();
#if RO_TIME_BKD
	rotx_->ronext_time.tree_time+= tree_time.lap();
#endif

	while(iter_->Valid()) {
		//if (iter_->CurNode()->counter > rotx_->oldsnapshot) break;
#if RO_TIME_BKD
		ov_time.lap();
#endif
		bool res = rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_);
#if RO_TIME_BKD
		rotx_->ronext_time.ov_time += ov_time.lap();
#endif

		if(res) {
#if RO_TIME_BKD
			tree_time.lap();
#endif
			cur_ = iter_->CurNode();
#if RO_TIME_BKD
			rotx_->ronext_time.tree_time += tree_time.lap();
#endif

#if RO_TIME_BKD
			rotx_->ronext_time.total_time += next_time.lap();
#endif
			return;
		}
#if RO_TIME_BKD
		tree_time.lap();
#endif
		iter_->Next();
#if RO_TIME_BKD
		rotx_->ronext_time.tree_time += tree_time.lap();
#endif

	}
	cur_ = NULL;
#if RO_TIME_BKD
	rotx_->ronext_time.total_time+= next_time.lap();
#endif

}

void DBROTX::Iterator::Prev() {
#if TREE_TIME
		util::timer tree_time;
#endif

	while(iter_->Valid()) {
#if TREE_TIME
		tree_time.lap();
#endif
		iter_->Prev();
#if TREE_TIME
		atomic_add64(&rotx_->treetime, tree_time.lap());
#endif

		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
			cur_ = iter_->CurNode();

			return;
		}
	}
	cur_ = NULL;

}

void DBROTX::Iterator::Seek(uint64_t key) {
#if TREE_TIME
			util::timer tree_time;
#endif

	iter_->Seek(key);
#if TREE_TIME
			atomic_add64(&rotx_->treetime, tree_time.lap());
#endif

	while(iter_->Valid()) {
		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
			cur_ = iter_->CurNode();

			return;
		}
#if TREE_TIME
				tree_time.lap();
#endif

		iter_->Next();
#if TREE_TIME
			atomic_add64(&rotx_->treetime, tree_time.lap());
#endif

	}

}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::Iterator::SeekToFirst() {
	iter_->SeekToFirst();
	while(iter_->Valid()) {

		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
			cur_ = iter_->CurNode();
			return;
		}
		iter_->Next();
	}

}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::Iterator::SeekToLast() {
	//TODO
	assert(0);
}

DBROTX::SecondaryIndexIterator::SecondaryIndexIterator(DBROTX* rotx, int tableid) {
	rotx_ = rotx;
	index_ = rotx_->txdb_->secondIndexes[tableid];
	iter_ = index_->GetIterator();
	cur_ = NULL;
	val_ =  NULL;
}

bool DBROTX::SecondaryIndexIterator::Valid() {
	return cur_ != NULL;
}


uint64_t DBROTX::SecondaryIndexIterator::Key() {
	return iter_->Key();
}

DBROTX::KeyValues* DBROTX::SecondaryIndexIterator::Value() {
	return val_;
}

void DBROTX::SecondaryIndexIterator::Next() {
	iter_->Next();
	while(iter_->Valid()) {

		uint64_t knum = iter_->CurNode()->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
			cur_ = iter_->CurNode();
			val_ = kvs;
			return;
		}

		delete kvs;
		iter_->Next();
	}

	cur_ = NULL;

}

void DBROTX::SecondaryIndexIterator::Prev() {
	iter_->Prev();
	while(iter_->Valid()) {

		uint64_t knum = iter_->CurNode()->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
			cur_ = iter_->CurNode();
			val_ = kvs;
			return;
		}

		delete kvs;
		iter_->Prev();
	}

	cur_ = NULL;

}


void DBROTX::SecondaryIndexIterator::Seek(uint64_t key) {
	iter_->Seek(key);
	while(iter_->Valid()) {

		uint64_t knum = iter_->CurNode()->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
			cur_ = iter_->CurNode();
			val_ = kvs;
			return;
		}

		delete kvs;
		iter_->Next();
	}
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::SecondaryIndexIterator::SeekToFirst() {
	iter_->SeekToFirst();
	while(iter_->Valid()) {

		uint64_t knum = iter_->CurNode()->seq;

		if(knum == 0) {
			iter_->Next();
			continue;
		}

		KeyValues* kvs = new KeyValues(knum);

		if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
			cur_ = iter_->CurNode();
			val_ = kvs;
			return;
		}

		delete kvs;
		iter_->Next();
	}

}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::SecondaryIndexIterator::SeekToLast() {
	//TODO
	assert(0);
}


}  // namespace leveldb

