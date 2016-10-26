#include "port/atomic.h"
#include "objpool.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

OBJPool::OBJPool() {
	gclists_ = NULL;
	curlist_ = NULL;

	freenum_ = 0;
	freelist_ = NULL;

	debug = false;
}

OBJPool::~OBJPool() {
	while(gclists_ != NULL) {
		Header* cur = gclists_;
		gclists_ = gclists_->next;
		FreeList(cur);
		delete cur;
	}
}

void OBJPool::AddGCObj(char* gobj, uint64_t sn) {
//	printf("AddGCObj %lx\n", gobj);
	Obj* o = (Obj*)(gobj - sizeof(Obj));

	if(curlist_ == NULL) {

		assert(gclists_ == NULL);
		gclists_ = curlist_ = new Header();
		curlist_->sn = sn;

	} else if(curlist_->sn != sn) {

		assert(curlist_->sn < sn);

		curlist_->next = new Header();
		curlist_ = curlist_->next;
		curlist_->sn = sn;
	}


	if(NULL == curlist_->tail) {
		curlist_->tail = o;
		assert(NULL == curlist_->head);
	}

	o->next = curlist_->head;
	curlist_->head = o;

	curlist_->gcnum++;

}


#if 0
char* OBJPool::GetFreeObj() {
	if(0 == freenum_)
		return NULL;

	assert(freenum_ > 0);


	Obj* r = freelist_;

	freelist_ = freelist_->next;
	freenum_--;

	r->next = NULL;
	char *val = (char *)&r->value[0];
//	printf("GetFreeObj %lx %lx\n", r, val);
	return val;
}
#endif

void OBJPool::GC(uint64_t safesn) {
	if(gclists_ == NULL) {
		assert(curlist_ == NULL);
		return;
	}

	while(gclists_ != NULL) { //&& gclists_->sn <= safesn) {
		Header* cur = gclists_;
		gclists_ = gclists_->next;
		if(cur == curlist_) {
			curlist_ = NULL;
			assert(gclists_ == NULL);
		}
		GCList(cur);
		delete cur;
	}
}

void OBJPool::GCList(Header* list) {
	assert(list->head != NULL);

	list->tail->next = freelist_;
	freelist_ = list->head;
	freenum_ += list->gcnum;
}

void OBJPool::FreeList(Header* list) {
	assert(list != NULL);
	int i = 0;
	while(NULL != list->head) {
		Obj* o = list->head;
		list->head = list->head->next;
		free(o);
	}
}

void OBJPool::Print() {
	printf("==================GC List=======================\n");

	int i = 0;
	Header* cur = gclists_;
	while(cur  != NULL) {
		cur = cur->next;
		i++;
		printf("Header [%d] cur %p elems %d sn %lu\n",
			   i, cur, cur->gcnum, cur->sn);
	}
	/*
		printf("==================Free List=======================\n");
		cur = freelist_;
		while(cur != NULL) {
			printf("Cur %lx Next %lx\n", cur, cur->next);
			cur = cur->next;
		}*/
}
