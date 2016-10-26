/*
 * Algorithm 2 corresponds to GetWithInsert(uint64_t key) function.
 * Algorithm 3 corresponds to ShuffleLeafInsert(uint64_t key, LeafNode *leaf, MemNode** val, uint64_t* upKey, bool insert_only) function.
 * In order to clarify the correctness of our algorithm, we add explanation in related functions with reference to our pseudocode.
 */
#ifndef MEMSTOREEUNOTREE_H
#define MEMSTOREEUNOTREE_H

#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <utmpx.h>
#include "util/rtmScope.h"
#include "util/rtm.h"
#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "util/numa_util.h"
#include "util/statistics.h"
#include "util/bloomfilter.h"
#include "util/ccm.h"
#include "port/port_posix.h"
#include "memstore.h"
#include "silo_benchmark/util.h"
//#define LEAF_NUM 15
#define N  15

//#define SEG_NUM 2
//#define SEG_LEN 7
//#define LEAF_NUM (SEG_NUM*SEG_LEN)

#define BTREE_PROF 0
#define BTREE_LOCK 0
#define BTPREFETCH 0
#define DUMMY 1

#define NODEMAP  0
#define NODEDUMP 0
#define KEYDUMP  0
#define KEYMAP   0
#define NUMADUMP 0

#define REMOTEACCESS 0
#define LEVEL_LOG 0

#define BUFFER_TEST 0
#define BUFFER_LEN (1<<8)
#define HASH_MASK (BUFFER_LEN-1)
#define OFFSET_BITS 4

#define BM_TEST 0
#define FLUSH_FREQUENCY 100

#define SHUFFLE_KEYS 0

#define ORIGIN_INSERT 0
#define SHUFFLE_INSERT 1
#define UNSORTED_INSERT 0

#define SEGS     4
#define EMP_LEN  4
#define HAL_LEN  (EMP_LEN >> 1)
#define LEAF_NUM (SEGS*EMP_LEN)

#define CONTENTION_LEVEL 5
#define FEW_THREADS 2

#define ADAPTIVE_LOCK 1
#define DEFAULT_LOCK 0
#define BM_QUERY 1
#define BM_PROF 0

#define SPEC_PROF 0

#define DUP_PROF 0

#define SPLIT_DEPTH 3
#define NO_SEQ_NO 0

#define ERROR_RATE 0.05
#define BM_SIZE 16

#define TIME_BKD 0
#define THRESHOLD 2

using namespace std;
namespace leveldb {

struct access_log {
	uint64_t gets;
	uint64_t writes;
	uint64_t splits;
};

struct key_log {
	uint64_t gets;
	uint64_t writes;
	uint64_t dels;
};

static uint32_t leaf_id = 0;
static int32_t inner_id = 0;
static uint32_t table_id = 0;
#define CACHE_LINE_SIZE 64

class MemstoreEunoTree: public Memstore {
//Test purpose
public:
	inline void set_thread_num(unsigned thread) {
		thread_num = thread;
	}
	int tableid;
	uint64_t insert_seq;

	uint64_t rconflict = 0;
	uint64_t wconflict = 0;

	uint64_t spec_hit;
	uint64_t spec_miss;

//	uint64_t inserts;
	uint64_t shifts;

	uint64_t half_born;
	uint64_t empty_born;

	uint64_t should_protect;
	uint64_t should_not_protect;

	uint64_t leaf_rightmost;
	uint64_t leaf_not_rightmost;

	bool first_leaf;

	uint64_t insert_time;

	uint64_t insert_times[SEGS + 1];
	uint64_t inserts[SEGS + 1];

	//uint64_t leaf_splits;
	uint64_t duplicate_keys;
	uint64_t dup_found, leaf_inserts, leaf_splits;
	//uint64_t inner_splits,  leaf_splits;
	uint64_t original_inserts, scope_inserts;

	uint64_t stage_1_time, bm_time, stage_2_time;

	uint64_t inner_rightmost, inner_not_rightmost;

	uint64_t bm_found_num, bm_miss_num;

	uint64_t consist, inconsist;
	//uint64_t bm_time;
	uint64_t node_difference;
	uint64_t node_inserts;
	bool contention_mode;

	SpinLock bm_lock;
	struct KeyValue {
		uint64_t key;
		MemNode* value;
	};

	struct {
		bool operator()(const struct KeyValue& a, const struct KeyValue& b) {
			return a.key < b.key;
		}
	} KVCompare;

#if SHUFFLE_INSERT
	struct Leaf_Seg {
		//key-value stored in the leaf segment
		//KeyValue kvs[EMP_LEN];
		KeyValue *kvs;
		//uint64_t paddings[4];
		unsigned max_room;
		//MemNode* vals[EMP_LEN];
		unsigned key_num;
		//cacheline alignment
		uint64_t paddings1[7];
		Leaf_Seg() {
			kvs = new KeyValue[EMP_LEN];
		}
		Leaf_Seg(unsigned multiple) {
			kvs = new KeyValue[HAL_LEN];
		}
		inline bool isFull() {
			return key_num >= max_room;
		}
		inline unsigned kvs_len() {
			return max_room;
		}
		inline void shrink() {
			delete[] kvs;
			kvs = new KeyValue[max_room];
		}
		~Leaf_Seg() {
			delete[] kvs;
		}
	};

	struct Insert_Log {
		Insert_Log(): one_try(0), two_try(0), check_all(0), split(0) {}
		uint64_t one_try;
		uint64_t two_try;
		uint64_t check_all;
		uint64_t split;
	};

	Insert_Log insert_log;
#endif
	struct InnerNode; //forward declaration

	struct LeafNode {
		LeafNode() : num_keys(0), bm_filter(NULL), seq(0), reserved(NULL), kvs_num(0), ccm(new CCM()) {

		}
		LeafNode(unsigned multiple) : num_keys(0), bm_filter(NULL), seq(0), reserved(NULL), kvs_num(0), ccm(new CCM()) {
			kvs = new KeyValue[8];
			kvs_size = 8;
			for(int i = 0; i < SEGS; i++) {
				leaf_segs[i] = Leaf_Seg(multiple);
			}
		}

#if SHUFFLE_INSERT
		/*leaf segments*/
		Leaf_Seg leaf_segs[SEGS];
		uint64_t paddings[8];//cacheline alignment
		SpinLock mlock;
#endif
		/*reserved keys*/
		KeyValue *kvs;
		unsigned kvs_num;
		unsigned kvs_size;
		BloomFilter* bm_filter;
		KeyValue* reserved;
		CCM* ccm;

		unsigned num_keys;
		LeafNode *left;
		LeafNode *right;
		InnerNode* parent;
		uint64_t seq;
		//unsigned last_thread_id;
	public:
		inline int total_keys() {
			int total_keys = 0;
			for(int i = 0 ; i < SEGS; i++) {
				total_keys += leaf_segs[i].key_num;
			}
			return total_keys;
		}

		/*move the keys from segments to reserved keys*/
		void moveToReserved() {
			init_reserved_keys();
			/*move the keys to reserved room*/
			int temp_idx = kvs_num;

			unsigned i = 0;
			unsigned num_ = total_keys() + kvs_num;
			while((num_keys >> i) > kvs_size) {
				i++;
			}
			if(i > 0) {
				expand(i);
			}
			for(int i = 0; i < SEGS; i++) {
				for(int j = 0; j < leaf_segs[i].key_num; j++) {
					kvs[temp_idx] = leaf_segs[i].kvs[j];
					temp_idx++;
					kvs_num++;
				}
				leaf_segs[i].key_num = 0;
			}
		}

		/*insert new keys to the segments*/
		MemNode* insertSegment(int idx, uint64_t key) {
			leaf_segs[idx].kvs[leaf_segs[idx].key_num].key = key;
			MemNode* reMem = NULL;

#if DUMMY
			leaf_segs[idx].kvs[leaf_segs[idx].key_num].value = dummyval_;
			reMem = dummyval_;
#else
			leaf_segs[idx].kvs[leaf_segs[idx].key_num].value = GetMemNode();
			reMem = leaf_segs[idx].kvs[leaf_segs[idx].key_num].value ;
#endif
			assert(reMem != NULL);

			leaf_segs[idx].key_num++;
			num_keys++;
			dummyval_ = NULL;
			return reMem;
		}

		inline bool hasRoom() {
			unsigned total_seg = num_keys - kvs_num;
			unsigned upper = (leaf_segs[0].max_room * SEGS) >> 1;
			return (num_keys < LEAF_NUM) && (total_seg <= upper);
		}

		/*shrink the capacity of segments*/
		void shrinkSegs() {
			if(leaf_segs[0].max_room > 2) {
				for(int i = 0; i < SEGS; i++) {
					leaf_segs[i].max_room = leaf_segs[i].max_room >> 1;
					leaf_segs[i].shrink();
				}
			}
		}

		inline bool isFull() {
			return num_keys >= LEAF_NUM;
		}

		inline bool needShrink() {
			return kvs_num < kvs_size << 2;
		}

		/*expand the length of reserved keys*/
		void expand(unsigned multiple) {
			kvs_size = kvs_size << multiple;
			KeyValue *temp = new KeyValue[kvs_size];
			for(int i = 0; i < kvs_num; i++) {
				temp[i] = kvs[i];
			}
			delete[] kvs;
			kvs = temp;
		}

		/*shrink the length of reserved keys*/
		void shrink() {
			if(kvs_size >> 1 >= kvs_num) {
				kvs_size = kvs_size >> 1;

				KeyValue *temp = new KeyValue[kvs_size];
				for(int i = 0; i < kvs_num; i++) {
					temp[i] = kvs[i];
				}
				delete[] kvs;
				kvs = temp;
			}
		}

		inline void init_reserved_keys() {
			if(kvs_size == 0) {
				kvs = new KeyValue[8];
				kvs_size = 8;
			}
		}
	};

	struct InnerNode {
		InnerNode() : num_keys(0) {}
		uint64_t 	 keys[N];
		void*	 children[N + 1];

		unsigned num_keys;
		InnerNode* parent;
	};

	//The result object of the delete function
	struct DeleteResult {
		DeleteResult(): value(0), freeNode(false), upKey(-1) {}
		Memstore::MemNode* value;  //The value of the record deleted
		bool freeNode;	//if the children node need to be free
		uint64_t upKey; //the key need to be updated -1: default value
	};

	class Iterator: public Memstore::Iterator {
	public:
		// Initialize an iterator over the specified list.
		// The returned iterator is not valid.
		Iterator() {};
		Iterator(MemstoreEunoTree* tree);

		// Returns true iff the iterator is positioned at a valid node.
		bool Valid();

		// Returns the key at the current position.
		// REQUIRES: Valid()
		MemNode* CurNode();

		uint64_t Key();

		// Advances to the next position.
		// REQUIRES: Valid()
		bool Next();

		// Advances to the previous position.
		// REQUIRES: Valid()
		bool Prev();

		// Advance to the first entry with a key >= target
		void Seek(uint64_t key);

		void SeekPrev(uint64_t key);

		// Position at the first entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToFirst();

		// Position at the last entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToLast();

		uint64_t* GetLink();

		uint64_t GetLinkTarget();

	private:
		MemstoreEunoTree* tree_;
		LeafNode* node_;
		uint64_t seq_;
		int leaf_index;
		uint64_t *link_;
		uint64_t target_;
		uint64_t key_;
		MemNode* value_;
		uint64_t snapshot_;
	};

public:
	MemstoreEunoTree() {
		insert_seq = 0;

		depth = 0;
#if BTREE_PROF
		writes = 0;
		reads = 0;
		calls = 0;
#endif
	}

	MemstoreEunoTree(int _tableid) {
		insert_seq = 0;
		first_leaf = true;
		depth = 0;

		tableid = _tableid;
		insert_time = 0;
		spec_hit = spec_miss = 0;
		duplicate_keys = 0;
		dup_found = leaf_splits = leaf_inserts = 0;
		//inner_splits = leaf_splits = 0;
		original_inserts = scope_inserts = 0;
		stage_1_time = stage_2_time = bm_time = 0;
		consist = inconsist = 0;
		node_difference = 0;
		node_inserts = 0;
		contention_mode = true;

		leaf_rightmost = leaf_not_rightmost = 0;
#if BTREE_PROF
		writes = 0;
		reads = 0;
		calls = 0;
#endif
	}

	~MemstoreEunoTree() {

#if SPEC_PROF
		printf("spec_hit = %lu, spec_miss = %lu\n", spec_hit, spec_miss);
#endif

#if BTREE_PROF
		printf("calls %ld avg %f writes %f\n", calls, (float)(reads + writes) / (float)calls, (float)(writes) / (float)calls);
#endif
	}

	void transfer_para(RTMPara& para) {
		prof.transfer_para(para);
	}

	inline void ThreadLocalInit() {
		if(false == localinit_) {
			arena_ = new RTMArena();
			dummyval_ = GetMemNode();
			dummyval_->value = NULL;
			dummyleaf_ = new LeafNode();
			dummyleaf_half = new LeafNode(1);
			localinit_ = true;
		}
	}

	inline LeafNode* new_leaf_node() {
#if DUMMY
		LeafNode* result = dummyleaf_;
		dummyleaf_ = NULL;
#else
		LeafNode* result = new LeafNode();
#endif
		return result;
	}

	inline LeafNode* new_leaf_node(unsigned multiple) {
#if DUMMY
		LeafNode* result = dummyleaf_half;
		dummyleaf_half = NULL;
#else
		LeafNode* result = new LeafNode(multiple);
#endif
		return result;
	}

	inline InnerNode* new_inner_node() {
		InnerNode* result = new InnerNode();
		return result;
	}

	inline LeafNode* FindLeaf(uint64_t key) {
		InnerNode* inner;
		register void* node = root;
		register unsigned d = depth;
		unsigned index = 0;
		while(d-- != 0) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			node = inner->children[index];

		}
		return reinterpret_cast<LeafNode*>(node);
	}

	inline InnerNode* SpecInner(uint64_t key) {
		InnerNode* inner;
		register void* node = root;
		register unsigned d = depth;
		unsigned index = 0;
		if(d == 0) {
			return reinterpret_cast<InnerNode*>(node);
		}
		while(d-- != 1) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			node = inner->children[index];
		}
		return reinterpret_cast<InnerNode*>(node);
	}

	//return a memNode if the key is found in one of leaf node
	inline MemNode* FindKeyInLeaf(LeafNode* targetLeaf, uint64_t key) {
		//printf("FindKeyInLeaf key = %lu\n", key);
		//dump_leaf(targetLeaf);
		for(int i = 0; i < targetLeaf->kvs_num; i++) {
			if(targetLeaf->kvs[i].key == key) {
				return targetLeaf->kvs[i].value;;
			}
		}
		for(int i = 0; i < SEGS; i++) {
			//printf("leaf->leaf_segs[%d].max_room = %lu\n",i,leaf->leaf_segs[i].max_room);
			for(int j = 0; j < targetLeaf->leaf_segs[i].key_num; j++) {
				//printf("key = %lu, targetkey = %lu\n",targetLeaf->leaf_segs[i].kvs[j].key, key);
				if(targetLeaf->leaf_segs[i].kvs[j].key == key) {
					return targetLeaf->leaf_segs[i].kvs[j].value;
				}
			}
		}
		return NULL;
	}

	/*
	 *Get the MemNode of a key.
	 *Return: the pointer to the MemNode corresponding to the key, NULL if the key
	 *does not exist.
	 */
	inline MemNode* Get(uint64_t key) {
		if(root == NULL) return NULL;
		LeafNode* targetLeaf = NULL;

TOP_RETRY:
		uint64_t seqno = 0;
		{
			RTMScope begtx(&prof, depth * 2, 1, &rtmlock, GET_TYPE);
			ScopeFind(key, &targetLeaf);
			seqno = targetLeaf->seq;
			//targetLeaf->ccm->add_conflict_num(begtx.getRetry());
		}
		targetLeaf->ccm->add_operation_num();
		unsigned int slot = targetLeaf->ccm->getIndex(key);

		/*lock the read-write lock in CCM*/
		targetLeaf->ccm->read_Lock(slot);

#if BM_QUERY
		/*check if the key is in the leaf node*/
		if(!targetLeaf->ccm->skipBF()) {
			targetLeaf->mlock.Lock();
			bool bm_found = queryBMFilter(targetLeaf, key, slot, 1);
			targetLeaf->mlock.Unlock();

			if(!bm_found) {
				targetLeaf->ccm->read_Unlock(slot);
				return NULL;
			}
		}
#endif

		MemNode* res = NULL;
		bool consistent = true;

		{
			RTMScope begtx(&prof, depth * 2, 1, &rtmlock, GET_TYPE);

			/*check the sequential number for consistency*/
			if(targetLeaf->seq == seqno) {
				res = FindKeyInLeaf(targetLeaf, key);
			} else {
				consistent = false;
			}
		}
		/*if the consistency check fails, it should retry from the very beginning*/
		if(!consistent) {
			targetLeaf->ccm->read_Unlock(slot);
			goto TOP_RETRY;
		}

		/*unlock the read-write lock in CCM */
		targetLeaf->ccm->read_Unlock(slot);
		return res;
	}

	/*
	 *Put a new value to a targe key.
	 *Return: pointer to the target MemNode.
	 */
	inline MemNode* Put(uint64_t k, uint64_t* val) {
		ThreadLocalInit();
		MemNode *node = GetWithInsert(k).node;
		if(node == NULL) {
			printf("k = %lu return NULL\n", k);
		}
		node->value = val;
#if BTREE_PROF
		reads = 0;
		writes = 0;
		calls = 0;
#endif
		return node;
	}

	inline int slotAtLeaf(uint64_t key, LeafNode* cur) {
		int slot = 0;
		while((slot < cur->num_keys) && cur->kvs[slot].key != key) {
			slot++;
		}
		return slot;
	}

	inline Memstore::MemNode* removeLeafEntry(LeafNode* cur, int slot) {
		assert(slot < cur->num_keys);
		cur->seq = cur->seq + 1;
		Memstore::MemNode* value = cur->kvs[slot].value;
		cur->num_keys--; //num_keys subtracts one
		/*The key deleted is the last one*/
		if(slot == cur->num_keys)
			return value;
		/*Re-arrange the entries in the leaf*/
		for(int i = slot + 1; i <= cur->num_keys; i++) {
			cur->kvs[i - 1] = cur->kvs[i];
			//cur->values[i - 1] = cur->values[i];
		}
		return value;
	}

	inline DeleteResult* LeafDelete(uint64_t key, LeafNode* cur) {
		/*step 1. find the slot of the key*/
		int slot = slotAtLeaf(key, cur);
		/*the record of the key doesn't exist, just return*/
		if(slot == cur->num_keys) {
			return NULL;
		}
		DeleteResult *res = new DeleteResult();

		/*step 2. remove the entry of the key, and get the deleted value*/
		res->value = removeLeafEntry(cur, slot);

		/*step 3. if node is empty, remove the node from the list*/
		if(cur->num_keys == 0) {
			if(cur->left != NULL)
				cur->left->right = cur->right;
			if(cur->right != NULL)
				cur->right->left = cur->left;

			/*Parent is responsible for the node deletion*/
			res->freeNode = true;
			return res;
		}
		/*The smallest key in the leaf node has been changed, update the parent key*/
		if(slot == 0) {
			res->upKey = cur->kvs[0].key;
		}
		return res;
	}

	inline int slotAtInner(uint64_t key, InnerNode* cur) {
		int slot = 0;
		while((slot < cur->num_keys) && (cur->keys[slot] <= key)) {
			slot++;
		}
		return slot;
	}

	inline void removeInnerEntry(InnerNode* cur, int slot, DeleteResult* res) {
		assert(slot <= cur->num_keys);
		/*If there is only one available entry*/
		if(cur->num_keys == 0) {
			assert(slot == 0);
			res->freeNode = true;
			return;
		}
		/*The key deleted is the last one*/
		if(slot == cur->num_keys) {
			cur->num_keys--;
			return;
		}
		/*rearrange the children slot*/
		for(int i = slot + 1; i <= cur->num_keys; i++)
			cur->children[i - 1] = cur->children[i];
		/*delete the first entry, upkey is needed*/
		if(slot == 0) {
			/*record the first key as the upkey*/
			res->upKey = cur->keys[slot];
			/*delete the first key*/
			for(int i = slot; i < cur->num_keys - 1; i++) {
				cur->keys[i] = cur->keys[i + 1];
			}
		} else {
			/*delete the previous key*/
			for(int i = slot; i < cur->num_keys; i++) {
				cur->keys[i - 1] = cur->keys[i];
			}
		}
		cur->num_keys--;
	}

	inline DeleteResult* InnerDelete(uint64_t key, InnerNode* cur , int depth) {
		DeleteResult* res = NULL;
		/*step 1. find the slot of the key*/
		int slot = slotAtInner(key, cur);

		/*
		 *step 2. remove the record recursively
		 *This is the last level of the inner nodes
		 */
		if(depth == 1) {
			res = LeafDelete(key, (LeafNode *)cur->children[slot]);
		} else {
			res = InnerDelete(key, (InnerNode *)cur->children[slot], (depth - 1));
		}
		/*The record is not found*/
		if(res == NULL) {
			return res;
		}
		/*step 3. Remove the entry if the TOTAL children nodes have been removed*/
		if(res->freeNode) {
			res->freeNode = false;
			removeInnerEntry(cur, slot, res);
			return res;
		}
		/*step 4. update the key if needed*/
		if(res->upKey != -1) {
			if(slot != 0) {
				cur->keys[slot - 1] = res->upKey; //the upkey should be updated
				res->upKey = -1;
			}
		}
		return res;
	}

	inline Memstore::MemNode* Delete_rtm(uint64_t key) {
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock, DEL_TYPE);
#endif
		DeleteResult* res = NULL;
		if(depth == 0) {
			//Just delete the record from the root
			res = LeafDelete(key, (LeafNode*)root);
		} else {
			res = InnerDelete(key, (InnerNode*)root, depth);
		}

		if(res == NULL)
			return NULL;

		if(res->freeNode)
			root = NULL;

		return res->value;
	}

	inline Memstore::MemNode* GetWithDelete(uint64_t key) {
		ThreadLocalInit();
		MemNode* value = Delete_rtm(key);
#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
		if(dummyleaf_half == NULL) {
			dummyleaf_half = new LeafNode(1);
		}
#endif
		return value;
	}

#if ADAPTIVE_LOCK
	/*
	 *Decide whether to lock the leaf accroding to conflict rate.
	 *Here we basically judge from the number of conflicts happened in each leaf node.
	 */
	inline bool ShouldLockLeaf(LeafNode* leaf) {
		return leaf->ccm->is_conflict();
	}
#endif

	/*
	 * This function queries the Conflict Control Module (CCM).
	 * Return: Whether a key exists in a certain leaf node, and set the corresponding
	 * mark bits.
	 */
	inline bool queryBMFilter(LeafNode* leafNode, uint64_t this_key, unsigned int slot, bool get) {
		bool bm_found = false;
		if(leafNode->ccm->mark_bits == 0) {
			bm_found = true;
			for(int i = 0; i < leafNode->kvs_num; i++) {
				uint64_t key = leafNode->kvs[i].key;
				unsigned int slot_temp = leafNode->ccm->getIndex(key);
				leafNode->ccm->set_mark_bits(key, slot_temp);
			}
			for(int i = 0; i < SEGS; i++) {
				for(int j = 0; j < leafNode->leaf_segs[i].key_num; j++) {
					uint64_t key = leafNode->leaf_segs[i].kvs[j].key;
					unsigned int slot_temp = leafNode->ccm->getIndex(key);
					leafNode->ccm->set_mark_bits(key, slot_temp);
				}
			}
		} else {
			if(leafNode->ccm->isfound(this_key))
				bm_found = true;
		}
		if(!get)
			leafNode->ccm->set_mark_bits(this_key, slot);
		return bm_found;
	}

	/*
	 * This function provides the PUT interface of Eunomia Tree
	 * If the inserted key exists in tree, it will return the pointer to its
	 * correspoding value; otherwise it will insert a new key and handle the
	 * split operation if any.
	 *
	 * It corresponds to Algorithm 2 in the Paper. The algorithm here is split into
	 * four steps, each corresponds to one step in the pseudocode of Algorithm 2.
	 *
	 * Return: pointer to the value corresponding to the inserted key
	 */
	inline Memstore::InsertResult GetWithInsert(uint64_t key) {
		bool should_sample = false;
		should_sample = key % 10 == 0;
		int cpu_id = sched_getcpu();
		ThreadLocalInit();
		LeafNode* target_leaf = NULL;

		MemNode* res = NULL;
		bool rtm = true;
		/*disable the two-step HTM region when tree size is small (depth < CONTENTION_LEVEL),
		 * or the thread number is small.*/
		if(depth < CONTENTION_LEVEL || thread_num <= FEW_THREADS) {
			res = Insert_rtm(key, &target_leaf, should_sample);
		} else {
			//scope_inserts++;
			LeafNode* leafNode = NULL;
			MemNode* memNode = NULL;
			int temp_depth;
			bool bm_found = true;
			bool locked = false;
			uint64_t this_key = key;
			uint64_t seqno = 0;
			int conflict_num = 0;

			/*
			 *Step 1: Upper region of two-step HTM region
			 *Corresponding to Line 3-6 in Algorithm 2.
			 */
TOP_RETRY:  {
				/*the beginning of HTM region*/
				RTMScope begtx(&prof, depth * 2, 1, &rtmlock, GET_TYPE, should_sample, &conflict_num);
				temp_depth = ScopeFind(this_key, &leafNode);
				seqno = leafNode->seq; //snapshot the current seqno of leaf node
			}
			/*
			 *Step 2: Checking the CCM to find the possibility of duplicate keys
			 *Corresponding to Line 9-20 in Algorithm 2.
			 */
			leafNode->ccm->add_operation_num();

			unsigned int slot = leafNode->ccm->getIndex(this_key);
#if BM_QUERY
			if(!leafNode->ccm->skipBF()) {
				leafNode->mlock.Lock();
				bm_found = queryBMFilter(leafNode, this_key, slot, 0); //it should be atomic
				leafNode->mlock.Unlock();
			}
#endif

#if BM_PROF
			if(bm_found) { //for profiling
				bm_found_num++;
			} else {
				bm_miss_num++;
			}
#endif

#if ADAPTIVE_LOCK
			/*
			 *If the CCM founds an existing duplicate key, or the leaf node is near
			 *full, we should lock the leaf to eliminate the subsequent accesses.
			 */
			if(bm_found || ShouldLockLeaf(leafNode)) {
				locked = true;
				leafNode->mlock.Lock(); //lock this leaf
			} else { //read lock and write lock
				leafNode->ccm->read_Lock(slot);
				leafNode->ccm->write_Lock(slot);
			}
#endif
			bool consistent = true;
			/*
			 *Step 3: Lower region of two-step HTM region
			 *Corresponding to Line 21-32 in Algorithm 2.
			 */
			{
				RTMScope begtx(&prof, temp_depth * 2, 1, &rtmlock, GET_TYPE, should_sample);
				if(leafNode->seq == seqno) {
					ScopeInsert(leafNode, this_key, &memNode, !bm_found, temp_depth);
				} else { //the node has been split -> re-search from the top
					consistent = false;
				}
			}

#if ADAPTIVE_LOCK
			if(locked) {
				leafNode->mlock.Unlock(); //unlock the leaf
			} else { //release read lock and write lock
				leafNode->ccm->write_Unlock(slot);
				leafNode->ccm->read_Unlock(slot);
			}
#endif

			/*
			 *Step 4: If the consistency check fails, it should retry from the very beginning.
			 *Corresponding to Line 33-38 in Algorithm 2.
			 */
			if(!consistent) {
				goto TOP_RETRY;
			}
			res = memNode;
		}

#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
		if(dummyleaf_half == NULL) {
			dummyleaf_half = new LeafNode(1);
		}
#endif

		return {res, false};
	}

	/*
	 *This function is the upper region of two-step HTM scope.
	 *It is used to find the pointer to the target leaf node where the queried key resides.
	 *Return: The depth of current tree structure for synchronization with the lower region.
	 *        If the lower region does not has the latest depth, it will not search into the
	 *        leaf nodes.
	 *@leafNode: storing the pointer to the target leaf node
	 */
	inline int ScopeFind(uint64_t key, LeafNode** leafNode) {
		LeafNode* leaf;
		InnerNode* inner;
		register void* node ;
		unsigned index ;
		register unsigned d;

		node = root;
		d = depth;

		while(d-- != 0) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			node = inner->children[index];
		}
		leaf = reinterpret_cast<LeafNode*>(node);
		*leafNode = leaf;
		return depth;
	}

	/*Return whether an inner node contains a target key*/
	inline bool InnerContains(InnerNode* inner, uint64_t target_key) {
		for(int i = 0 ; i < inner->num_keys; i++) {
			if(inner->keys[i] == target_key) {
				return true;
			}
		}
		return false;
	}

	/*Return whether a leaf node contains a target key*/
	inline bool LeafContains(LeafNode* leaf, uint64_t target_key) {
		for(int i = 0; i < SEGS; i++) {
			for(int j = 0; j < leaf->leaf_segs[i].key_num; j++) {
				if(leaf->leaf_segs[i].kvs[j].key == target_key) {
					return true;
				}
			}
		}
		for(int i = 0; i < LEAF_NUM; i++) {
			if(leaf->kvs[i].key == target_key) {
				return true;
			}
		}

		return false;
	}

	/*
	 *This is the lower region of the two-step HTM scope.
	 *This function scans the target leaf node found by ScopeFind(), which is the upper
	 *region.
	 *Insert/update a key in target leaf node, and propagate the splits upwards if any.
	 *@leaf: the pointer to the target leaf node found by ScopeFind()
	 *@insert_only: true if the key is definitely nonexistent in the leaf, false if the
	 * key is probably existent in the leaf.
	 *@temp_depth: the depth returned by ScopeFind(). We need to accept this parameter to
	 * prevent searching in a null pointer.
	 */
	inline void ScopeInsert(LeafNode* leaf, uint64_t key, MemNode** val, bool insert_only, int temp_depth) {
		unsigned current_depth;
		unsigned index;
		InnerNode* temp_inner;

		LeafNode* new_leaf = NULL;
		uint64_t leaf_upKey = 0;
		if(!insert_only) { //not only insert
			bool found = FindDuplicate(leaf, key, val);
			if(found) { //duplicate insertion
#if	DUP_PROF
				dup_found++;
#endif
				return ;
			}
		}

		/*
		 *No need to find duplicate twice.
		 *leaf_upKey is the key that should be inserted into the parent InnerNode.
		 */
		new_leaf = ShuffleLeafInsert(key, leaf, val, &leaf_upKey, true);

		if(new_leaf != NULL) { //should insert new leafnode
			InnerNode* insert_inner = new_leaf->parent;
			InnerNode* toInsert = insert_inner;
			int k = 0;
			while((k < insert_inner->num_keys) && (key >= insert_inner->keys[k])) {
				k++;
			}

			InnerNode* new_sibling = NULL;

			uint64_t inner_upKey = 0;
			/*the inner node is full -> split it*/
			if(insert_inner->num_keys == N) {

				new_sibling = new_inner_node();
				new_sibling->parent = insert_inner->parent;
				if(new_leaf->leaf_segs[0].max_room == EMP_LEN) { //LeafNode is at rightmost
					new_sibling->num_keys = 0; //new sibling is also at rightmost
					inner_upKey = leaf_upKey;
					toInsert = new_sibling;
					k = -1;
				} else {
					unsigned threshold = (N + 1) / 2;
					/*num_keys(new inner node) = num_keys(old inner node) - threshold*/
					new_sibling->num_keys = insert_inner->num_keys - threshold;
					/*moving the excessive keys to the new inner node*/
					for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
						new_sibling->keys[i] = insert_inner->keys[threshold + i];
						new_sibling->children[i] = insert_inner->children[threshold + i];
						reinterpret_cast<LeafNode*>(insert_inner->children[threshold + i])->parent = new_sibling;
					}

					/*the LAST child, there is one more children than keys*/
					new_sibling->children[new_sibling->num_keys] = insert_inner->children[insert_inner->num_keys];
					reinterpret_cast<LeafNode*>(insert_inner->children[insert_inner->num_keys])->parent = new_sibling;
					/*the num_key of the original node should be below the threshold*/
					insert_inner->num_keys = threshold - 1; //=>7
					/*upkey should be the delimiter of the old/new node in their common parent*/
					uint64_t new_upKey = insert_inner->keys[threshold - 1]; //the largest key of the old innernode

					/*the new leaf node could be the child of old/new inner node*/
					if(leaf_upKey >= new_upKey) {
						toInsert = new_sibling;//should insert at the new innernode
						/*if the new inner node is to be inserted, the index to insert should subtract threshold*/
						if(k >= threshold) k = k - threshold;
						else k = 0; //or insert at the first slot
					}
					inner_upKey = new_upKey;
				}
				new_sibling->keys[N - 1] = inner_upKey;
			}

			/*insert the new key at the (k)th slot of the parent node (old or new)*/
			if(k != -1) {
				for(int i = toInsert->num_keys; i > k; i--) { //move afterwards the remaining keys
					toInsert->keys[i] = toInsert->keys[i - 1];
					toInsert->children[i + 1] = toInsert->children[i];
				}
				toInsert->num_keys++; //add a new key
				toInsert->keys[k] = leaf_upKey;
			}

			toInsert->children[k + 1] = new_leaf;
			new_leaf->parent = toInsert;

			while(new_sibling != NULL) {

				uint64_t original_upKey = inner_upKey;
				InnerNode* child_sibling = new_sibling;
				InnerNode* insert_inner = new_sibling->parent;
				InnerNode* toInsert = insert_inner;

				if(toInsert == NULL) { //so the parent should be the new root
					//root_lock.Lock();
					InnerNode *new_root = new_inner_node();
					new_root->num_keys = 1;
					new_root->keys[0] = inner_upKey;
					new_root->children[0] = root;
					new_root->children[1] = new_sibling;

					reinterpret_cast<InnerNode*>(root)->parent = new_root;
					new_sibling->parent = new_root;

					root = new_root;
					reinterpret_cast<InnerNode*>(root)->parent = NULL;
					depth++;
					return;
				}

				new_sibling = NULL;
				k = 0;
				while((k < insert_inner->num_keys) && (key >= insert_inner->keys[k])) {
					k++;
				}

				unsigned threshold = (N + 1) / 2;
				/*the current node is full, creating a new node to hold the inserted key*/
				if(insert_inner->num_keys == N) {
					//inner_splits++;
					//insert_inner->seq++;
					//inner_splits++;
					new_sibling = new_inner_node();

					new_sibling->parent = insert_inner->parent;

					if(child_sibling->num_keys == 0) {

						new_sibling->num_keys = 0;
						inner_upKey = child_sibling->keys[N - 1];
						toInsert = new_sibling;
						k = -1;
					} else {

						/*new_sibling should hold the excessive (>=threshold) keys*/
						new_sibling->num_keys = insert_inner->num_keys - threshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = insert_inner->keys[threshold + i];
							new_sibling->children[i] = insert_inner->children[threshold + i];
							reinterpret_cast<InnerNode*>(insert_inner->children[threshold + i])->parent = new_sibling;
						}
						new_sibling->children[new_sibling->num_keys] = insert_inner->children[insert_inner->num_keys];
						reinterpret_cast<InnerNode*>(insert_inner->children[insert_inner->num_keys])->parent = new_sibling;

						insert_inner->num_keys = threshold - 1;

						inner_upKey = insert_inner->keys[threshold - 1];
						/*After split, the new key could be inserted into the old node or the new node*/
						if(key >= inner_upKey) {
							toInsert = new_sibling;
							if(k >= threshold) k = k - threshold;
							else k = 0;
						}
					}
					new_sibling->keys[N - 1] = inner_upKey;
				} else {
					new_sibling = NULL;
				}
				/*Inserting the new key to appropriate position*/
				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++;
					toInsert->keys[k] = child_sibling->keys[N - 1];

				}
				toInsert->children[k + 1] = child_sibling;
				child_sibling->parent = toInsert;
			}
		}
	}

	/*The original version of ScopeInsert() function, we keep this for comparison*/
	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, MemNode** val, LeafNode** target_leaf) {
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;
		/*find the appropriate position of the new key*/
		while((k < inner->num_keys) && (key >= inner->keys[k])) {
			k++;
		}

		void *child = inner->children[k]; //search the descendent layer

		/*inserting at the lowest inner level*/
		if(d == 1) {
			//*target_inner = inner;
			uint64_t temp_upKey;
			LeafNode *new_leaf = ShuffleLeafInsert(key, reinterpret_cast<LeafNode*>(child), val, &temp_upKey, false);

			if(new_leaf != NULL) {	//if a new leaf node is created
				InnerNode *toInsert = inner;
				/*the inner node is full -> split it*/
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();

					new_sibling->parent = inner->parent;

					if(new_leaf->leaf_segs[0].max_room == EMP_LEN) { //the new LeafNode is at rightmost

						new_sibling->num_keys = 0;
						upKey = temp_upKey;

						toInsert = new_sibling;
						k = -1;
					} else {

						unsigned threshold = (N + 1) / 2;
						//num_keys(new inner node) = num_keys(old inner node) - threshold
						new_sibling->num_keys = inner->num_keys - threshold;
						//moving the excessive keys to the new inner node
						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[threshold + i];
							new_sibling->children[i] = inner->children[threshold + i];
							reinterpret_cast<LeafNode*>(inner->children[threshold + i])->parent = new_sibling;
						}

						//the last child
						new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
						reinterpret_cast<LeafNode*>(inner->children[inner->num_keys])->parent = new_sibling;
						//the num_key of the original node should be below the threshold
						inner->num_keys = threshold - 1;
						//upkey should be the delimiter of the old/new node in their common parent

						upKey = inner->keys[threshold - 1];

						//the new leaf node could be the child of old/new inner node
						if(temp_upKey >= upKey) {
							toInsert = new_sibling;
							//if the new inner node is to be inserted, the index to insert should subtract threshold
							if(k >= threshold) k = k - threshold;
							else k = 0;
						}

					}
					new_sibling->keys[N - 1] = upKey;

				}
				//insert the new key at the (k)th slot of the parent node (old or new)
				if(k != -1) {

					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++; //add a new key
					//toInsert->keys[k] = new_leaf->kvs[0].key;
					toInsert->keys[k] = temp_upKey; //subtle bugs

				}

				toInsert->children[k + 1] = new_leaf;
				new_leaf->parent = toInsert;

			}
		} else {
			/*
			 *not inserting at the lowest inner level
			 *recursively insert at the lower levels
			 */
			InnerNode *new_inner =
				InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val, target_leaf);

			if(new_inner != NULL) {

				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;

				unsigned treshold = (N + 1) / 2; //split equally
				//the current node is full, creating a new node to hold the inserted key
				if(inner->num_keys == N) {

					new_sibling = new_inner_node();

					new_sibling->parent = inner->parent;

					if(child_sibling->num_keys == 0) {

						new_sibling->num_keys = 0;

						upKey = child_sibling->keys[N - 1];
						toInsert = new_sibling;
						k = -1;
					} else {
						/*new_sibling should hold the excessive (>=threshold) keys*/
						new_sibling->num_keys = inner->num_keys - treshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[treshold + i];
							new_sibling->children[i] = inner->children[treshold + i];
							reinterpret_cast<InnerNode*>(inner->children[treshold + i])->parent = new_sibling;
						}

						new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
						reinterpret_cast<InnerNode*>(inner->children[inner->num_keys])->parent = new_sibling;

						inner->num_keys = treshold - 1;

						upKey = inner->keys[treshold - 1];
						/*after split, the new key could be inserted into the old node or the new node*/
						if(key >= upKey) {
							toInsert = new_sibling;
							if(k >= treshold) k = k - treshold;
							else k = 0;
						}
					}
					new_sibling->keys[N - 1] = upKey;

				}
				/*inserting the new key to appropriate position*/
				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++;
					toInsert->keys[k] = child_sibling->keys[N - 1];

				}
				child_sibling->parent = toInsert;
				toInsert->children[k + 1] = child_sibling;
			}
		}

		if(d == depth && new_sibling != NULL) {
			InnerNode *new_root = new_inner_node();
			new_root->num_keys = 1;
			new_root->keys[0] = upKey;
			new_root->children[0] = root;
			new_root->children[1] = new_sibling;

			reinterpret_cast<InnerNode*>(root)->parent = new_root;
			new_sibling->parent = new_root;

			root = new_root;

			depth++;
		}
		return new_sibling; //return the newly-created node (if exists)
	}

	inline Memstore::MemNode* GetForRead(uint64_t key) {
		ThreadLocalInit();
		MemNode* value = Get(key);
#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
		if(dummyleaf_half == NULL) {
			dummyleaf_half = new LeafNode(1);
		}
#endif
		return value;
	}

	/*
	 *This function is a fallback path when contention rate (or thread number) is low.
	 *It bypasses the two-step HTM scope, to skip the overhead from CCM.
	 *Return: the pointer to the MemNode correspoding to the key.
	 *@target_leaf: storing the pointer to the target leaf node.
	 */
	inline Memstore::MemNode* Insert_rtm(uint64_t key, LeafNode** target_leaf, bool should_sample = false) {
		//(*target_leaf)->ccm->add_operation_num();
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock, ADD_TYPE, should_sample);
#endif
		if(root == NULL) {
			LeafNode* root_leaf = new_leaf_node();

			for(int i = 0; i < SEGS; i++) {
				root_leaf->leaf_segs[i].max_room = EMP_LEN;
				root_leaf->leaf_segs[i].key_num = 0;
			}

			root_leaf->left = NULL;
			root_leaf->right = NULL;
			root_leaf->parent = NULL;
			root_leaf->seq = 0;
			root = root_leaf;
			depth = 0;
		}

		MemNode* val = NULL;
		if(depth == 0) {
			uint64_t upKey;
			//LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val, &upKey);
			LeafNode *new_leaf = ShuffleLeafInsert(key, reinterpret_cast<LeafNode*>(root), &val, &upKey, false);

			if(new_leaf != NULL) { //a new leaf node is created, therefore adding a new inner node to hold
				InnerNode *inner = new_inner_node();

				inner->num_keys = 1;
				inner->keys[0] = upKey;
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				reinterpret_cast<LeafNode*>(root)->parent = inner;
				new_leaf->parent = inner;
				depth++; //depth=1
				root = inner;
			}
		} else {
#if BTPREFETCH
			for(int i = 0; i <= 64; i += 64)
				prefetch(reinterpret_cast<char*>(root) + i);
#endif
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val, target_leaf);
		}
		//(*target_leaf)->ccm->add_conflict_num(begtx.getRetry());
		return val;
	}

	/*Dump the internal details of a leaf node.*/
	void dump_leaf(LeafNode* leaf) {
		if(leaf == root) {
			printf("This is root\n");
		}
		printf("leaf->kvs_num = %u\n" , leaf->kvs_num);
		for(int i = 0; i < leaf->kvs_num; i++) {
			printf("leaf->kvs[%d].key = %lu, value = %p\n", i, leaf->kvs[i].key, leaf->kvs[i].value);
		}
		for(int i = 0; i < SEGS; i++) {
			printf(
				"leaf->leaf_segs[%d].key_num = %u max_room = %u\n" , i, leaf->leaf_segs[i].key_num, leaf->leaf_segs[i].max_room);
			for(int j = 0; j < leaf->leaf_segs[i].key_num; j++) {
				printf("[num = %d] leaf->leaf_segs[%d].kvs[%d].key = %lu, value = %p\n", leaf->leaf_segs[i].key_num, i, j, leaf->leaf_segs[i].kvs[j].key,
					   leaf->leaf_segs[i].kvs[j].value);
			}
		}
	}

	/*Dump the internal details of an inner node.*/
	void dump_inner(InnerNode* inner) {
		for(int i = 0 ; i < inner->num_keys; i++) {
			printf("inner->keys[%d] = %lu\n", i, inner->keys[i]);
		}
	}

	/*
	 *Searching for duplicate keys in a leaf node.
	 *Return: whether duplicate keys are found.
	 *@val: storing the pointer of the target value.
	 */
	inline bool FindDuplicate(LeafNode* leaf, uint64_t key, MemNode** val) {
		for(int i = 0; i < SEGS; i++) {
			for(int j = 0; j < leaf->leaf_segs[i].key_num; j++) {
				if(leaf->leaf_segs[i].kvs[j].key == key) {
					*val = leaf->leaf_segs[i].kvs[j].value;
					return true;
				}
			}
		}
		for(int i = 0; i < leaf->kvs_num; i++) {
			if(leaf->kvs[i].key == key) {
				*val = leaf->kvs[i].value;
				return true;
			}
		}
		return false;
	}

	/*
	 *Reorganizing the unordered keys in the segments.
	 *Move them to Reserved Keys in order.
	 *This function is invoked when the node is being range queried.
	 */
	inline void ReorganizeLeafNode(LeafNode* leaf) {
		if(leaf->reserved == NULL) {
			leaf->reserved = (KeyValue*)calloc(LEAF_NUM, sizeof(KeyValue));
		} else {
			memset(leaf->reserved, 0, sizeof(KeyValue)*LEAF_NUM);
		}
		for(int i = 0; i < leaf->kvs_num; i++) {
			leaf->reserved[i] = leaf->kvs[i];
		}

		int kvs_num = leaf->kvs_num;
		int temp = 0;
		int segment_keys = 0;
		for(int i = 0 ; i < SEGS; i++) {
			segment_keys += leaf->leaf_segs[i].key_num;
			for(int j = 0; j < leaf->leaf_segs[i].key_num; j++) {
				leaf->reserved[kvs_num + temp] = leaf->leaf_segs[i].kvs[j];
				temp++;
			}
		}
		//std::sort(leaf->reserved, leaf->reserved + segment_keys+kvs_num, KVCompare);
		leaf->num_keys = segment_keys + kvs_num;
	}

	inline void dump_reserved(LeafNode* leaf) {
		printf("reserved[%p] = [", leaf);
		for(int i = 0; i < leaf->num_keys; i++) {
			printf("%lu, ", leaf->reserved[i].key);
		}
		printf("]\n");
	}

	/*
	 *This function handles inserting/updating new keys in a certain leaf node.
	 *It corresponds to Algorithm 3 in the Paper. The algorithm here is split into
	 *four cases, each corresponds to one case in Algorithm 3. We are sorry that Line 8-9
	 *in Algorithm 3 (tryLock) should be removed.
	 *(1) It first probe the leaf node to find duplicate keys if CCM return a positve result.
	 *(2) If there are no duplicate keys, it will insert a new key/value pair into the
	 * segments or reserved keys. Move the keys between the two structures if necessary.
	 *(3) If the insertion incurs splits, it will propagate the splits from bottom to top.
	 *Return: new leaf node created if insert incurs splits
	 *@val: pointer to the value of inserted key
	 *@upKey: the new key that should be inserted into the parent nodes
	 */
	inline LeafNode* ShuffleLeafInsert(uint64_t key, LeafNode *leaf, MemNode** val, uint64_t* upKey, bool insert_only) {
#if DUP_PROF
		leaf_inserts++;
#endif
		LeafNode *new_sibling = NULL;
		/*CCM do have non-zero false positive rate, so we have to re-search the duplicate keys here.*/
		if(!insert_only) {
			bool found = FindDuplicate(leaf, key, val); //if found, val is already set to the retrieved value
			if(found) { //duplicate insertion
#if	DUP_PROF
				dup_found++;
#endif
				return NULL;
			}
		}

		/*
		 *First we select an available segment randomly.
		 *This corresponds to Line 2-4 in Algorithm 3.
		 */
		int idx = key % SEGS;
		int retries = 0;
		while(leaf->leaf_segs[idx].isFull() && retries < THRESHOLD) {
			idx = (idx << 1) % SEGS;
			retries++;
		}
		/*
		 *[Case #1] Shuffled to an empty segment within THRESHOLD times, insert immediately
		 *This corresponds to Line 5-6 in Algorithm 3.
		 */
		if(!leaf->leaf_segs[idx].isFull() && !leaf->isFull()) {
			*val = leaf->insertSegment(idx, key);
			return NULL;
		} else {
			/*
			 *[Case #2] The node still has suffient room, move the keys to Reserved Keys
			 *This corresponds to Line 10-13 in Algorithm 3.
			 */
			if(leaf->hasRoom() && !leaf->isFull())  {//the node still has sufficient room
				leaf->moveToReserved();//move the keys to reserved room
				leaf->shrinkSegs();
				*val = leaf->insertSegment(idx, key);
				return NULL;
				/*
				 *[Case #3] The node does not have sufficient room, but not full
				 *This corresponds to Line 14-16 in Algorithm 3.
				 */
			} else if(!leaf->isFull()) { //node is not full
				leaf->moveToReserved();//move the keys to reserved room
				//leaf->shrinkSegs();
				*val = leaf->insertSegment(idx, key);
				return NULL;
				/*
				 *[Case #4] The node is really full, we need to split the node
				 *This corresponds to Line 17-23 in Algorithm 3.
				 */
			} else {
#if DUP_PROF
				leaf_splits++;
#endif

				/*increase the sequential number of split leaf to keep consistency*/
				leaf->seq++;

				/*move the keys to reserved room*/
				leaf->moveToReserved();

				/*
				 *Sort the keys to find whether the new key should be inserted into the
				 *old node or the new node.
				 */
				std::sort(leaf->kvs, leaf->kvs + LEAF_NUM, KVCompare);
				unsigned k = 0;
				while((k < LEAF_NUM) && (leaf->kvs[k].key < key)) {
					++k;
				}

				LeafNode *toInsert = leaf;
				new_sibling = new_leaf_node();
				/*Initialize the reserved keys*/
				new_sibling -> init_reserved_keys();
				/*The new leafnode is created at rightmost*/
				if(leaf->right == NULL && k == LEAF_NUM) {
					for(int i = 0; i < SEGS; i++) {
						leaf->leaf_segs[i].max_room = 0;
						leaf->leaf_segs[i].key_num = 0;
					}
					leaf->kvs_num = LEAF_NUM;
					toInsert = new_sibling;
					if(new_sibling->leaf_segs[0].key_num != 0) {
						for(int i = 0; i < SEGS; i++) {
							new_sibling->leaf_segs[i].key_num = 0;
						}
					}
					for(int i = 0; i < SEGS; i++) {
						new_sibling->leaf_segs[i].key_num = 0;
						new_sibling->leaf_segs[i].max_room = EMP_LEN;
					}
					new_sibling->kvs_num = 0;
					new_sibling->num_keys = 1;
					toInsert->leaf_segs[0].key_num = 1;
					toInsert->leaf_segs[0].kvs[0].key = key;
					toInsert->kvs[0].key = key; //keys[0] should be set here
					*upKey = key; //the sole new key should be the upkey
					/*
					 *The new node is not created at rightmost.
					 *Therefore the new node is created with half 'old keys'
					 */
				} else {
					unsigned threshold = (LEAF_NUM + 1) / 2;
					unsigned new_sibling_num_keys = LEAF_NUM - threshold;

					leaf->kvs_num = threshold;
					new_sibling->kvs_num = new_sibling_num_keys;
					/*Moving the keys above the threshold to the new sibling*/

					for(int i = 0; i < SEGS; i++) {
						leaf->leaf_segs[i].key_num = 0;
						leaf->leaf_segs[i].max_room = HAL_LEN;
						new_sibling->leaf_segs[i].key_num = 0;
						new_sibling->leaf_segs[i].max_room = HAL_LEN;
						new_sibling->leaf_segs[i].shrink();
					}

					for(int i = 0; i < new_sibling_num_keys; i++) {
						new_sibling->kvs[i] = leaf->kvs[threshold + i];
					}

					leaf->num_keys = threshold;
					new_sibling->num_keys = new_sibling_num_keys;
					/*In this case, the new key should be inserted in the newly-born node*/
					if(k >= threshold) {
						toInsert = new_sibling;
					}
					toInsert->leaf_segs[0].key_num = 1;
					toInsert->leaf_segs[0].kvs[0].key = key;
					toInsert->num_keys++;
					if(k == threshold) {
						*upKey = key;
					} else {
						*upKey = new_sibling->kvs[0].key;
					}
				}

				/*inserting the newsibling at the right of the old leaf node*/
				if(leaf->right != NULL) {
					leaf->right->left = new_sibling;
				}

				new_sibling->right = leaf->right;
				new_sibling->left = leaf;
				leaf->right = new_sibling;

				new_sibling->parent = leaf->parent;

#if DUMMY
				toInsert->leaf_segs[0].kvs[0].value = dummyval_;
				*val = dummyval_;
#else
				toInsert->leaf_segs[0].kvs[0].value = GetMemNode();
				*val = toInsert->leaf_segs[0].kvs[0].value;
#endif

				assert(*val != NULL);
				dummyval_ = NULL;
				/*Clear the mark_bit*/
				leaf->ccm->clearMarkCount();
				/*Shrink the reserved keys to save memory usage*/
				leaf->shrink();
			}
		}
		return new_sibling;
	}

	/*
	 *Insert a key at the leaf level
	 *Return: the new node where the new key resides, NULL if no new node is created
	 *@val: storing the pointer to new value in val
	 */
	inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, MemNode** val, uint64_t* upKey) {
#if SHUFFLE_INSERT
		return ShuffleLeafInsert(key, leaf, val, upKey, false);
#else
		return SimpleLeafInsert(key, leaf, val);
#endif
	}

	Memstore::Iterator* GetIterator() {
		return new MemstoreEunoTree::Iterator(this);
	}
	void printLeaf(LeafNode *n);
	void printInner(InnerNode *n, unsigned depth);
	void PrintStore();
	void PrintList();
	void checkConflict(int sig, int mode) ;

//YCSB TREE COMPARE Test Purpose
	void TPut(uint64_t key, uint64_t *value) {
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif

		if(root == NULL) {
			root = new_leaf_node();
			reinterpret_cast<LeafNode*>(root)->left = NULL;
			reinterpret_cast<LeafNode*>(root)->right = NULL;
			reinterpret_cast<LeafNode*>(root)->seq = 0;
			depth = 0;
		}

		if(depth == 0) {
			LeafNode *new_leaf = TLeafInsert(key, reinterpret_cast<LeafNode*>(root), value);
			if(new_leaf != NULL) {
				InnerNode *inner = new_inner_node();
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->kvs[0].key;
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++;
				root = inner;
#if BTREE_PROF
				writes++;
#endif
			}
		} else {

#if BTPREFETCH
			for(int i = 0; i <= 64; i += 64)
				prefetch(reinterpret_cast<char*>(root) + i);
#endif

			TInnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, value);
		}
	}

	inline LeafNode* TLeafInsert(uint64_t key, LeafNode *leaf, uint64_t *value) {
		LeafNode *new_sibling = NULL;
		unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->kvs[k].key < key)) {
			++k;
		}

		if((k < leaf->num_keys) && (leaf->kvs[k].key == key)) {
			leaf->kvs[k].value = (Memstore::MemNode *)value;
			return NULL;
		}

		LeafNode *toInsert = leaf;
		if(leaf->num_keys == LEAF_NUM) {
			new_sibling = new_leaf_node();
			if(leaf->right == NULL && k == leaf->num_keys) {
				new_sibling->num_keys = 0;
				toInsert = new_sibling;
				k = 0;
			} else {
				unsigned threshold = (LEAF_NUM + 1) / 2;
				new_sibling->num_keys = leaf->num_keys - threshold;
				for(unsigned j = 0; j < new_sibling->num_keys; ++j) {
					new_sibling->kvs[j] = leaf->kvs[threshold + j];
					//new_sibling->values[j] = leaf->values[threshold + j];
				}
				leaf->num_keys = threshold;

				if(k >= threshold) {
					k = k - threshold;
					toInsert = new_sibling;
				}
			}
			if(leaf->right != NULL) leaf->right->left = new_sibling;
			new_sibling->right = leaf->right;
			new_sibling->left = leaf;
			leaf->right = new_sibling;
			new_sibling->seq = 0;
#if BTREE_PROF
			writes++;
#endif
		}

		for(int j = toInsert->num_keys; j > k; j--) {
			toInsert->kvs[j] = toInsert->kvs[j - 1];
			//toInsert->values[j] = toInsert->values[j - 1];
		}

		toInsert->num_keys = toInsert->num_keys + 1;
		toInsert->kvs[k].key = key;
		toInsert->kvs[k].value = (Memstore::MemNode *)value;

		return new_sibling;
	}

	inline InnerNode* TInnerInsert(uint64_t key, InnerNode *inner, int d, uint64_t* value) {
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;

		while((k < inner->num_keys) && (key >= inner->keys[k])) {
			++k;
		}

		void *child = inner->children[k];

#if BTPREFETCH
		for(int i = 0; i <= 64; i += 64)
			prefetch(reinterpret_cast<char*>(child) + i);
#endif

		if(d == 1) {
			LeafNode *new_leaf = TLeafInsert(key, reinterpret_cast<LeafNode*>(child), value);
			if(new_leaf != NULL) {
				InnerNode *toInsert = inner;
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();
					if(new_leaf->num_keys == 1) {
						new_sibling->num_keys = 0;
						upKey = new_leaf->kvs[0].key;
						toInsert = new_sibling;
						k = -1;
					} else {
						unsigned treshold = (N + 1) / 2;
						new_sibling->num_keys = inner->num_keys - treshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[treshold + i];
							new_sibling->children[i] = inner->children[treshold + i];
						}

						new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
						inner->num_keys = treshold - 1;

						upKey = inner->keys[treshold - 1];

						if(new_leaf->kvs[0].key >= upKey) {
							toInsert = new_sibling;
							if(k >= treshold) k = k - treshold;
							else k = 0;
						}
					}
//					inner->keys[N-1] = upKey;
					new_sibling->keys[N - 1] = upKey;
				}

				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++;
					toInsert->keys[k] = new_leaf->kvs[0].key;
				}
				toInsert->children[k + 1] = new_leaf;
			}

//			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
		} else {
			bool s = true;
			InnerNode *new_inner =
				TInnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, value);

			if(new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;


				unsigned treshold = (N + 1) / 2;
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();

					if(child_sibling->num_keys == 0) {
						new_sibling->num_keys = 0;
						upKey = child_sibling->keys[N - 1];
						toInsert = new_sibling;
						k = -1;
					}

					else  {
						new_sibling->num_keys = inner->num_keys - treshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[treshold + i];
							new_sibling->children[i] = inner->children[treshold + i];
						}
						new_sibling->children[new_sibling->num_keys] =
							inner->children[inner->num_keys];


						inner->num_keys = treshold - 1;

						upKey = inner->keys[treshold - 1];
						//printf("UP %lx\n",upKey);
						if(key >= upKey) {
							toInsert = new_sibling;
							if(k >= treshold) k = k - treshold;
							else k = 0;
						}
					}
					//inner->keys[N-1] = upKey;
					new_sibling->keys[N - 1] = upKey;


				}
				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}

					toInsert->num_keys++;
					toInsert->keys[k] = reinterpret_cast<InnerNode*>(child_sibling)->keys[N - 1];
				}
				toInsert->children[k + 1] = child_sibling;
			}
		}

		if(d == depth && new_sibling != NULL) {
			InnerNode *new_root = new_inner_node();
			new_root->num_keys = 1;
			new_root->keys[0] = upKey;
			new_root->children[0] = root;
			new_root->children[1] = new_sibling;
			root = new_root;
			depth++;
		}
//		else if (d == depth) checkConflict(reinterpret_cast<InnerNode*>(root)->signature, 0);
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling;
	}

public:
	static __thread RTMArena* arena_;
	static __thread bool localinit_;
	static __thread MemNode *dummyval_;
	static __thread LeafNode *dummyleaf_;
	static __thread LeafNode *dummyleaf_half;

	//char padding1[64];
	void *root;
	int depth;

	//char padding2[64];
	RTMProfile delprof;
	//char padding3[64];

	RTMProfile prof;
	RTMProfile prof1;
	RTMProfile prof2;
	//char padding6[64];
	port::SpinLock slock;
#if BTREE_PROF
public:
	uint64_t reads;
	uint64_t writes;
	uint64_t calls;
#endif
	//char padding4[64];
	SpinLock rtmlock;
	//char padding5[64];

	int current_tid;
	//int waccess[4][CONFLICT_BUFFER_LEN];
	//int raccess[4][CONFLICT_BUFFER_LEN];
	int windex[4];
	int rindex[4];
};

//__thread RTMArena* MemstoreEunoTree::arena_ = NULL;
//__thread bool MemstoreEunoTree::localinit_ = false;
}
#endif
