#ifndef SECONDINDEXUINTBPLUSTREE_H
#define SECONDINDEXUINTBPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include "util/rtmScope.h" 
#include "util/rtm.h" 
#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"
#include "memstore.h"
#include "secondindex.h"
#define IIM  15
#define IIN  15

#define IIBTREE_PROF 0
#define IIBTREE_LOCK 0

//static uint64_t writes = 0;
//static uint64_t reads = 0;
	
/*static int total_key = 0;
static int total_nodes = 0;
static uint64_t rconflict = 0;
static uint64_t wconflict = 0;
*/
namespace leveldb {
class SecondIndexUint64BPlusTree: public SecondIndex{
	
private:	
	struct LeafNode {
		LeafNode() : num_keys(0){}//, writes(0), reads(0) {}
//		uint64_t padding[4];
		unsigned num_keys;
		uint64_t keys[IIM];
		SecondNode *values[IIM];
		LeafNode *left;
		LeafNode *right;
		uint64_t seq;
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[4];
	};
		 
	struct InnerNode {
    	InnerNode() : num_keys(0) {}//, writes(0), reads(0) {}
//		uint64_t padding[8];
//		unsigned padding;
		unsigned num_keys;
		uint64_t keys[IIN];
		void*	 children[IIN+1];
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[8];
	};

	class Iterator: public SecondIndex::Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  Iterator(){};
	  Iterator(SecondIndexUint64BPlusTree* tree);

	  // Returns true iff the iterator is positioned at a valid node.
	  bool Valid();

	  // Returns the key at the current position.
	  // REQUIRES: Valid()
	  SecondNode* CurNode();

	  
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
	  SecondIndexUint64BPlusTree* tree_;
	  LeafNode* node_;
	  uint64_t seq_;
	  int leaf_index;
	  uint64_t *link_;
	  uint64_t target_;
	  uint64_t key_;
	  SecondNode* value_;
	  uint64_t snapshot_;
	  // Intentionally copyable
	};

public:	
	SecondIndexUint64BPlusTree() {
		root = new LeafNode();
		reinterpret_cast<LeafNode*>(root)->left = NULL;
		reinterpret_cast<LeafNode*>(root)->right = NULL;
		reinterpret_cast<LeafNode*>(root)->seq = 0;
		depth = 0;
				
#if IIBTREE_PROF
		writes = 0;
		reads = 0;
		calls = 0;
#endif
				//		printf("root addr %lx\n", &root);
				//		printf("depth addr %lx\n", &depth);
				/*		for (int i=0; i<4; i++) {
							windex[i] = 0;
							rindex[i] = 0;
						}*/
	}



	
	~SecondIndexUint64BPlusTree() {
		//prof.reportAbortStatus();
		//PrintList();
		//PrintStore();
		//printf("rwconflict %ld\n", rconflict);
		//printf("wwconflict %ld\n", wconflict);
		//printf("depth %d\n",depth);
		//printf("reads %ld\n",reads);
		//printf("writes %ld\n", writes);
		//printf("calls %ld touch %ld avg %f\n", calls, reads + writes,  (float)(reads + writes)/(float)calls );
#if IIBTREE_PROF
		printf("calls %ld avg %f writes %f\n", calls, (float)(reads + writes)/(float)calls,(float)(writes)/(float)calls );
#endif
	
		//PrintStore();
		//top();
	}
  	  
	inline void ThreadLocalInit(){
		if(false == localinit_) {
			arena_ = new RTMArena();

			dummywrapper_ = new MemNodeWrapper();
			dummyval_ = new SecondNode();
			
			localinit_ = true;
		}
			
	}

	inline LeafNode* new_leaf_node() {
			LeafNode* result = new LeafNode();
			//LeafNode* result = (LeafNode *)(arena_->AllocateAligned(sizeof(LeafNode)));
			return result;
	}
		
	inline InnerNode* new_inner_node() {
			InnerNode* result = new InnerNode();
			//InnerNode* result = (InnerNode *)(arena_->AllocateAligned(sizeof(InnerNode)));
			return result;
	}



	inline LeafNode* FindLeaf(uint64_t key) 
	{
		InnerNode* inner;
		register void* node= root;
		register unsigned d= depth;
		unsigned index = 0;
		while( d-- != 0 ) {
				index = 0;
				inner= reinterpret_cast<InnerNode*>(node);
				while(index < inner->num_keys && key >= inner->keys[index])  {
				   ++index;
				}				
				node= inner->children[index];
		}
		return reinterpret_cast<LeafNode*>(node);
	}
	

	inline SecondNode* RoGet(uint64_t key)
	{
//		RTMArenaScope begtx(&rtmlock, &prof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock);

		InnerNode* inner;
		register void* node= root;
		register unsigned d= depth;
		unsigned index = 0;
		while( d-- != 0 ) {
				index = 0;
				inner= reinterpret_cast<InnerNode*>(node);
//				reads++;
				while(index < inner->num_keys && key >= inner->keys[index]) {
				   ++index;
				}				
				node= inner->children[index];
		}
		LeafNode* leaf= reinterpret_cast<LeafNode*>(node);
//		reads++;
		
	    unsigned k = 0;
		while(k < leaf->num_keys && leaf->keys[k] < key) {
		   ++k;
		}
		if(leaf->keys[k] == key) {
			return leaf->values[k];
		} else {
			return NULL;
		}
	}


	inline SecondNode* Get(uint64_t key) {
		ThreadLocalInit();
		SecondNode *secn = Get_sec(key);
		return secn;
	}
	
	void Put(uint64_t seckey, uint64_t prikey, Memstore::MemNode* memnode){
		uint64_t *secseq;
		MemNodeWrapper *w = GetWithInsert(seckey, prikey, &secseq);
		w->memnode = memnode;
		w->valid = 1;
	}
	
	inline MemNodeWrapper* GetWithInsert(uint64_t seckey, uint64_t prikey, uint64_t **secseq) {

		ThreadLocalInit();
//		NewNodes *dummy= new NewNodes(depth);
//		NewNodes dummy(depth);

/*		MutexSpinLock lock(&slock);
		current_tid = tid;
		windex[tid] = 0;
		rindex[tid] = 0;
	*/	
		SecondNode *secn = Get_sec(seckey);
		MemNodeWrapper *mnw = Insert_Sec(secn, prikey);
		if(dummyval_ == NULL)
			dummyval_ = new SecondNode();
		if (dummywrapper_ == NULL)
			dummywrapper_ = new MemNodeWrapper();
					
		*secseq = &(secn->seq);
		return mnw;
		
//		Insert_rtm(key, &dummy);

/*		if (dummy->leaf->num_keys <=0) delete dummy->leaf;
		for (int i=dummy->used; i<dummy->d;i++) {
			delete dummy->inner[i];
			//if (dummy.inner[i]->num_keys > 0) printf("!!!\n");
		}*/
//		delete dummy;
		
	}

	inline MemNodeWrapper* Insert_Sec(SecondNode *secn, uint64_t prikey) {
		
#if IIBTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif		
		MemNodeWrapper *wrapper = secn->head;
		while (wrapper != NULL) {
			if (wrapper->key == prikey) 
		//		if (wrapper->valid) return NULL; else
				return wrapper;
			wrapper = wrapper->next;
		}
		dummywrapper_->key = prikey;
		dummywrapper_->next = secn->head;
		secn->head = dummywrapper_;
		secn->seq++;
		dummywrapper_ = NULL;
		
		return secn->head;
	}
	
	inline SecondNode* Get_sec(uint64_t key) {
#if IIBTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif

#if IIBTREE_PROF
		calls++;
#endif

		SecondNode* val = NULL;
		if (depth == 0) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val);
			if (new_leaf != NULL) {
				InnerNode *inner = new_inner_node();
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->keys[0];
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++;
				root = inner;
//				checkConflict(inner, 1);
//				checkConflict(&root, 1);
#if IIBTREE_PROF
				writes++;
#endif
//				inner->writes++;
			}
//			else checkConflict(&root, 0);
		}
		else {
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val);
			
		}

		return val;
	}

	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, SecondNode** val) {
	
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;
		//printf("key %lx\n",key);
		//printf("d %d\n",d);
		while(k < inner->num_keys && key >= inner->keys[k])  {
		   	++k;
		}
		void *child = inner->children[k];
/*		if (child == NULL) {
			printf("Key %lx\n");
			printInner(inner, d);
		}*/
		//printf("child %d\n",k);
		if (d == 1) {
			//printf("leafinsert\n");
			//printTree();
			
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val);
			//printTree();
			if (new_leaf != NULL) {
				InnerNode *toInsert = inner;
				if (inner->num_keys == IIN) {										
					unsigned treshold= (IIN+1)/2;
					new_sibling = new_inner_node();
					
					new_sibling->num_keys= inner->num_keys -treshold;
					//printf("sibling num %d\n",new_sibling->num_keys);
                    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
                    	new_sibling->keys[i]= inner->keys[treshold+i];
                        new_sibling->children[i]= inner->children[treshold+i];
                    }
                    new_sibling->children[new_sibling->num_keys]=
                                inner->children[inner->num_keys];
                    inner->num_keys= treshold-1;
					//printf("remain num %d\n",inner->num_keys);
					upKey = inner->keys[treshold-1];
					//printf("UP %lx\n",upKey);
					if (new_leaf->keys[0] >= upKey) {
						toInsert = new_sibling;
						if (k >= treshold) k = k - treshold; 
						else k = 0;
					}
					inner->keys[IIN-1] = upKey;
//					checkConflict(new_sibling, 1);
#if IIBTREE_PROF
					writes++;
#endif
//					new_sibling->writes++;
					
				}
				
				for (int i=toInsert->num_keys; i>k; i--) {
					toInsert->keys[i] = toInsert->keys[i-1];
					toInsert->children[i+1] = toInsert->children[i];					
				}
				toInsert->num_keys++;
				toInsert->keys[k] = new_leaf->keys[0];
				toInsert->children[k+1] = new_leaf;
//				checkConflict(inner, 1);
#if IIBTREE_PROF
				writes++;
#endif
//				inner->writes++;
			}
			else {
#if IIBTREE_PROF
				reads++;
#endif
//				inner->reads++;
//				checkConflict(inner, 0);
			}
			
//			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
		}
		else {
			//printf("inner insert\n");
			bool s = true;
			InnerNode *new_inner = 
				InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val);
			
			
			if (new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;
				unsigned treshold= (IIN+1)/2;
				if (inner->num_keys == IIN) {										
					
					new_sibling = new_inner_node();
					new_sibling->num_keys= inner->num_keys -treshold;
					
                    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
                    	new_sibling->keys[i]= inner->keys[treshold+i];
                        new_sibling->children[i]= inner->children[treshold+i];
                    }
                    new_sibling->children[new_sibling->num_keys]=
                                inner->children[inner->num_keys];
                                
                    //XXX: should threshold ???
                    inner->num_keys= treshold-1;
					
					upKey = inner->keys[treshold-1];
					//printf("UP %lx\n",upKey);
					if (key >= upKey) {
						toInsert = new_sibling;
						if (k >= treshold) k = k - treshold; 
						else k = 0;
					}

					//XXX: what is this used for???
					inner->keys[IIN-1] = upKey;

#if IIBTREE_PROF
					writes++;
#endif
//					new_sibling->writes++;
//					checkConflict(new_sibling, 1);
				}	
				
				for (int i=toInsert->num_keys; i>k; i--) {
					toInsert->keys[i] = toInsert->keys[i-1];
					toInsert->children[i+1] = toInsert->children[i];					
				}
			
				toInsert->num_keys++;
				toInsert->keys[k] = reinterpret_cast<InnerNode*>(child)->keys[IIN-1];

				toInsert->children[k+1] = child_sibling;
														
#if IIBTREE_PROF
				writes++;
#endif
//				inner->writes++;
//				checkConflict(inner, 1);
			}
			else {
#if IIBTREE_PROF
				reads++;
#endif
//				inner->reads++;
//				checkConflict(inner, 0);
			}
	
			
		}
		
		if (d==depth && new_sibling != NULL) {
			InnerNode *new_root = new_inner_node();			
			new_root->num_keys = 1;
			new_root->keys[0]= upKey;
			new_root->children[0] = root;
			new_root->children[1] = new_sibling;
			root = new_root;
			depth++;	

#if IIBTREE_PROF
			writes++;
#endif
//			new_root->writes++;
//			checkConflict(new_root, 1);
//			checkConflict(&root, 1);
		}
//		else if (d == depth) checkConflict(&root, 0);
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling;
	}

	inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, SecondNode** val) {
		LeafNode *new_sibling = NULL;
		unsigned k = 0;
		while(k < leaf->num_keys && leaf->keys[k] < key)  {
		   ++k;
		}
		
		if(k < leaf->num_keys)  {
			if (leaf->keys[k] == key) {
				*val = leaf->values[k];
#if IIBTREE_PROF
				reads++;
#endif
				assert(*val != NULL);
				return NULL;
			}
		}
			

		LeafNode *toInsert = leaf;
		if (leaf->num_keys == IIM) {
			new_sibling = new_leaf_node();
			unsigned threshold= (IIM+1)/2;
			new_sibling->num_keys= leaf->num_keys -threshold;
            for(unsigned j=0; j < new_sibling->num_keys; ++j) {
            	new_sibling->keys[j]= leaf->keys[threshold+j];
				new_sibling->values[j]= leaf->values[threshold+j];
            }
            leaf->num_keys= threshold;
			

			if (k>=threshold) {
				k = k - threshold;
				toInsert = new_sibling;
			}

			if (leaf->right != NULL) leaf->right->left = new_sibling;
			new_sibling->right = leaf->right;
			new_sibling->left = leaf;
			leaf->right = new_sibling;
			new_sibling->seq = 0;
#if IIBTREE_PROF
			writes++;
#endif
//			new_sibling->writes++;
//			checkConflict(new_sibling, 1);
		}
		
		
		//printf("IN LEAF1 %d\n",toInsert->num_keys);
		//printTree();

        for (int j=toInsert->num_keys; j>k; j--) {
			toInsert->keys[j] = toInsert->keys[j-1];
			toInsert->values[j] = toInsert->values[j-1];
        }
		
		toInsert->num_keys = toInsert->num_keys + 1;
		toInsert->keys[k] = key;
		toInsert->values[k] = dummyval_;
		*val = dummyval_;
		assert(*val != NULL);
		dummyval_ = NULL;
		
#if IIBTREE_PROF
		writes++;
#endif
//		leaf->writes++;
//		checkConflict(leaf, 1);
		//printf("IN LEAF2");
		//printTree();
		leaf->seq = leaf->seq + 1;
		return new_sibling;
	}



	
	SecondIndex::Iterator* GetIterator() {
		return new SecondIndexUint64BPlusTree::Iterator(this);
	}

	void printLeaf(LeafNode *n);
	void printInner(InnerNode *n, unsigned depth);
	void PrintStore();
	void PrintList();
	
private:
		
		static __thread RTMArena* arena_;	  // Arena used for allocations of nodes
		static __thread bool localinit_;
		static __thread SecondNode *dummyval_;
		static __thread MemNodeWrapper *dummywrapper_;
		
		char padding1[64];
		void *root;
		int depth;

		char padding2[64];
		RTMProfile prof;
		char padding3[64];
  		port::SpinLock slock;
#if IIBTREE_PROF
		uint64_t reads;
		uint64_t writes;
		uint64_t calls;
#endif		
		char padding4[64];
	    SpinLock rtmlock;
		char padding5[64];
		
/*		
		int current_tid;
		void *waccess[4][30];
		void *raccess[4][30];
		int windex[4];
		int rindex[4];*/
			
		
};

//__thread RTMArena* MemstoreBPlusTree::arena_ = NULL;
//__thread bool MemstoreBPlusTree::localinit_ = false;

}

#endif
