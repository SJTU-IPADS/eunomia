#ifndef MYBPLUSTREE_H
#define MYBPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include "util/rtmScope.h" 
#include "util/rtm_arena.h"

#define M  10
#define N  10


//static uint64_t writes = 0;

//static uint64_t reads = 0;
	
static int total_key = 0;
static int total_nodes = 0;
static uint64_t rconflict = 0;
static uint64_t wconflict = 0;
namespace leveldb {
class BPlusTree {


	
	struct LeafNode {
//		LeafNode() : num_keys(0){}//, writes(0), reads(0) {}
//		uint64_t padding[4];
		unsigned num_keys;
		uint64_t 	 keys[M];
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[4];
	};
		 
	struct InnerNode {
//		InnerNode() : num_keys(0) {}//, writes(0), reads(0) {}
//		uint64_t padding[8];
//		unsigned padding;
		unsigned num_keys;
		uint64_t 	 keys[N];
		void*	 children[N+1];
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[8];
	};

/*	
	struct NewNodes {
		NewNodes(int depth) {
			d = depth + 6;
			leaf = new LeafNode();
			leaf->num_keys = 0;
			inner = new InnerNode *[d];
			used = 0;
			for (int i=0; i<d; i++) {
				inner[i] = new InnerNode();
				inner[i]->num_keys = 0;
				//if ((uint64_t)inner[i] % 64 !=0) printf("not align\n");
				//printf("%lx \n",inner[i]);
			}
		}
		unsigned d;
		unsigned used;
		LeafNode *leaf;
		InnerNode **inner;
						
	};

*/

public:	
	BPlusTree() {
		root = new LeafNode();
		depth = 0;
//		printf("root addr %lx\n", &root);
//		printf("depth addr %lx\n", &depth);
/*		for (int i=0; i<4; i++) {
			windex[i] = 0;
			rindex[i] = 0;
		}*/
	}


	
	~BPlusTree() {
		prof.reportAbortStatus();
		//printf("rwconflict %ld\n", rconflict);
		//printf("wwconflict %ld\n", wconflict);
		//printf("depth %d\n",depth);
		//printf("reads %ld\n",reads);
		//printf("writes %ld\n", writes);
		//printTree();
		//top();
	}

	void threadlocalinit(){
		if(false == localinit_) {
			arena_ = new RTMArena();
			localinit_ = true;
		}
			
	}

	
	inline LeafNode* new_leaf_node() {
			LeafNode* result = (LeafNode *)(arena_->AllocateAligned(sizeof(LeafNode)));
			#if 0 
			while(_xtest())
				_xend();
			printf("new_leaf_node\n");
			#endif
			return result;
	}
		
	inline InnerNode* new_inner_node() {
			InnerNode* result = (InnerNode *)(arena_->AllocateAligned(sizeof(InnerNode)));

			#if 0 
			while(_xtest())
				_xend();
			printf("new_inner_node\n");
			#endif
			
			return result;
	}
	
	bool find(uint64_t key) {
		InnerNode* inner;
		register void* node= root;
		register unsigned d= depth;
		unsigned index = 0;
		while( d-- != 0 ) {
				index = 0;
				inner= reinterpret_cast<InnerNode*>(node);
				while((index < inner->num_keys) && (key >= inner->keys[index])) {
				   ++index;
				}				
				node= inner->children[index];
		}
		LeafNode* leaf= reinterpret_cast<LeafNode*>(node);
		
		unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
		   ++k;
		}
		if( leaf->keys[k] == key ) {
				return true;
		} else {
				return false;
		}
	}

	void insert(uint64_t key, int tid) {
		threadlocalinit();
//		NewNodes *dummy= new NewNodes(depth);
//		NewNodes dummy(depth);

/*		MutexSpinLock lock(&slock);
		current_tid = tid;
		windex[tid] = 0;
		rindex[tid] = 0;
	*/	
		
		Insert_rtm(key);
//		Insert_rtm(key, &dummy);

/*		if (dummy->leaf->num_keys <=0) delete dummy->leaf;
		for (int i=dummy->used; i<dummy->d;i++) {
			delete dummy->inner[i];
			//if (dummy.inner[i]->num_keys > 0) printf("!!!\n");
		}*/
//		delete dummy;
		
	}
	
	inline void Insert_rtm(uint64_t key) {
//		MutexSpinLock lock(&slock);
		RTMArenaScope begtx(&rtmlock, &prof, arena_);
		if (depth == 0) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root));
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
//				writes++;
//				inner->writes++;
			}
//			else checkConflict(&root, 0);
		}
		else {
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth);
			
		}
	}

	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d) {
	
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;
		//printf("key %lx\n",key);
		//printf("d %d\n",d);
		while((k < inner->num_keys) && (key >= inner->keys[k])) {
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
			
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child));
			//printTree();
			if (new_leaf != NULL) {
				InnerNode *toInsert = inner;
				if (inner->num_keys == N) {										
					unsigned treshold= (N+1)/2;
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
					inner->keys[N-1] = upKey;
//					checkConflict(new_sibling, 1);
//					writes++;
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
//				writes++;
//				inner->writes++;
			}
			else {
//				reads++;
//				inner->reads++;
//				checkConflict(inner, 0);
			}
			
//			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
		}
		else {
			//printf("inner insert\n");
			bool s = true;
			InnerNode *new_inner = InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1);
			
			
			if (new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;
				unsigned treshold= (N+1)/2;
				if (inner->num_keys == N) {										
					
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
					inner->keys[N-1] = upKey;

//					writes++;
//					new_sibling->writes++;
//					checkConflict(new_sibling, 1);
				}	
				
				for (int i=toInsert->num_keys; i>k; i--) {
					toInsert->keys[i] = toInsert->keys[i-1];
					toInsert->children[i+1] = toInsert->children[i];					
				}
			
				toInsert->num_keys++;
				toInsert->keys[k] = reinterpret_cast<InnerNode*>(child)->keys[N-1];

				toInsert->children[k+1] = child_sibling;
														
//				writes++;
//				inner->writes++;
//				checkConflict(inner, 1);
			}
			else {
//				reads++;
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

//			writes++;
//			new_root->writes++;
//			checkConflict(new_root, 1);
//			checkConflict(&root, 1);
		}
//		else if (d == depth) checkConflict(&root, 0);
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling;
	}

	inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf) {
		LeafNode *new_sibling = NULL;
		unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
		   ++k;
		}

		LeafNode *toInsert = leaf;
		if (leaf->num_keys == M) {
			new_sibling = new_leaf_node();
			unsigned threshold= (M+1)/2;
			new_sibling->num_keys= leaf->num_keys -threshold;
            for(unsigned j=0; j < new_sibling->num_keys; ++j) {
            	new_sibling->keys[j]= leaf->keys[threshold+j];                               
            }
            leaf->num_keys= threshold;
			

			if (k>=threshold) {
				k = k - threshold;
				toInsert = new_sibling;
			}
//			writes++;
//			new_sibling->writes++;
//			checkConflict(new_sibling, 1);
		}
		
		
		//printf("IN LEAF1 %d\n",toInsert->num_keys);
		//printTree();

        for (int j=toInsert->num_keys; j>k; j--) 
			toInsert->keys[j] = toInsert->keys[j-1];

		toInsert->num_keys = toInsert->num_keys + 1;
		toInsert->keys[k] = key;
//		writes++;
//		leaf->writes++;
//		checkConflict(leaf, 1);
		//printf("IN LEAF2");
		//printTree();

		return new_sibling;
	}

	
/*	void printLeaf(LeafNode *n) {
		printf("Leaf Key num %d\n", n->num_keys);
		for (int i=0; i<n->num_keys;i++)
			printf("\t%lx  ",n->keys[i]);
			printf("\n");
		total_key += n->num_keys;
	}
*/	
	void printInner(InnerNode *n, unsigned depth) {
		printf("Inner %d Key num %d\n", depth, n->num_keys);
		for (int i=0; i<n->num_keys;i++)
			 printf("\t%lx	",n->keys[i]);
		 printf("\n");
//		for (int i=0; i<=n->num_keys; i++)
//			if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
//			else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}
/*
	void printTree() {
		 printf("========================================\n");
		 total_key = 0;
		 if (depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
		 else {
			  printInner(reinterpret_cast<InnerNode*>(root), depth);
		 }
		 printf("========================================\n");
		 printf("Total key num %d\n", total_key);
	} 

	void topLeaf(LeafNode *n) {
		total_nodes++;
		if (n->writes > 40) printf("Leaf %lx , w %ld , r %ld\n", n, n->writes, n->reads);
		
	}

	void topInner(InnerNode *n, unsigned depth){
		total_nodes++;
		if (n->writes > 40) printf("Inner %lx depth %d , w %ld , r %ld\n", n, depth, n->writes, n->reads);
		for (int i=0; i<=n->num_keys;i++)
			if (depth > 1) topInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
			else topLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}
	
	void top(){
		if (depth == 0) topLeaf(reinterpret_cast<LeafNode*>(root));
		else topInner(reinterpret_cast<InnerNode*>(root), depth);
		printf("TOTAL NODES %d\n", total_nodes);
	}
	*/

/*	void checkConflict(void *node, int mode) {
		if (mode == 1) {
			waccess[current_tid][windex[current_tid]] = node;
			windex[current_tid]++;
			for (int i= 0; i<4; i++) {
				if (i==current_tid) continue;
				for (int j=0; j<windex[i]; j++)
					if (node == waccess[i][j]) wconflict++;
				for (int j=0; j<rindex[i]; j++)
					if (node == raccess[i][j]) rconflict++;
			}
		}
		else {
			raccess[current_tid][rindex[current_tid]] = node;
			rindex[current_tid]++;
			for (int i= 0; i<4; i++) {
				if (i==current_tid) continue;
				for (int j=0; j<windex[i]; j++)
					if (node == waccess[i][j]) rconflict++;
			}
		}
		
	}*/
private:
		
		static __thread RTMArena* arena_;	  // Arena used for allocations of nodes
		static __thread bool localinit_;
		
		char padding1[64];
		void *root;
		int depth;

		char padding2[64];
		RTMProfile prof;
  		port::SpinLock slock;
		char padding3[64];
	    SpinLock rtmlock;
		char padding4[64];

/*		
		int current_tid;
		void *waccess[4][30];
		void *raccess[4][30];
		int windex[4];
		int rindex[4];*/
		
};

__thread RTMArena* BPlusTree::arena_ = NULL;
__thread bool BPlusTree::localinit_ = false;

}

#endif
