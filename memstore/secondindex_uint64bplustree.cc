#include "memstore/secondindex_uint64bplustree.h"

namespace leveldb {
	
	__thread RTMArena* SecondIndexUint64BPlusTree::arena_ = NULL;
	__thread bool SecondIndexUint64BPlusTree::localinit_ = false;
	__thread SecondIndex::SecondNode* SecondIndexUint64BPlusTree::dummyval_ = NULL;
	__thread SecondIndex::MemNodeWrapper* SecondIndexUint64BPlusTree::dummywrapper_ = NULL;

	void SecondIndexUint64BPlusTree::printLeaf(LeafNode *n) {
			printf("Leaf Key num %u\n", n->num_keys);
			for (int i=0; i<n->num_keys;i++){
				printf("key  %ld value %p \n",n->keys[i], n->values[i]);
				SecondIndex::SecondNode *sn = n->values[i];
				MemNodeWrapper *w = sn->head;
				while (w!=NULL) {
					printf("%lu %d %p\t ",w->key, w->valid, w->memnode->value);
					w = w->next;
				}
				printf("\n");
			}
				
//			total_key += n->num_keys;
		}
	

	void SecondIndexUint64BPlusTree::printInner(InnerNode *n, unsigned depth) {
		printf("Inner %d Key num %d\n", depth, n->num_keys);
		for (int i=0; i<n->num_keys;i++)
			 printf("\t%lx	",n->keys[i]);
		 printf("\n");
		for (int i=0; i<=n->num_keys; i++)
			if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
			else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}

	void SecondIndexUint64BPlusTree::PrintStore() {
		
		 printf("===============B+ Tree=========================\n");
//		 total_key = 0;
		 if (depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
		 else {
			  printInner(reinterpret_cast<InnerNode*>(root), depth);
		 }
		 printf("========================================\n");
//		 printf("Total key num %d\n", total_key);
	} 

	void SecondIndexUint64BPlusTree::PrintList() {
		void* min = root;
		int d = depth;
		while (d > 0) {
			min = reinterpret_cast<InnerNode*>(min)->children[0]; 
			d--;
		}
		LeafNode *leaf = reinterpret_cast<LeafNode*>(min);
		while (leaf != NULL) {
			printLeaf(leaf);
			if (leaf->right != NULL)
				assert(leaf->right->left == leaf);
			leaf = leaf->right;
		}
			
	}
	/*

	void MemstoreBPlusTree::topLeaf(LeafNode *n) {
		total_nodes++;
		if (n->writes > 40) printf("Leaf %lx , w %ld , r %ld\n", n, n->writes, n->reads);
		
	}

	void MemstoreBPlusTree::topInner(InnerNode *n, unsigned depth){
		total_nodes++;
		if (n->writes > 40) printf("Inner %lx depth %d , w %ld , r %ld\n", n, depth, n->writes, n->reads);
		for (int i=0; i<=n->num_keys;i++)
			if (depth > 1) topInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
			else topLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}
	
	void MemstoreBPlusTree::top(){
		if (depth == 0) topLeaf(reinterpret_cast<LeafNode*>(root));
		else topInner(reinterpret_cast<InnerNode*>(root), depth);
		printf("TOTAL NODES %d\n", total_nodes);
	}

	void MemstoreBPlusTree::checkConflict(void *node, int mode) {
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
		
	SecondIndexUint64BPlusTree::Iterator::Iterator(SecondIndexUint64BPlusTree* tree)
	{
		tree_ = tree;
		node_ = NULL;
	}
	
	uint64_t* SecondIndexUint64BPlusTree::Iterator::GetLink()
	{
		return link_;
	}
	
	uint64_t SecondIndexUint64BPlusTree::Iterator::GetLinkTarget()
	{
		return target_;
	}
	
	
	// Returns true iff the iterator is positioned at a valid node.
	bool SecondIndexUint64BPlusTree::Iterator::Valid()
	{
		bool b = node_ != NULL && node_->num_keys > 0;
	//	printf("b %d\n",b);
		return b;
	}
	
	// Advances to the next position.
	// REQUIRES: Valid()
	bool SecondIndexUint64BPlusTree::Iterator::Next()
	{
		//get next different key

		//if (node_->seq != seq_) printf("%d %d\n",node_->seq,seq_);
		bool b = true;
		RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
		if (node_->seq != seq_) {
			b = false;
			while (node_ != NULL) {
				int k = 0; 
				int num = node_->num_keys;
				while (k < num && key_ >= node_->keys[k])  {
			   		++k;
				}
				if (k == num) {
					node_ = node_->right;
					if (node_ == NULL) return b;
				}
				else {
					leaf_index = k;
					break;
				}
			}
			
		}
		else leaf_index++;
		if (leaf_index >= node_->num_keys) {
			node_ = node_->right;
			leaf_index = 0;		
			if (node_ != NULL){
				link_ = (uint64_t *)(&node_->seq);
				target_ = node_->seq;		
			}
		}
		if (node_ != NULL) {
			key_ = node_->keys[leaf_index];
			value_ = node_->values[leaf_index];
			seq_ = node_->seq;
		}
		return b;
	}
	
	// Advances to the previous position.
	// REQUIRES: Valid()
	bool SecondIndexUint64BPlusTree::Iterator::Prev()
	{
	  // Instead of using explicit "prev" links, we just search for the
	  // last node that falls before key.
	  assert(Valid());
	
	  //FIXME: This function doesn't support link information
	//  printf("PREV\n");
	  bool b = true;
	  RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
	  if (node_->seq != seq_) {
	  	  b = false;
		  while (node_ != NULL) {
			  int k = 0; 
			  int num = node_->num_keys;
			  while (k < num && key_ > node_->keys[k])  {
				  ++k;
			  }
			  if (k == num) {
			  	  if (node_->right == NULL) break;
				  node_ = node_->right;
			  }
			  else {
				  leaf_index = k;
				  break;
			  }
		  }
	  }
	 // printf("id %d\n",leaf_index);
	  leaf_index--;
	  if (leaf_index < 0) {
	  	node_ = node_->left;	
		//if (node_ != NULL) printf("NOTNULL\n");
		if (node_ != NULL) {
			leaf_index = node_->num_keys - 1;
			link_ = (uint64_t *)(&node_->seq);
			target_ = node_->seq;		
		}
	  }
	  
	  if (node_ != NULL) {
		  key_ = node_->keys[leaf_index];
		  value_ = node_->values[leaf_index];
		  seq_ = node_->seq;
	  }
	  return b;
	}
	
	uint64_t SecondIndexUint64BPlusTree::Iterator::Key()
	{
		return (uint64_t)key_;
	}
	
	SecondIndex::SecondNode* SecondIndexUint64BPlusTree::Iterator::CurNode()
	{
		if (!Valid()) return NULL;
		return value_;
	}
	
	// Advance to the first entry with a key >= target
	void SecondIndexUint64BPlusTree::Iterator::Seek(uint64_t key)
	{
		RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
		LeafNode *leaf = tree_->FindLeaf(key);		
		link_ = (uint64_t *)(&leaf->seq);
		target_ = leaf->seq;		
		int num = leaf->num_keys;
		int k = 0; 
		while (k < num && key > leaf->keys[k])  {
		   ++k;
		}
		if (k == num) {
			node_ = leaf->right;
			leaf_index = 0;
			if(node_ == NULL)
				return;
		}
		else {
			leaf_index = k;
			node_ = leaf;
		}		
		seq_ = node_->seq;
		key_ = node_->keys[leaf_index];
		value_ = node_->values[leaf_index];
	}
	
	void SecondIndexUint64BPlusTree::Iterator::SeekPrev(uint64_t key)
	{
		LeafNode *leaf = tree_->FindLeaf(key);
		link_ = (uint64_t *)(&leaf->seq);
		target_ = leaf->seq;		
		
		int k = 0; 
		int num = leaf->num_keys;
		while (k < num && key > leaf->keys[k])  {
		   ++k;
		}
		if (k == 0) {
			node_ = leaf->left;			
			link_ = (uint64_t *)(&node_->seq);
			target_ = node_->seq;		
			leaf_index = node_->num_keys - 1;						
		}
		else {
			k = k - 1;
			node_ = leaf;
		}
	}
	
	
	// Position at the first entry in list.
	// Final state of iterator is Valid() iff list is not empty.
	void SecondIndexUint64BPlusTree::Iterator::SeekToFirst()
	{
		void* min = tree_->root;
		int d = tree_->depth;
		while (d > 0) {
			min = reinterpret_cast<InnerNode*>(min)->children[0]; 
			d--;
		}
		node_ = reinterpret_cast<LeafNode*>(min);
		link_ = (uint64_t *)(&node_->seq);
		target_ = node_->seq;		
		leaf_index = 0;
		RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
		key_ = node_->keys[0];
		value_ = node_->values[0];
		seq_ = node_->seq;
	}
	
	// Position at the last entry in list.
	// Final state of iterator is Valid() iff list is not empty.
	void SecondIndexUint64BPlusTree::Iterator::SeekToLast()
	{
		//TODO
		assert(0);
	}


}

