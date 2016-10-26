#include "memstore/secondindex_stringbplustree.h"

namespace leveldb {
	
	__thread RTMArena* SecondIndexStringBPlusTree::arena_ = NULL;
	__thread bool SecondIndexStringBPlusTree::localinit_ = false;
	__thread SecondIndex::SecondNode* SecondIndexStringBPlusTree::dummyval_ = NULL;
	__thread SecondIndex::MemNodeWrapper* SecondIndexStringBPlusTree::dummywrapper_ = NULL;

	void SecondIndexStringBPlusTree::printLeaf(LeafNode *n) {
			printf("Leaf Key num %d\n", n->num_keys);
			for (int i=0; i<n->num_keys;i++)
				printf("key  %s value %p \t ",n->keys[i], n->values[i]);
				printf("\n");
			total_key += n->num_keys;
		}
	

	void SecondIndexStringBPlusTree::printInner(InnerNode *n, unsigned depth) {
		printf("Inner %d Key num %d\n", depth, n->num_keys);
		for (int i=0; i<n->num_keys;i++)
			 printf("\t%p	",n->keys[i]);
		 printf("\n");
		for (int i=0; i<=n->num_keys; i++)
			if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
			else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}

	void SecondIndexStringBPlusTree::PrintStore() {
		 printf("===============B+ Tree=========================\n");
		 total_key = 0;
		 if (depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
		 else {
			  printInner(reinterpret_cast<InnerNode*>(root), depth);
		 }
		 printf("========================================\n");
		 printf("Total key num %d\n", total_key);
	} 

	void SecondIndexStringBPlusTree::PrintList() {
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
		
	SecondIndexStringBPlusTree::Iterator::Iterator(SecondIndexStringBPlusTree* tree)
	{
		tree_ = tree;
		node_ = NULL;
	}
	
	uint64_t* SecondIndexStringBPlusTree::Iterator::GetLink()
	{
		return link_;
	}
	
	uint64_t SecondIndexStringBPlusTree::Iterator::GetLinkTarget()
	{
		return target_;
	}
	
	
	// Returns true iff the iterator is positioned at a valid node.
	bool SecondIndexStringBPlusTree::Iterator::Valid()
	{
		bool b = node_ != NULL && node_->num_keys > 0;
	//	printf("b %d\n",b);
		return b;
	}
	
	// Advances to the next position.
	// REQUIRES: Valid()
	bool SecondIndexStringBPlusTree::Iterator::Next()
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
				while (k < num)  {
					int tmp = tree_->Compare(key_, node_->keys[k]);
					if (tmp < 0) break;
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
	bool SecondIndexStringBPlusTree::Iterator::Prev()
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
			  while (k < num)  {
			  	  int tmp = tree_->Compare(key_ , node_->keys[k]);
				  if (tmp <= 0) break;
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
	
	uint64_t SecondIndexStringBPlusTree::Iterator::Key()
	{
		return (uint64_t)key_;
	}
	
	SecondIndex::SecondNode* SecondIndexStringBPlusTree::Iterator::CurNode()
	{
		if (!Valid()) return NULL;
		return value_;
	}
	
	// Advance to the first entry with a key >= target
	void SecondIndexStringBPlusTree::Iterator::Seek(uint64_t key)
	{
		RTMArenaScope begtx(&tree_->rtmlock, &tree_->prof, tree_->arena_);
		LeafNode *leaf = tree_->FindLeaf((char *)key);		
		link_ = (uint64_t *)(&leaf->seq);
		target_ = leaf->seq;		
		int num = leaf->num_keys;
		int k = 0; 
		while (k < num)  {
	//	   printf("a %s\n",key +4);
	//	   printf("b %s\n",leaf->keys[k] +4);
		   int tmp = tree_->Compare((char *)key, leaf->keys[k]);
		   
		   if (tmp <= 0) break;
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
	
	void SecondIndexStringBPlusTree::Iterator::SeekPrev(uint64_t key)
	{
		LeafNode *leaf = tree_->FindLeaf((char *)key);
		link_ = (uint64_t *)(&leaf->seq);
		target_ = leaf->seq;		
		
		int k = 0; 
		int num = leaf->num_keys;
		while (k < num)  {
		   int tmp = tree_->Compare((char *)key, leaf->keys[k]);
		   if (tmp <= 0) break;
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
	void SecondIndexStringBPlusTree::Iterator::SeekToFirst()
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
	void SecondIndexStringBPlusTree::Iterator::SeekToLast()
	{
		//TODO
		assert(0);
	}


}

