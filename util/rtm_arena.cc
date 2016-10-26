// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/rtm_arena.h"
#include <assert.h>
#include <stdio.h>
#include <immintrin.h>


#define CACHSIM 0

static const int kBlockSize = 16*4096;

RTMArena::RTMArena() {
  blocks_memory_ = 0;
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;

#if CACHSIM
  for(int i = 0; i < 64; i ++)
  	cacheset[i] = 0;
  cachelineaddr = 0;
#endif
}

RTMArena::~RTMArena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
  	//printf("Free %lx\n", blocks_[i]);
    delete[] blocks_[i];
  }
  
#if CACHSIM
  for(int i = 0; i < 64; i ++)
  	printf("cacheset[%d] %d ", i, cacheset[i]);
  printf("\n");
#endif
}

char* RTMArena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

void RTMArena::AllocateFallback() {

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  for(int i = 0; i < kBlockSize; i+=4096)
  	alloc_ptr_[i] = 0;
  
  alloc_bytes_remaining_ = kBlockSize;
}

char* RTMArena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  //printf("Allocate %lx\n", result);
  blocks_memory_ += block_bytes;
  blocks_.push_back(result);
  return result;
}

