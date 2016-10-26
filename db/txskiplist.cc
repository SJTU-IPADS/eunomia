// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/txskiplist.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

TXSkiplist::TXSkiplist(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      table_(comparator_, &arena_) {
}

TXSkiplist::~TXSkiplist() {

}

int TXSkiplist::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class TXSkiplistIterator: public Iterator {
 public:
  explicit TXSkiplistIterator(TXSkiplist::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  TXSkiplist::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  TXSkiplistIterator(const TXSkiplistIterator&);
  void operator=(const TXSkiplistIterator&);
};

Iterator* TXSkiplist::NewIterator() {
  return new TXSkiplistIterator(&table_);
}

Status TXSkiplist::Put(const Slice& key,
					   const Slice& value, uint64_t seq)
{
	//
	Table::ThreadLocalInit();
	
	size_t key_size = key.size();
  	size_t val_size = value.size();
  	size_t internal_key_size = key_size + 8;
  	const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
	
  	char* buf = (char *)malloc(encoded_len);
  	char* p = EncodeVarint32(buf, internal_key_size);
  	memcpy(p, key.data(), key_size);
  	p += key_size;
  	EncodeFixed64(p, (seq << 8) | kTypeValue);
 	p += 8;
    p = EncodeVarint32(p, val_size);
	//printf("Put\n");
    memcpy(p, value.data(), val_size);
    assert((p + val_size) - buf == encoded_len);
    table_.Insert(buf);

	return Status::OK();
}

Status TXSkiplist::Get(const Slice& key,
					   Slice* value, uint64_t seq)
{

	  Table::ThreadLocalInit();
	  
	  size_t key_size = key.size();
  	  size_t internal_key_size = key_size + 8;
  	  const size_t encoded_len = VarintLength(internal_key_size) + internal_key_size;
	
  	  char* buf = new char[encoded_len];
  	  char* p = EncodeVarint32(buf, internal_key_size);
  	  memcpy(p, key.data(), key_size);
  	  p += key_size;
  	  EncodeFixed64(p, (seq << 8) | kTypeValue);
	
	  Table::Iterator iter(&table_);
	  iter.Seek(buf);

	  delete[] buf;
	  
	  if (iter.Valid()) {
		// entry format is:
		//	  klength  varint32
		//	  userkey  char[klength]
		//	  tag	   uint64
		//	  vlength  varint32
		//	  value    char[vlength]
		const char* entry = iter.key();
		uint32_t key_length;
		Status s;
		const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
		if (comparator_.comparator.user_comparator()->Compare(
							Slice(key_ptr, key_length - 8), key) == 0) {
				
		  	// Correct user key
		  
		  	uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);

		  	if(tag != ((seq << 8) | kTypeValue)) 
				return Status::NotFound(Slice());

		  
			*value = GetLengthPrefixedSlice(key_ptr + key_length);
	 		//value->assign(v.data(), v.size());
		    
		  	return Status::OK();

		}
	}

	 return Status::NotFound(Slice());
}


Status TXSkiplist::Delete(const Slice& key, uint64_t seq) 
{
	Table::ThreadLocalInit();
	
	return Status::NotFound(Slice());
}



void TXSkiplist::DumpTXSkiplist()
{

  Table::Iterator iter(&table_);
  iter.SeekToFirst();
  while(iter.Valid()) {
	const char* entry = iter.key();
	uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
	Slice k = Slice(key_ptr, key_length - 8);
	const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
	uint64_t seq = tag>>8;
	Slice v = GetLengthPrefixedSlice(key_ptr + key_length);

	mutex_.Lock();
	printf("Key %s ", k.ToString().c_str());
	printf("Seq %ld ", seq);
	printf("Value %s", v.ToString().c_str());
	printf("\n");
	mutex_.Unlock();
	iter.Next();
  }
}

Status TXSkiplist::GetMaxSeq(const Slice& key, uint64_t* seq)
{
    size_t key_size = key.size();
  	size_t internal_key_size = key_size + 8;
  	const size_t encoded_len = VarintLength(internal_key_size) + internal_key_size;
	
  	char* buf = new char[encoded_len];
  	char* p = EncodeVarint32(buf, internal_key_size);
  	memcpy(p, key.data(), key_size);
  	p += key_size;

  	EncodeFixed64(p, 0xffffff | kTypeValue);
	
	Table::Iterator iter(&table_);
	iter.Seek(buf);

	delete[] buf;
	
	if (iter.Valid()) {
    	const char* entry = iter.key();
    	uint32_t key_length;
    	const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    	if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key) == 0) {

      	uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
	  	*seq = (tag >>8);
        return Status::OK();
      }
    }
  return Status::NotFound(Slice());
}


}  // namespace leveldb
