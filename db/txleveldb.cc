#include "txleveldb.h"
#include "util/coding.h"

#include <malloc.h>

namespace leveldb {

TXLeveldb::TXLeveldb(DB* db)
{
	db_ = db;
}

TXLeveldb::~TXLeveldb() {}

Status TXLeveldb::Put(const Slice& key,
                     const Slice& value, uint64_t seq)
{	
	return db_->Put(WriteOptions(), getInternalKey(key, seq), value);	
}

//  virtual Status PutBatch(const Slice& key,
  //                   const Slice& value, uint64_t seq) = 0;

Status TXLeveldb::Get(const Slice& key,
                     std::string* value, uint64_t seq)
{
	return db_->Get(ReadOptions(), getInternalKey(key, seq), value);
}
  
Status TXLeveldb::Delete(const Slice& key, uint64_t seq)
{
	return db_->Delete(WriteOptions(), getInternalKey(key, seq));
}

Slice TXLeveldb::getInternalKey(const Slice& key, uint64_t seq)
{
	size_t key_size = key.size();
  	size_t internal_key_size = key_size + 8;
	
  	char* buf = (char *)malloc(internal_key_size);
  	char* p = EncodeVarint32(buf, internal_key_size);
  	memcpy(p, key.data(), key_size);
  	p += key_size;
  	EncodeFixed64(p, seq);
	
  	return Slice(buf, internal_key_size);
}

}
