#ifndef STORAGE_LEVELDB_INCLUDE_TXLEVELDB_H_
#define STORAGE_LEVELDB_INCLUDE_TXLEVELDB_H_

#include "txdb.h"
#include "leveldb/db.h"

namespace leveldb {

class TXLeveldb: public TXDB {

private:
	
 DB* db_;
	
public:

  TXLeveldb(DB* db);
  virtual ~TXLeveldb();

  virtual Status Put(const Slice& key,
                     const Slice& value, uint64_t seq);

//  virtual Status PutBatch(const Slice& key,
  //                   const Slice& value, uint64_t seq) = 0;

  virtual Status Get(const Slice& key,
                     std::string* value, uint64_t seq);
  
  virtual Status Delete(const Slice& key, uint64_t seq);

private:

  Slice getInternalKey(const Slice& key, uint64_t seq);

};

}

#endif
