// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include "db/hashtable_template.h"
#include "db/dbtransaction_template.h"
#include "db/txmemstore_template.h"

#include "tpccdb.h"


namespace leveldb {

typedef int64_t Key;
typedef uint64_t Value;

class KeyComparator : public Comparator {
    public:
	int operator()(const Key& a, const Key& b) const {
		if (a < b) {
	      return -1;
	    } else if (a > b) {
	      return +1;
	    } else {
	      return 0;
	    }
	}

	virtual int Compare(const Slice& a, const Slice& b) const {
		assert(0);
		return 0;
	}

	virtual const char* Name()  const {
		assert(0);
		return 0;
	};

   virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit)  const {
		assert(0);

	}
  
   virtual void FindShortSuccessor(std::string* key)  const {
		assert(0);

	}

};

class KeyHash : public HashFunction  {

    public:

	uint64_t MurmurHash64A ( const void * key, int len, unsigned int seed )	{

		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;
		uint64_t h = seed ^ (len * m);
		const uint64_t * data = (const uint64_t *)key;
		const uint64_t * end = data + (len/8);

		while(data != end)	{
			uint64_t k = *data++;
			k *= m; 
			k ^= k >> r; 
			k *= m; 	
			h ^= k;
			h *= m; 
		}

		const unsigned char * data2 = (const unsigned char*)data;

		switch(len & 7)	{
  		  case 7: h ^= uint64_t(data2[6]) << 48;
		  case 6: h ^= uint64_t(data2[5]) << 40;
		  case 5: h ^= uint64_t(data2[4]) << 32;
		  case 4: h ^= uint64_t(data2[3]) << 24;
		  case 3: h ^= uint64_t(data2[2]) << 16;
		  case 2: h ^= uint64_t(data2[1]) << 8;
		  case 1: h ^= uint64_t(data2[0]);
		  		  h *= m;
		};

		h ^= h >> r;
		h *= m;
		h ^= h >> r;	

		return h;
	} 

	virtual int64_t hash(int64_t& key)	{
		return key;
//		return MurmurHash64A((void *)&key, 8, 0);
	}
};

class TPCCTxMemStore : public TPCCDB {
 public :
  HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> *seqs;
  TXMemStore<leveldb::Key, leveldb::Value, leveldb::KeyComparator>* store;
  KeyComparator *cmp;
  uint64_t abort;
  uint64_t conflict;
  uint64_t capacity;

  TPCCTxMemStore();
  ~TPCCTxMemStore();
  
  void insertWarehouse(const Warehouse & warehouse);
  void insertItem(const Item& item);
  void insertStock(const Stock & stock);
  void insertDistrict(const District & district);
  void insertCustomer(const Customer & customer);
  Order* insertOrder(const Order & order);
  NewOrder* insertNewOrder(int32_t w_id,int32_t d_id,int32_t o_id);
  History* insertHistory(const History & history);
  OrderLine* insertOrderLine(const OrderLine & orderline);
  
	
  virtual bool newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			  const std::vector<NewOrderItem>& items, const char* now,
			  NewOrderOutput* output, TPCCUndo** undo);
  virtual bool newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
			  const std::vector<NewOrderItem>& items, const char* now,
			  NewOrderOutput* output, TPCCUndo** undo);


  //not used yet
  virtual bool newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
  		  const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
  		  TPCCUndo** undo);
  virtual int32_t stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold);
  virtual void orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id, OrderStatusOutput* output);
  virtual void orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last, OrderStatusOutput* output);
  
  virtual void payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
  		  PaymentOutput* output, TPCCUndo** undo);
  virtual void payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, const char* c_last, float h_amount, const char* now,
  		  PaymentOutput* output, TPCCUndo** undo);
  virtual void paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, int32_t c_id, float h_amount, const char* now,
  		  PaymentOutput* output, TPCCUndo** undo);
  virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
  		  TPCCUndo** undo);
  virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
  		  TPCCUndo** undo);
  virtual void delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
  		  std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo);
  virtual bool hasWarehouse(int32_t warehouse_id);
  virtual void applyUndo(TPCCUndo* undo);
  virtual void freeUndo(TPCCUndo* undo);

  
};
}  
