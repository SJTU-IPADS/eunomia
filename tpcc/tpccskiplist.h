// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>

#include "db/dbtx.h"
#include "db/dbrotx.h"
#include "db/dbtables.h"
#include "memstore/memstore_skiplist.h"


#include "tpccdb.h"

#define PROFILEDELIVERY 0

namespace leveldb {

typedef int64_t Key;
typedef uint64_t Value;


class TPCCSkiplist : public TPCCDB {
 public :
  
  DBTables* store;
  uint64_t abort;
  uint64_t conflict;
  uint64_t capacity;

  uint64_t neworderabort;
  uint64_t paymentabort;
  uint64_t stocklevelabort;
  uint64_t orderstatusabort;
  uint64_t deliverabort;

  uint64_t newordernum;
  uint64_t paymentnum;
  uint64_t stocklevelnum;
  uint64_t orderstatusnum;
  uint64_t delivernum;

  uint64_t neworderreadcount;
  uint64_t neworderwritecount;
  uint64_t paymentreadcount;
  uint64_t paymentwritecount;
  uint64_t stocklevelitercount;
  uint64_t stocklevelreadcount;
  uint64_t orderstatusitercount;
  uint64_t orderstatusreadcount;
  uint64_t deliverreadcount;
  uint64_t deliverwritecount;
  uint64_t deliveritercount;

  uint64_t neworderreadmax;
  uint64_t neworderwritemax;
  uint64_t paymentreadmax;
  uint64_t paymentwritemax;
  uint64_t stocklevelitermax;
  uint64_t stocklevelreadmax;
  uint64_t orderstatusitermax;
  uint64_t orderstatusreadmax;
  uint64_t deliverreadmax;
  uint64_t deliverwritemax;
  uint64_t deliveritermax;

  uint64_t neworderreadmin;
  uint64_t neworderwritemin;
  uint64_t paymentreadmin;
  uint64_t paymentwritemin;
  uint64_t stocklevelitermin;
  uint64_t stocklevelreadmin;
  uint64_t orderstatusitermin;
  uint64_t orderstatusreadmin;
  uint64_t deliverreadmin;
  uint64_t deliverwritemin;
  uint64_t deliveritermin;

#if PROFILEBUFFERNODE
  int bufferMiss;
  int bufferHit;
  int bufferGet;
#endif

#if PROFILEDELIVERY
  uint64_t dstep1;
  uint64_t dstep2;
  uint64_t dstep3;
  uint64_t dstep4;
  uint64_t search;
  uint64_t traverse;
  uint64_t traverseCount;
  uint64_t seekCount;
#endif
  char *fstart;
  char *fend;
  static __thread Warehouse *warehouse_dummy;
  static __thread District *district_dummy;
  static __thread Customer *customer_dummy;
  static __thread Order *order_dummy;
  static __thread OrderLine *orderline_dummy;
  static __thread Stock *stock_dummy;
  static __thread History *history_dummy;
  static __thread NewOrder *neworder_dummy;
  static __thread std::vector<uint64_t> *vector_dummy;
  static __thread uint64_t *array_dummy;
  static __thread uint64_t secs;
  inline unsigned long rdtsc(void)                                                                                                                      
  {
        unsigned a, d;                                                                                                                              
        __asm __volatile("rdtsc":"=a"(a), "=d"(d));
        return ((unsigned long)a) | (((unsigned long) d) << 32);                                                                                    
  }

  TPCCSkiplist();
  ~TPCCSkiplist();
  void ThreadLocalInit();
  
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
  virtual void delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
  		  std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo);

  //not used yet
  virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
  		  TPCCUndo** undo);
  virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
  		  int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
  		  TPCCUndo** undo);
  virtual bool hasWarehouse(int32_t warehouse_id);
  virtual void applyUndo(TPCCUndo* undo);
  virtual void freeUndo(TPCCUndo* undo);

  virtual void printSkiplist();
  
  virtual bool newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
  		  const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
  		  TPCCUndo** undo);
};
}  
