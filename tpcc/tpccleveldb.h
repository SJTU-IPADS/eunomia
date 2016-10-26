// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/txskiplist.h"
#include <string>
#include "util/random.h"
#include "db/hashtable.h"
#include "tpccdb.h"
#include "port/port.h"
#include "util/mutexlock.h"



namespace leveldb {

class TPCCLevelDB : public TPCCDB {
 public:

  uint32_t warehouse_num;
  port::Mutex* storemutex;
  HashTable *latestseq_ ;
  TXSkiplist* memstore_ ;	

  int wcount;
  int rcount;
  TPCCLevelDB(uint32_t w_num, HashTable* ht, TXSkiplist* store, port::Mutex* mutex); 
  TPCCLevelDB();
  
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


  Slice* marshallWarehouseKey(int32_t w_id);
  Slice* marshallWarehouseValue(const Warehouse& w);
  float getW_TAX(Slice& value);
  Slice* marshallDistrictKey(int32_t d_w_id, int32_t d_id);
  Slice* marshallDistrictValue(const District& d);
  float getD_TAX(Slice& value);
  int32_t getD_NEXT_O_ID(Slice& value);
  Slice* updateD_NEXT_O_ID(Slice& value, int32_t id);
  Slice* marshallCustomerKey(int32_t c_w_id, int32_t c_d_id, int32_t c_id);
  Slice* marshallCustomerValue(const Customer& c);
  float getC_DISCOUNT(Slice& value);
  char* getC_LAST(Slice& value);
  char* getC_CREDIT(Slice& value);
  Slice* marshallOrderKey(int32_t o_w_id, int32_t o_d_id, int32_t o_id);
  Slice* marshallOrderValue(Order o);
  Slice* marshallNewOrderKey(const NewOrder& no);
  Slice* marshallStockKey(int32_t s_w_id, int32_t s_i_id);
  Slice* marshallStockValue(Stock s);
  Stock* unmarshallStockValue(Slice& value);
  Slice* marshallItemKey(int32_t i_id);
  Slice* marshallItemValue(Item i);
  Item* unmarshallItemValue(Slice& value);
  Slice* marshallOrderLineKey(int32_t ol_w_id, int32_t ol_d_id, int32_t ol_o_id, int32_t ol_number);
  Slice* marshallOrderLineValue(OrderLine line);
  //char* getS_DATA(std::string& value);
  //char** getS_DIST(std::string& value);

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

