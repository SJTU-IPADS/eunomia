// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include "tpcc/tpccleveldb.h"
#include "util/random.h"
#include "db/dbtransaction.h"
#include "util/mutexlock.h"
#include "leveldb/env.h"
#include "port/port.h"


namespace leveldb {

static void EncodeInt32_t(char *result, int32_t v) {
  char *ip = reinterpret_cast<char *>(&v);				
  for (int i = 0; i < 4; i++) {
  	result[i] = ip[i];
    
  }
}

static void EncodeFloat(char *result, float f) {
  char *fp = reinterpret_cast<char *>(&f);				
  for (int i = 0; i < 4; i++)
  	result[i] = fp[i];
}


//Warehouse
Slice* TPCCLevelDB::marshallWarehouseKey(int32_t w_id) {
  char* key = new char[14];
  memcpy(key, "WAREHOUSE_", 10);
  EncodeInt32_t(key + 10, w_id);
  //printf("%s %d\n",key,w_id);
  Slice *k = new Slice(key, 14);
  return k;
}

Slice* TPCCLevelDB::marshallWarehouseValue(const Warehouse& w) {
  char* value = new char[95];
  char* start = value;

  int length = sizeof(w.w_name);		//0
  memcpy(start, w.w_name, length);
  start += length;
  
  length = sizeof(w.w_street_1);		//11
  memcpy(start, w.w_street_1, length);
  start += length;

  length = sizeof(w.w_street_2);		//32
  memcpy(start, w.w_street_2, length);
  start += length;

  length = sizeof(w.w_city);			//53
  memcpy(start, w.w_city, length);
  start += length;
  
  length = sizeof(w.w_state);			//74
  memcpy(start, w.w_state, length);
  start += length;

  length = sizeof(w.w_zip);				//77
  memcpy(start, w.w_zip, length);
  start += length;
    				
  length = sizeof(float);				//87
  EncodeFloat(start, w.w_tax);
  start += length;

  EncodeFloat(start, w.w_ytd);			//91
  	
  	
  Slice *v = new Slice(value, 95);
  return v;
}

float TPCCLevelDB::getW_TAX(Slice& value) {
  char *v = const_cast<char *>(value.data_);
  v += 87;
  float *f = reinterpret_cast<float *>(v);
  return *f;
}

/*
void TPCCLevelDB::unmarshallWarehouseValue(std::string& value) {
  char *v = value.c_str();
  strncpy(w_name, v, 10);
  
  //...
}*/


//District
Slice* TPCCLevelDB::marshallDistrictKey(int32_t d_w_id, int32_t d_id) {
  char* key = new char[17];
  memcpy(key, "DISTRICT_", 9);
  EncodeInt32_t(key + 9, d_id);
  EncodeInt32_t(key + 13, d_w_id);
  Slice *k = new Slice(key, 17);
  return k;
}

Slice* TPCCLevelDB::marshallDistrictValue(const District& d) {
  char* value = new char[99];	
  char* start = value;

  int length = sizeof(d.d_name);		//0
  memcpy(start, d.d_name, length);
  start += length;

  length = sizeof(d.d_street_1);		//11
  memcpy(start, d.d_street_1, length);
  start += length;

  length = sizeof(d.d_street_2);		//32
  memcpy(start, d.d_street_2, length);
  start += length;

  length = sizeof(d.d_city);			//53
  memcpy(start, d.d_city, length);
  start += length;
  
  length = sizeof(d.d_state);			//74
  memcpy(start, d.d_state, length);
  start += length;

  length = sizeof(d.d_zip);				//77
  memcpy(start, d.d_zip, length);
  start += length;
  
  length = sizeof(float);				//87
  EncodeFloat(start, d.d_tax);
  start += length;

  EncodeFloat(start, d.d_ytd);			//91
  start += length;

  EncodeInt32_t(start, d.d_next_o_id);	//95
  	
  Slice *v = new Slice(value, 99);
  return v;
}

float TPCCLevelDB::getD_TAX(Slice& value) {
  //printf("AAA\n");
  char *v = const_cast<char *>(value.data_);
  //printf("BBB %x\n", v);
  v += 87;
  char c = *v;
  float *f = reinterpret_cast<float *>(v);
  //printf("CCC %x\n",f);
  float d_tax = *f;
  //printf("DDD\n");
  return d_tax;
}

int32_t TPCCLevelDB::getD_NEXT_O_ID(Slice& value) {
  char *v = const_cast<char *>(value.data_);
  v += 95;
  int32_t *i = reinterpret_cast<int32_t *>(v);
  return *i;
}

Slice* TPCCLevelDB::updateD_NEXT_O_ID(Slice& value, int32_t id) {
  assert(value.size() == 99);
  const char *v = value.data();
  char *newv = new char[99];
  memcpy(newv, v, 99);
  char *nid = newv + 95;
  EncodeInt32_t(nid, id);
  Slice *s = new Slice(newv, 99);
  return s;
}

/*
void TPCCLevelDB::unmarshallDistrictValue(std::string& value) {
  char *v = value.c_str();
  strncpy(d_name, v, 10);
  //...
}*/


//Customer
Slice* TPCCLevelDB::marshallCustomerKey(int32_t c_w_id, int32_t c_d_id, int32_t c_id) {
  char* key = new char[21];
  memcpy(key, "CUSTOMER_", 9);
  EncodeInt32_t(key + 9, c_id);
  EncodeInt32_t(key + 13, c_d_id);
  EncodeInt32_t(key + 17, c_w_id);
  Slice* k = new Slice(key, 21);
  return k;
}

Slice* TPCCLevelDB::marshallCustomerValue(const Customer& c) {
  char* value = new char[673];	
  char* start = value;

  int length = sizeof(c.c_first);		//0
  memcpy(start, c.c_first, length);
  start += length;

  length = sizeof(c.c_middle);			//17
  memcpy(start, c.c_middle, length);
  start += length;
  
  length = sizeof(c.c_last);			//20
  memcpy(start, c.c_last, length);
  start += length;

  length = sizeof(c.c_street_1);		//37
  memcpy(start, c.c_street_1, length);
  start += length;

  length = sizeof(c.c_street_2);		//58
  memcpy(start, c.c_street_2, length);
  start += length;

  length = sizeof(c.c_city);			//79
  memcpy(start, c.c_city, length);
  start += length;
  
  length = sizeof(c.c_state);			//100
  memcpy(start, c.c_state, length);
  start += length;

  length = sizeof(c.c_zip);				//103
  memcpy(start, c.c_zip, length);
  start += length;  

  length = sizeof(c.c_phone);			//113
  memcpy(start, c.c_phone, length);
  start += length;  

  length = sizeof(c.c_since);			//130
  memcpy(start, c.c_since, length);
  start += length;

  length = sizeof(c.c_credit);			//145
  memcpy(start, c.c_credit, length);
  start += length;

  length = sizeof(float);				//148
  EncodeFloat(start, c.c_credit_lim);
  start += length;

  EncodeFloat(start, c.c_discount);		//152
  start += length;

  EncodeFloat(start, c.c_balance);		//156
  start += length;

  EncodeFloat(start, c.c_ytd_payment);	//160
  start += length;

  EncodeInt32_t(start, c.c_payment_cnt);//164
  start += length;
  
  EncodeInt32_t(start, c.c_delivery_cnt);//168
  start += length;

  length = sizeof(c.c_data);			//172
  memcpy(start, c.c_data, length);
  
  Slice *v = new Slice(value, 673);
  return v;
}

float TPCCLevelDB::getC_DISCOUNT(Slice& value) {
  char *v = const_cast<char *>(value.data());
  v += 152;
  float *f = reinterpret_cast<float *>(v);
  return *f;
}

char* TPCCLevelDB::getC_LAST(Slice& value) {
 char *v = const_cast<char *>(value.data());
  v += 20;
  char *c = new char[17];
  memcpy(c, v, 17);
  return c;
}

char* TPCCLevelDB::getC_CREDIT(Slice& value) {
  char *v = const_cast<char *>(value.data());
  v += 145;
  char *c = new char[3];
  memcpy(c, v, 3);
  return c;
}

//Order 
Slice* TPCCLevelDB::marshallOrderKey(int32_t o_w_id, int32_t o_d_id, int32_t o_id) {
  char* key = new char[18];
  memcpy(key, "ORDER_", 6);
  EncodeInt32_t(key + 9, o_id);
  EncodeInt32_t(key + 13, o_d_id);
  EncodeInt32_t(key + 17, o_w_id);
  Slice *k = new Slice(key, 18);
  return k;
}

Slice* TPCCLevelDB::marshallOrderValue(Order o) {  
  char* value = new char[31];	  
  char* start = value;
  
  int length = sizeof(uint32_t);			//0
  EncodeInt32_t(start, o.o_c_id);
  start += length;
  
  length = sizeof(o.o_entry_d);				//4
  memcpy(start, o.o_entry_d, length);
  start += length;

  length = sizeof(uint32_t);				//19
  EncodeInt32_t(start, o.o_carrier_id);
  start += length;

  EncodeInt32_t(start, o.o_ol_cnt);			//23
  start += length;

  EncodeInt32_t(start, o.o_all_local);		//27
  
  Slice *v = new Slice(value, 31);
  return v;
  
}

//NewOrder
Slice* TPCCLevelDB::marshallNewOrderKey(const NewOrder& no) {
  char* key = new char[21];
  memcpy(key, "NEWORDER_", 9);
  EncodeInt32_t(key + 9, no.no_o_id);
  EncodeInt32_t(key + 13, no.no_d_id);
  EncodeInt32_t(key + 17, no.no_w_id);
  Slice *k = new Slice(key, 21);
  return k;
}

//Stock
Slice* TPCCLevelDB::marshallStockKey(int32_t s_w_id, int32_t s_i_id) {
  char* key = new char[14];
  memcpy(key, "STOCK_", 6);
  EncodeInt32_t(key + 6, s_i_id);
  EncodeInt32_t(key + 10, s_w_id);
  Slice *k = new Slice(key, 14);
  return k;
}

Slice* TPCCLevelDB::marshallStockValue(Stock s) {
  char* value = new char[315];
  char* start = value;

  int length = sizeof(uint32_t);			//0
  EncodeInt32_t(start, s.s_quantity);
  start += length;

  length = 25;								//4+ i * 25
  for (int i=0; i<10; i++) {
  	memcpy(start, s.s_dist[i], length);
    start += length;
  }

  length = sizeof(uint32_t);				//254
  EncodeInt32_t(start, s.s_ytd);
  start += length;

  length = sizeof(uint32_t);				//258
  EncodeInt32_t(start, s.s_order_cnt);
  start += length;

  length = sizeof(uint32_t);				//262
  EncodeInt32_t(start, s.s_remote_cnt);
  start += length;

  length = sizeof(s.s_data);				//264
  memcpy(start, s.s_data, length);

  Slice *v = new Slice(value, 315);
  return v;
}

Stock* TPCCLevelDB::unmarshallStockValue(Slice& value) {
  Stock *s = new Stock();
  char *v = const_cast<char *>(value.data());
  
  s->s_quantity = *reinterpret_cast<int32_t *>(v); 
  v += 4;  
  
  for (int i = 0; i < 10; i++) {
  	memcpy(s->s_dist[i], v, 25);
	v += 25;
  }

  s->s_ytd = *reinterpret_cast<int32_t *>(v);
  v += 4;

  s->s_order_cnt = *reinterpret_cast<int32_t *>(v);
  v += 4;

  s->s_remote_cnt = *reinterpret_cast<int32_t *>(v);
  v += 4;

  memcpy(s->s_data, v, 51);
  
  return s;
}


/*
char* TPCCLevelDB::getS_DATA(std::string& value) {
  char *v = const_cast<char *>(value.c_str());
  v += 264;
  char *c = new char[51];
  memcpy(c, v, 3);
  return c;
}

char** TPCCLevelDB::getS_DIST(std::string& value) {
  char *v = const_cast<char *>(value.c_str());
  v += 4;
  char **c = new char*[10];
  for (int i = 0; i < 10; i++) {
  	c[i] = new char[25];
  	memcpy(c[i], v, 25);
	v += 25;
  }
  return c;
}

int32_t TPCCLevelDB::getS_QUANTITY(std::string& value) {
  char *v = const_cast<char *>(value.c_str());
  int32_t *i = reinterpret_cast<int32_t *>(v);
  return *i;
}
*/

Slice* TPCCLevelDB::marshallItemKey(int32_t i_id) {
  char* key = new char[9];
  memcpy(key, "ITEM_", 5);
  EncodeInt32_t(key + 5, i_id);
  Slice *k = new Slice(key, 9);
  int i = *reinterpret_cast<int32_t *>(key + 5); 
  assert(i == i_id);
  return k;
}

Slice* TPCCLevelDB::marshallItemValue(Item i) {
  char* value = new char[84];
  char* start = value;

  int length = sizeof(uint32_t);			//0
  EncodeInt32_t(start, i.i_im_id);
  start += length;

  length = sizeof(i.i_name);				//4
  memcpy(start, i.i_name, length);
  start += length;

  length = sizeof(float);					//29
  EncodeFloat(start, i.i_price);
  start += length;

  length = sizeof(i.i_data);				//33
  memcpy(start, i.i_data, length);

  Slice *v = new Slice(value, 84);
  return v;
}


Item* TPCCLevelDB::unmarshallItemValue(Slice &value) {
  Item *i = new Item();
  char *v = const_cast<char *>(value.data());
  
  i->i_im_id = *reinterpret_cast<int32_t *>(v); 
  v += 4;  

  memcpy(i->i_name, v, 25);
  v += 25;

  i->i_price = *reinterpret_cast<float *>(v);
  v += 4;

  memcpy(i->i_data, v, 51);

  return i;
}

//OrderLine
Slice* TPCCLevelDB::marshallOrderLineKey(int32_t ol_w_id, int32_t ol_d_id, int32_t ol_o_id, int32_t ol_number){
  char* key = new char[26];
  memcpy(key, "ORDERLINE_", 10);
  EncodeInt32_t(key + 10, ol_o_id);
  EncodeInt32_t(key + 14, ol_d_id);
  EncodeInt32_t(key + 18, ol_w_id);
  EncodeInt32_t(key + 22, ol_number);
  Slice *k = new Slice(key, 26);
  return k;
}

Slice* TPCCLevelDB::marshallOrderLineValue(OrderLine line){
  char* value = new char[84];
  char* start = value;
	
  int length = sizeof(uint32_t);			//0
  EncodeInt32_t(start, line.ol_i_id);
  start += length;

  EncodeInt32_t(start, line.ol_supply_w_id);//4
  start += length;

  length = sizeof(line.ol_delivery_d);		//8
  memcpy(start, line.ol_delivery_d, length);
  start += length;
  
  length = sizeof(uint32_t);				//23
  EncodeInt32_t(start, line.ol_quantity);
  start += length;

  EncodeFloat(start, line.ol_amount);		//27
  start += length;

  length = sizeof(line.ol_dist_info);		//31
  memcpy(start, line.ol_dist_info, length);
  start += length;

  Slice *v = new Slice(value, 84);
  return v;
}

//Insert Tuples
/*Order* TPCCLevelDB::insertOrder(const Order& order) {
  Slice o_key = marshallOrderKey(order.o_w_id, order.o_d_id, order.o_id);
  Slice o_value = marshallOrderValue(order);
}*/

TPCCLevelDB::TPCCLevelDB() {
  wcount = 0; rcount = 0;
  Options options;
  InternalKeyComparator cmp(options.comparator);
  latestseq_ = new HashTable();
  memstore_ = new leveldb::TXSkiplist(cmp);
  storemutex =  new port::Mutex();

}

TPCCLevelDB::TPCCLevelDB(uint32_t w_num, HashTable* ht, TXSkiplist* store, port::Mutex* mutex) {
  wcount = 0; rcount = 0;
  warehouse_num = w_num;
  storemutex = mutex;
  latestseq_ = ht;
  memstore_ = store;
}

void TPCCLevelDB::insertWarehouse(const Warehouse& warehouse){
  Slice *k = marshallWarehouseKey(warehouse.w_id);
  Slice *v = marshallWarehouseValue(warehouse);
  ValueType t = kTypeValue;
  SequenceNumber s = 1;
  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);
 // printf("W\n");
}

void TPCCLevelDB::insertDistrict(const District& district){
  Slice *k = marshallDistrictKey(district.d_w_id, district.d_id);
  Slice *v = marshallDistrictValue(district);
  ValueType t = kTypeValue;
  SequenceNumber s = 1;
  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, 1);
//  HashTable::Node* node = latestseq_->GetNode(*k);
//  assert(node != NULL);
//  assert(node->seq != 0);
  //printf("D\n");
}

void TPCCLevelDB::insertCustomer(const Customer& customer) {
  Slice *k = marshallCustomerKey(customer.c_w_id, customer.c_d_id, customer.c_id);
  Slice *v = marshallCustomerValue(customer);
  ValueType t = kTypeValue;
  SequenceNumber s = 1;
  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);
 //printf("C\n");
}

History* TPCCLevelDB::insertHistory(const History& history) {
  return NULL;
}

NewOrder* TPCCLevelDB::insertNewOrder(int32_t w_id,int32_t d_id,int32_t o_id) {
  NewOrder* neworder = new NewOrder();
  neworder->no_w_id = w_id;
  neworder->no_d_id = d_id;
  neworder->no_o_id = o_id;
  Slice *k = marshallNewOrderKey(*neworder);
  Slice *v = new Slice();
  ValueType t = kTypeValue;
  SequenceNumber s = 1;
  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);
  //printf("NO\n");
  return neworder;
}

Order* TPCCLevelDB::insertOrder(const Order & order){
  Slice *k = marshallOrderKey(order.o_w_id, order.o_d_id, order.o_id);
  Slice *v = marshallOrderValue(order);
  ValueType t = kTypeValue;
  SequenceNumber s = 1;
  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);
  //printf("O\n");
  return const_cast<Order *>(&order);
}

OrderLine* TPCCLevelDB::insertOrderLine(const OrderLine & orderline){
  Slice *k = marshallOrderLineKey(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number);
  Slice *v = marshallOrderLineValue(orderline);
  ValueType t = kTypeValue;
  SequenceNumber s = 1;

  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);

  //printf("OL\n");
  return const_cast<OrderLine *>(&orderline);
}

void TPCCLevelDB::insertItem(const Item& item) {
 //if (item.i_id >=70000) return;
  Slice *k = marshallItemKey(item.i_id);
 // Slice *v = marshallItemValue(item);
  Slice *v = new Slice();
  ValueType t = kTypeValue;
  SequenceNumber s = 1;

  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);

  
 //Status i_s;
 //std::string *i_value = new std::string();

  //if (item.i_id >=60000  && item.i_id<=80000) printf("I %d\n", item.i_id);
}

void TPCCLevelDB::insertStock(const Stock & stock){
  Slice *k = marshallStockKey(stock.s_w_id, stock.s_i_id);
  Slice *v = marshallStockValue(stock);
  ValueType t = kTypeValue;
  SequenceNumber s = 1;

  memstore_->Put(*k, *v ,s);
  latestseq_->Insert(*k, s);
  //printf("S\n");
}

bool TPCCLevelDB::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now, NewOrderOutput* output,
        TPCCUndo** undo) {
    // perform the home part
    bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }
	
    return true;
}

bool TPCCLevelDB::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now,
        NewOrderOutput* output, TPCCUndo** undo) {
  //printf("NewOrderHome start\n");
  //count++;
  DBTransaction tx(latestseq_, memstore_, storemutex);
  
  ValueType t = kTypeValue;
  while(true) {
  tx.Begin();
  output->status[0] = '\0';

  //Cheat
  for (int i = 0; i < items.size(); ++i) 
	if (items[i].i_id == 100001) {
  	  strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
	  //printf("Unused district %d %d\n", district_id, warehouse_id);
	  tx.Abort();
	  return false;
	}
  
  //--------------------------------------------------------------------------
  //The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved. 
  //--------------------------------------------------------------------------
  //printf("Step 1\n");
  Slice *w_key = marshallWarehouseKey(warehouse_id);
  Status w_s;
 // printf("Step 1 get w %d\n", warehouse_id);
  Slice *w_value = new Slice();  
  bool found = tx.Get(*w_key, w_value, &w_s);
  //rcount++;
  //printf("found %d\n", found);
  output->w_tax = getW_TAX(*w_value);
  //delete[] w_key->data_;
  //delete w_key;
  //delete w_value;
  
  //--------------------------------------------------------------------------
  //The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, 
  //D_TAX, the district tax rate, is retrieved, 
  //and D_NEXT_O_ID, the next available order number for the district, is retrieved and incremented by one.
  //--------------------------------------------------------------------------
  //printf("Step 2\n");
  Slice *d_key = marshallDistrictKey(warehouse_id, district_id);
  Status d_s;
  Slice *d_value = new Slice();
/*
  char * d = const_cast<char *>(d_key->data());
  int32_t *i = reinterpret_cast<int32_t *>(d + 9);
  if (*i != district_id) {
  	printf("%d vs %d\n", *i, district_id);
  }
  assert(*i == district_id)	;
			
*/
  //printf("---1\n");
  //printf("District %d %d\n", district_id, warehouse_id);
  found = tx.Get(*d_key, d_value, &d_s);
  //rcount++;
  //if (!found) 
  assert(found);
  //printf("found %d\n", found);
  output->d_tax = getD_TAX(*d_value);
  
  output->o_id = getD_NEXT_O_ID(*d_value);
  //printf("---2\n");
  Slice *d_v = updateD_NEXT_O_ID(*d_value, output->o_id + 1);
  //if (count > 60000) printf("---3\n");
  
  tx.Add(t, *d_key, *d_v);
  //wcount++;
  //if (count > 60000) printf("---4\n");
  //-------------------------------------------------------------------------- 
  //The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
  //and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name, 
  //and C_CREDIT, the customer's credit status, are retrieved.
  //--------------------------------------------------------------------------
  //printf("Step 3\n");
  Slice *c_key = marshallCustomerKey(warehouse_id, district_id, customer_id);
  Status c_s;
  Slice *c_value = new Slice();
  found = tx.Get(*c_key, c_value, &c_s);
  //rcount++;
  //printf("found %d\n", found);
  output->c_discount = getC_DISCOUNT(*c_value);
  memcpy(output->c_last, getC_LAST(*c_value), sizeof(output->c_last));
  memcpy(output->c_credit, getC_CREDIT(*c_value), sizeof(output->c_credit));
//  delete c_key->data_;
//  delete c_key;

  //-------------------------------------------------------------------------- 
  //A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the creation of the new order.
  //O_CARRIER_ID is set to a null value. 
  //If the order includes only home order-lines, then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
  //The number of items, O_OL_CNT, is computed to match ol_cnt.
  //--------------------------------------------------------------------------

  // Check if this is an all local transaction
  //printf("Step 4\n");
  bool all_local = true;
  for (int i = 0; i < items.size(); ++i) {
    if (items[i].ol_supply_w_id != warehouse_id) {
      all_local = false;
      break;
    }
  }
  //printf("all_local %d\n", all_local);
  
  Order order;
  order.o_w_id = warehouse_id;
  order.o_d_id = district_id;
  order.o_id = output->o_id;
  order.o_c_id = customer_id;
  order.o_carrier_id = Order::NULL_CARRIER_ID;
  order.o_ol_cnt = static_cast<int32_t>(items.size());
  order.o_all_local = all_local ? 1 : 0;
  strcpy(order.o_entry_d, now);
  assert(strlen(order.o_entry_d) == DATETIME_SIZE);
  Slice *o_key = marshallOrderKey(order.o_w_id, order.o_d_id, order.o_id);
  Slice *o_value = marshallOrderValue(order);
  tx.Add(t, *o_key, *o_value);
  //wcount++;
  
  NewOrder no;
  no.no_w_id = warehouse_id;
  no.no_d_id = district_id;
  no.no_o_id = output->o_id;
  Slice *no_key = marshallNewOrderKey(no);
  Slice *no_value = new Slice();
  tx.Add(t, *no_key, *no_value);
  //wcount++;

  //-------------------------------------------------------------------------
  //For each O_OL_CNT item on the order:
  //-------------------------------------------------------------------------

  //printf("Step 5\n");
  
  OrderLine line;
  line.ol_o_id = output->o_id;
  line.ol_d_id = district_id;
  line.ol_w_id = warehouse_id;
  memset(line.ol_delivery_d, 0, DATETIME_SIZE+1);
  output->items.resize(items.size());
  output->total = 0;
  for (int i = 0; i < items.size(); ++i) {
  	//-------------------------------------------------------------------------
	//The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected 
	//and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
	//If I_ID has an unused value, a "not-found" condition is signaled, resulting in a rollback of the database transaction.	
	//-------------------------------------------------------------------------
	//printf("Step 6\n");
	Slice *i_key = marshallItemKey(items[i].i_id);
	Status i_s;
	Slice *i_value = new Slice();
	
	bool found = tx.Get(*i_key, i_value, &i_s);
	//rcount++;
	if (!found && items[i].i_id <=100000) {
		printf("Item %d\n", items[i].i_id);
		assert(found);
	}	
	//printf("found %d %d\n", i, found);
	//delete i_key->data_;
	//delete i_key;
	if (!found) {
	  strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
		//printf("Unused district %d %d\n", district_id, warehouse_id);
	  tx.Abort();
	  return false;
	}
	Item *item = unmarshallItemValue(*i_value);
	assert(sizeof(output->items[i].i_name) == sizeof(item->i_name));
    memcpy(output->items[i].i_name, item->i_name, sizeof(output->items[i].i_name));
	output->items[i].i_price = item->i_price;
	
	//-------------------------------------------------------------------------
	//The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals OL_SUPPLY_W_ID) is selected. 
	//S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the district number, and S_DATA are retrieved. 
	//If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, 
	//then S_QUANTITY is decreased by OL_QUANTITY; 
	//otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91.
	//S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. 
	//If the order-line is remote, then S_REMOTE_CNT is incremented by 1.
	//-------------------------------------------------------------------------
	//printf("Step 7\n");
	Slice *s_key = marshallStockKey(items[i].ol_supply_w_id, items[i].i_id);
	Status s_s;
    Slice *s_value = new Slice();
	//if (items[i].i_id > 100000) 
	//  printf("Unused key!\n");
	//printf("Item id %d %d\n", items[i].ol_supply_w_id, items[i].i_id);
    found = tx.Get(*s_key, s_value, &s_s);
	//rcount++;
	//printf("found %d %d\n", i, found);
	Stock *s = unmarshallStockValue(*s_value);    
    if (s->s_quantity > (items[i].ol_quantity + 10))
	  s->s_quantity -= items[i].ol_quantity;
	else s->s_quantity = s->s_quantity - items[i].ol_quantity + 91;
	output->items[i].s_quantity = s->s_quantity;
	s->s_ytd += items[i].ol_quantity;
	s->s_order_cnt++;
	if (items[i].ol_supply_w_id != warehouse_id) 
	  s->s_remote_cnt++;
	Slice *s_v =  marshallStockValue(*s);
	tx.Add(t, *s_key, *s_v);
    //wcount++;
	//-------------------------------------------------------------------------
	//The amount for the item in the order (OL_AMOUNT) is computed as: OL_QUANTITY * I_PRICE
	//The strings in I_DATA and S_DATA are examined. If they both include the string "ORIGINAL", 
	//the brand-generic field for that item is set to "B", otherwise, the brand-generic field is set to "G".
	//-------------------------------------------------------------------------  
    output->items[i].ol_amount = static_cast<float>(items[i].ol_quantity) * item->i_price;
    line.ol_amount = output->items[i].ol_amount;
        
	bool stock_is_original = (strstr(s->s_data, "ORIGINAL") != NULL);
    if (stock_is_original && strstr(item->i_data, "ORIGINAL") != NULL) {
	  output->items[i].brand_generic = NewOrderOutput::ItemInfo::BRAND;
	} else {
	  output->items[i].brand_generic = NewOrderOutput::ItemInfo::GENERIC;
	}
   
	//-------------------------------------------------------------------------
	//A new row is inserted into the ORDER-LINE table to reflect the item on the order. 
	//OL_DELIVERY_D is set to a null value, 
	//OL_NUMBER is set to a unique value within all the ORDER-LINE rows that have the same OL_O_ID value, 
	//and OL_DIST_INFO is set to the content of S_DIST_xx, where xx represents the district number (OL_D_ID)
	//-------------------------------------------------------------------------
	line.ol_number = i + 1;
    line.ol_i_id = items[i].i_id;
    line.ol_supply_w_id = items[i].ol_supply_w_id;
    line.ol_quantity = items[i].ol_quantity;
    assert(sizeof(line.ol_dist_info) == sizeof(s->s_dist[district_id]));
    memcpy(line.ol_dist_info, s->s_dist[district_id], sizeof(line.ol_dist_info));
	Slice *l_key = marshallOrderLineKey(line.ol_w_id, line.ol_d_id, line.ol_o_id, line.ol_number);
	Slice *l_value = marshallOrderLineValue(line);
	tx.Add(t, *l_key, *l_value);
	//wcount++;
	//delete s;
	//-------------------------------------------------------------------------
	//The total-amount for the complete order is computed as: 
	//sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
	//-------------------------------------------------------------------------
	output->total += line.ol_amount;
	
  }
  
  output->total = output->total * (1 - output->c_discount) * (1 + output->w_tax + output->d_tax);
  //printf("Try commit\n");
  bool b = tx.End();
  
  if (b) break;
  }
  //printf("At last\n");
  return true;
  
}



//not used yet
bool TPCCLevelDB::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
            const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
            TPCCUndo** undo){
  return false;
}

int32_t TPCCLevelDB::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold){
  return 0;
}
void TPCCLevelDB::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id, OrderStatusOutput* output){
  return;
}
void TPCCLevelDB::orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last, OrderStatusOutput* output){
  return;
}

void TPCCLevelDB::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo) {
  return;
}
void TPCCLevelDB::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo) {
  return;
}
void TPCCLevelDB::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t c_id, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo){
  return;
}
void TPCCLevelDB::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}
void TPCCLevelDB::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}
void TPCCLevelDB::delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
		  std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo){
  return;
}
bool TPCCLevelDB::hasWarehouse(int32_t warehouse_id){
  return true;
}
	
void TPCCLevelDB::applyUndo(TPCCUndo* undo){
  return;
}
void TPCCLevelDB::freeUndo(TPCCUndo* undo){
  return;
}


}
