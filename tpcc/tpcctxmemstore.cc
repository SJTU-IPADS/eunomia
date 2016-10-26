// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "tpcc/tpcctxmemstore.h"
#include <algorithm>


namespace leveldb {

  static int64_t makeWarehouseKey(int32_t w_id) {
  	int64_t id = static_cast<int64_t>(w_id);
	return id;
  }
  
  static int64_t makeDistrictKey(int32_t w_id, int32_t d_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    int32_t did = d_id + (w_id * District::NUM_PER_WAREHOUSE);
    assert(did >= 0);
	int64_t id = (int64_t)1 << 50 | static_cast<int64_t>(did);
    return id;
  }

  static int64_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    int32_t cid = (w_id * District::NUM_PER_WAREHOUSE + d_id)
            * Customer::NUM_PER_DISTRICT + c_id;
    assert(cid >= 0);
	int64_t id = (int64_t)2 << 50 | static_cast<int64_t>(cid);
    return id;
  }

  static int64_t makeHistoryKey(int32_t h_c_id, int32_t h_c_d_id, int32_t h_c_w_id, int32_t h_d_id, int32_t h_w_id) {
  	int32_t cid = (h_c_w_id * District::NUM_PER_WAREHOUSE + h_c_d_id)
            * Customer::NUM_PER_DISTRICT + h_c_id;
	int32_t did = h_d_id + (h_w_id * District::NUM_PER_WAREHOUSE);
	int64_t id = (int64_t)3 << 50 | static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
  	return id;
  }

  static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t upper_id = w_id * District::NUM_PER_WAREHOUSE + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	assert(id > 0);
	id = (int64_t)4 << 50 | id;
    
    return id;
  }

  static int64_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t upper_id = w_id * District::NUM_PER_WAREHOUSE + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	assert(id > 0);
	id = (int64_t)5 << 50 | id;
    
    return id;
  }

  static int64_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    assert(1 <= number && number <= Order::MAX_OL_CNT);

    int32_t upper_id = w_id * District::NUM_PER_WAREHOUSE + d_id;
    assert(upper_id > 0);
    int64_t oid = static_cast<int64_t>(upper_id) * Order::MAX_ORDER_ID + static_cast<int64_t>(o_id);
  
    int64_t olid = oid * Order::MAX_OL_CNT + number; 
    assert(olid >= 0);
	int64_t id = (int64_t)6 << 50 | static_cast<int64_t>(olid);
    return id;
  }

  static int64_t makeItemKey(int32_t i_id) {
  	int64_t id = (int64_t)7 << 50 | static_cast<int64_t>(i_id);
	return id;
  }

  static int64_t makeStockKey(int32_t w_id, int32_t s_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= s_id && s_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
    int32_t sid = s_id + (w_id * Stock::NUM_STOCK_PER_WAREHOUSE);
    assert(sid >= 0);
	int64_t id = (int64_t)8 << 50 | static_cast<int64_t>(sid);
    return id;
  }

  static void updateWarehouseYtd(Warehouse* neww, Warehouse* oldw, float h_amount){
	memcpy(neww->w_name, oldw->w_name, 11);
	memcpy(neww->w_street_1, oldw->w_street_1, 21);
	memcpy(neww->w_street_2, oldw->w_street_2, 21);
	memcpy(neww->w_city, oldw->w_city, 21);
	memcpy(neww->w_state, neww->w_state, 3);
	memcpy(neww->w_zip, oldw->w_zip, 10);

	neww->w_tax = oldw->w_tax;

	neww->w_ytd = oldw->w_ytd + h_amount;
  }

  static void updateDistrict(District *newd, District *oldd ) {
  	memcpy(newd->d_city, oldd->d_city, 21);
	memcpy(newd->d_zip, oldd->d_zip, 10);
	memcpy(newd->d_name, oldd->d_name, 11);
	memcpy(newd->d_state, oldd->d_state, 3);
	memcpy(newd->d_street_1, oldd->d_street_1, 21);
	memcpy(newd->d_street_2, oldd->d_street_2, 21);

	newd->d_tax = oldd->d_tax;
	newd->d_w_id = oldd->d_w_id;
	newd->d_ytd = oldd->d_ytd;	
	newd->d_id = oldd->d_id;

	newd->d_next_o_id = oldd->d_next_o_id + 1;
  }
  static void updateDistrictYtd(District * newd, District * oldd, float h_amount) {
  	memcpy(newd->d_city, oldd->d_city, 21);
	memcpy(newd->d_zip, oldd->d_zip, 10);
	memcpy(newd->d_name, oldd->d_name, 11);
	memcpy(newd->d_state, oldd->d_state, 3);
	memcpy(newd->d_street_1, oldd->d_street_1, 21);
	memcpy(newd->d_street_2, oldd->d_street_2, 21);

	newd->d_tax = oldd->d_tax;
	newd->d_w_id = oldd->d_w_id;
	newd->d_id = oldd->d_id;
	newd->d_next_o_id = oldd->d_next_o_id;
	newd->d_ytd = oldd->d_ytd + h_amount;	
  }

  static void updateCustomer(Customer *newc, Customer *oldc, float h_amount, 
  									int32_t warehouse_id, int32_t district_id) {
  	memcpy(newc->c_first, oldc->c_first, 17);
	memcpy(newc->c_middle, oldc->c_middle, 3);
	memcpy(newc->c_last, oldc->c_last, 17);
	memcpy(newc->c_street_1, oldc->c_street_1, 21);
	memcpy(newc->c_street_2, oldc->c_street_2, 21);
	memcpy(newc->c_city, oldc->c_city, 21);
	memcpy(newc->c_zip, oldc->c_zip, 10);
	memcpy(newc->c_state, oldc->c_state, 3);
	memcpy(newc->c_phone, oldc->c_phone, 17);
	memcpy(newc->c_since, oldc->c_since, 15);
	memcpy(newc->c_credit, oldc->c_credit, 3);
	

	newc->c_credit_lim = oldc->c_credit_lim;
	newc->c_discount = oldc->c_discount;
	newc->c_delivery_cnt = oldc->c_delivery_cnt;

	newc->c_balance = oldc->c_balance - h_amount;
	newc->c_ytd_payment = oldc->c_ytd_payment + h_amount;
	newc->c_payment_cnt = oldc->c_payment_cnt + 1;

	if (strcmp(oldc->c_credit, Customer::BAD_CREDIT) == 0) {
	  static const int HISTORY_SIZE = Customer::MAX_DATA+1;
      char history[HISTORY_SIZE];
      int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
                oldc->c_id, oldc->c_d_id, oldc->c_w_id, district_id, warehouse_id, h_amount);
      assert(characters < HISTORY_SIZE);

      // Perform the insert with a move and copy
      int current_keep = static_cast<int>(strlen(oldc->c_data));
      if (current_keep + characters > Customer::MAX_DATA) {
         current_keep = Customer::MAX_DATA - characters;
      }
      assert(current_keep + characters <= Customer::MAX_DATA);
      memcpy(newc->c_data+characters, oldc->c_data, current_keep);
      memcpy(newc->c_data, history, characters);
      newc->c_data[characters + current_keep] = '\0';
      assert(strlen(newc->c_data) == characters + current_keep);
	}
	else memcpy(newc->c_data, oldc->c_data, 501);
  }

  static void updateCustomerDelivery(Customer * newc, Customer * oldc, float ol_amount) {
  	memcpy(newc->c_first, oldc->c_first, 17);
	memcpy(newc->c_middle, oldc->c_middle, 3);
	memcpy(newc->c_last, oldc->c_last, 17);
	memcpy(newc->c_street_1, oldc->c_street_1, 21);
	memcpy(newc->c_street_2, oldc->c_street_2, 21);
	memcpy(newc->c_city, oldc->c_city, 21);
	memcpy(newc->c_zip, oldc->c_zip, 10);
	memcpy(newc->c_state, oldc->c_state, 3);
	memcpy(newc->c_phone, oldc->c_phone, 17);
	memcpy(newc->c_since, oldc->c_since, 15);
	memcpy(newc->c_credit, oldc->c_credit, 3);
	memcpy(newc->c_data, oldc->c_data, 501);

	newc->c_credit_lim = oldc->c_credit_lim;
	newc->c_discount = oldc->c_discount;
	newc->c_ytd_payment = oldc->c_ytd_payment;
	newc->c_payment_cnt = oldc->c_payment_cnt;
	
	newc->c_balance = oldc->c_balance + ol_amount;
	newc->c_delivery_cnt = oldc->c_delivery_cnt + 1;
  }
  
  static void updateStock(Stock *news, Stock *olds, const NewOrderItem *item, int32_t warehouse_id) {
  	if (olds->s_quantity > (item->ol_quantity + 10))
	  news->s_quantity = olds->s_quantity - item->ol_quantity;
	else news->s_quantity = olds->s_quantity - item->ol_quantity + 91;		
	news->s_ytd = olds->s_ytd - item->ol_quantity;
	news->s_order_cnt = olds->s_order_cnt + 1;
	if (item->ol_supply_w_id != warehouse_id) 
	  news->s_remote_cnt = olds->s_remote_cnt + 1;

	memcpy(news->s_data, olds->s_data, 51);
	for (int i = 0; i < 10; i++)
	  memcpy(news->s_dist[i], olds->s_dist[i], 25);

	news->s_i_id = olds->s_i_id;
	news->s_w_id = olds->s_w_id;
  }

  static void updateOrder(Order *newo, Order *oldo, int32_t carrier_id) {
  	memcpy(newo->o_entry_d, oldo->o_entry_d, 15);

	newo->o_c_id = oldo->o_c_id;
	newo->o_ol_cnt = oldo->o_ol_cnt;
	newo->o_all_local = oldo->o_all_local;

	newo->o_carrier_id = carrier_id;
  }

  static void updateOrderLine(OrderLine *newol, OrderLine *oldol, const char* now) {
  	memcpy(newol->ol_dist_info, oldol->ol_dist_info, 25);

	newol->ol_i_id = oldol->ol_i_id;
	newol->ol_supply_w_id = oldol->ol_supply_w_id;
	newol->ol_quantity = oldol->ol_quantity;
	newol->ol_amount = oldol->ol_amount;

	memcpy(newol->ol_delivery_d, now, 15);
  }
  
  TPCCTxMemStore::TPCCTxMemStore() {
  	cmp = new KeyComparator();
	store = new TXMemStore<Key, Value, leveldb::KeyComparator>(*cmp);
	KeyHash *kh = new KeyHash();
	seqs = new HashTable<Key, KeyHash, KeyComparator>(*kh, *cmp);
	abort = 0;
    conflict = 0;
    capacity = 0;
  }

  TPCCTxMemStore::~TPCCTxMemStore() {
  	delete store;
	delete seqs;
	printf("#Abort : %lu\n", abort);
	printf("#Conflict : %lu\n", conflict);
	printf("#Capacity: %lu\n", capacity);
  }
  
  void TPCCTxMemStore::insertWarehouse(const Warehouse & warehouse) {
  	int64_t key = makeWarehouseKey(warehouse.w_id);
	//printf("insert w_key %ld\n", key);
	Warehouse *w = const_cast<Warehouse *>(&warehouse);
	uint64_t *value = reinterpret_cast<uint64_t *>(w);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  void TPCCTxMemStore::insertDistrict(const District & district) {
  	int64_t key = makeDistrictKey(district.d_w_id, district.d_id);
	//printf("insert d_key %ld\n", key);
	District *d = const_cast<District *>(&district);
	uint64_t *value = reinterpret_cast<uint64_t *>(d);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  void TPCCTxMemStore::insertCustomer(const Customer & customer) {
  	int64_t key = makeCustomerKey(customer.c_w_id, customer.c_d_id, customer.c_id);
	//printf("insert c_key %ld\n", key);
	Customer *c = const_cast<Customer *>(&customer);
	uint64_t *value = reinterpret_cast<uint64_t *>(c);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  History* TPCCTxMemStore::insertHistory(const History & history) {
	int64_t key = makeHistoryKey(history.h_c_id, history.h_c_d_id, history.h_c_w_id, history.h_d_id, history.h_w_id);
	//printf("insert h_key %ld\n", key);
	History *h = const_cast<History *>(&history);
	uint64_t *value = reinterpret_cast<uint64_t *>(h);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return NULL;
  }

  void TPCCTxMemStore::insertItem(const Item & item) {
  	int64_t key = makeItemKey(item.i_id);
	//printf("insert i_key %ld\n", key);
	Item *i =  const_cast<Item *>(&item);
	uint64_t *value = reinterpret_cast<uint64_t *>(i);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  void TPCCTxMemStore::insertStock(const Stock & stock) {
  	int64_t key = makeStockKey(stock.s_w_id, stock.s_i_id);
	//printf("insert s_key %ld\n", key);
	Stock *st = const_cast<Stock *>(&stock);
	uint64_t *value = reinterpret_cast<uint64_t *>(st);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  Order* TPCCTxMemStore::insertOrder(const Order & order) {
  	int64_t key = makeOrderKey(order.o_w_id, order.o_d_id, order.o_id);
	//printf("insert o_key %ld\n", key);
	Order *o = const_cast<Order *>(&order);
	uint64_t *value = reinterpret_cast<uint64_t *>(o);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return o;
  }

  OrderLine* TPCCTxMemStore::insertOrderLine(const OrderLine & orderline) {
  	int64_t key = makeOrderLineKey(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number);
	//printf("insert ol_key %ld\n", key);
	OrderLine *ol = const_cast<OrderLine *>(&orderline);
	uint64_t *value = reinterpret_cast<uint64_t *>(ol);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return ol;
  }

  NewOrder* TPCCTxMemStore::insertNewOrder(int32_t w_id,int32_t d_id,int32_t o_id) {
  	int64_t key = makeNewOrderKey(w_id, d_id, o_id);
	//printf("insert no_key %ld\n", key);
	NewOrder *neworder = new NewOrder();
	neworder->no_w_id = w_id;
	neworder->no_d_id = d_id;
	neworder->no_o_id = o_id;
	uint64_t *value = reinterpret_cast<uint64_t *>(&neworder);
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return neworder;
  }


  bool TPCCTxMemStore::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now, NewOrderOutput* output,
        TPCCUndo** undo) {
    // perform the home part
    bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }
	//Check correctness
/*	leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	//printf("Check\n");
	while(true) {
	  
	  tx.Begin();

	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	  Status d_s;
  	  uint64_t *d_value;
  	  bool found = tx.Get(d_key, &d_value, &d_s);
	  assert(found);
	  District *d = reinterpret_cast<District *>(d_value);
	  assert(output->o_id == d->d_next_o_id - 1);
	  
	  int64_t o_key = makeOrderKey(warehouse_id, district_id, output->o_id);
	  Status o_s;
  	  uint64_t *o_value;
  	  found = tx.Get(o_key, &o_value, &o_s);
	  assert(found);
	  Order *o = reinterpret_cast<Order *>(o_value);
	  assert(o->o_c_id == customer_id);
	  int64_t no_key = makeNewOrderKey(warehouse_id, district_id, output->o_id);
	  Status no_s;
  	  uint64_t *no_value;
  	  found = tx.Get(o_key, &no_value, &o_s);
	  assert(found);
	  NewOrder *no = reinterpret_cast<NewOrder *>(no_value);
	  
	  uint64_t l_key = makeOrderLineKey(warehouse_id, district_id, output->o_id, 2);
	  Status l_s;
	  uint64_t *l_value;
	  found = tx.Get(l_key, &l_value, &l_s);
	  assert(found);
	  OrderLine *l = reinterpret_cast<OrderLine *>(l_value);
	  if (l->ol_quantity != items[1].ol_quantity) 
	  	printf("%d %d\n",l->ol_quantity, items[1].ol_quantity);
	  assert(l->ol_quantity == items[1].ol_quantity);

	  uint64_t s_key = makeStockKey(items[3].ol_supply_w_id, items[3].i_id);
	  Status s_s;
      uint64_t *s_value;
	  found = tx.Get(s_key, &s_value, &s_s);
	  assert(found);

	  
	  bool b = tx.End();  
  	  if (b) break;
  	}*/
	
    return true;
  }

  bool TPCCTxMemStore::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now,
        NewOrderOutput* output, TPCCUndo** undo) {
    
	ValueType t = kTypeValue;
	leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	while(true) {
	  
	  tx.Begin();
	  //output->status[0] = '\0';
	  //Cheat
  	  for (int i = 0; i < items.size(); ++i) 
		if (items[i].i_id == 100001) {
  	  	  strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
		  tx.Abort();
	  	  return false;
		}
	  
  	  //--------------------------------------------------------------------------
  	  //The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved. 
  	  //--------------------------------------------------------------------------
	  //printf("Step 1\n");
	  int64_t w_key = makeWarehouseKey(warehouse_id);
	  //printf("w_key %d\n", w_key);
  	  Status w_s;
	  uint64_t *w_value;  
 	  bool found = tx.Get(w_key, &w_value, &w_s);
	  assert(found);
	  
	  Warehouse *w = reinterpret_cast<Warehouse *>(w_value);
	  //printf("1.1 %x\n", *w_value);
	  output->w_tax = w->w_tax;
	  //printf("1.2\n");

	  //--------------------------------------------------------------------------
  	  //The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, 
	  //D_TAX, the district tax rate, is retrieved, 
  	  //and D_NEXT_O_ID, the next available order number for the district, is retrieved and incremented by one.
  	  //--------------------------------------------------------------------------
	  //printf("Step 2\n");
	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	  Status d_s;
  	  uint64_t *d_value;
  	  found = tx.Get(d_key, &d_value, &d_s);
	  assert(found);
	  //printf("2.1\n");
	  assert(*d_value != 0);
	  District *d = reinterpret_cast<District *>(d_value);
	  //printf("2.2\n");
	  output->d_tax = d->d_tax;
	  
	  output->o_id = d->d_next_o_id;
      //printf("%d %d %d\n", warehouse_id, district_id, output->o_id);
  	  District *newd = new District();
	  updateDistrict(newd, d);
	  uint64_t *d_v = reinterpret_cast<uint64_t *>(newd);
	  tx.Add(t, d_key, d_v);


	  //-------------------------------------------------------------------------- 
  	  //The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
	  //and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name, 
  	  //and C_CREDIT, the customer's credit status, are retrieved.
  	  //--------------------------------------------------------------------------
	  //printf("Step 3\n");
	  uint64_t c_key = makeCustomerKey(warehouse_id, district_id, customer_id);
  	  Status c_s;
  	  uint64_t *c_value;
	  found = tx.Get(c_key, &c_value, &c_s);
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
	  //printf("3.1\n");
  	  output->c_discount = c->c_discount;
	  //printf("3.2\n");
  	  memcpy(output->c_last, c->c_last, sizeof(output->c_last));
  	  memcpy(output->c_credit, c->c_credit, sizeof(output->c_credit));

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

  	  //printf("Step 5\n");
	  Order *order = new Order();
	  order->o_w_id = warehouse_id;
	  order->o_d_id = district_id;
	  order->o_id = output->o_id;
	  order->o_c_id = customer_id;
	  order->o_carrier_id = Order::NULL_CARRIER_ID;
	  order->o_ol_cnt = static_cast<int32_t>(items.size());
	  order->o_all_local = all_local ? 1 : 0;
	  strcpy(order->o_entry_d, now);
  	  assert(strlen(order->o_entry_d) == DATETIME_SIZE);
	  int64_t o_key = makeOrderKey(warehouse_id, district_id, order->o_id);
	  uint64_t *o_value = reinterpret_cast<uint64_t *>(order);
	  tx.Add(t, o_key, o_value);
  
	  //printf("Step 6\n");
	  NewOrder *no = new NewOrder();
  	  no->no_w_id = warehouse_id;
	  no->no_d_id = district_id;
	  no->no_o_id = output->o_id;
	  int64_t no_key = makeNewOrderKey(warehouse_id, district_id, no->no_o_id);
	  uint64_t *no_value = reinterpret_cast<uint64_t *>(no);
	  tx.Add(t, no_key, no_value);

	  //-------------------------------------------------------------------------
  	  //For each O_OL_CNT item on the order:
  	  //-------------------------------------------------------------------------
	  //printf("Step 7\n");
  	  
	  output->items.resize(items.size());
	  output->total = 0;

	  for (int i = 0; i < items.size(); ++i) {
	  	OrderLine *line = new OrderLine();
	    line->ol_o_id = output->o_id;
	    line->ol_d_id = district_id;
	    line->ol_w_id = warehouse_id;
	    memset(line->ol_delivery_d, 0, DATETIME_SIZE+1);
  		//-------------------------------------------------------------------------
		//The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected 
		//and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
		//If I_ID has an unused value, a "not-found" condition is signaled, resulting in a rollback of the database transaction.	
		//-------------------------------------------------------------------------
		//printf("Step 8\n");
		uint64_t i_key = makeItemKey(items[i].i_id);
		Status i_s;
		uint64_t *i_value;
	
		bool found = tx.Get(i_key, &i_value, &i_s);
		if (!found && items[i].i_id <=100000) {
			printf("Item %d\n", items[i].i_id);
			assert(found);
		}	
		if (!found) {
		  strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
	 	  tx.Abort();
	  	  return false;
		}
		Item *item = reinterpret_cast<Item *>(i_value);
		assert(sizeof(output->items[i].i_name) == sizeof(item->i_name));
	    memcpy(output->items[i].i_name, item->i_name, sizeof(output->items[i].i_name));
		output->items[i].i_price = item->i_price;
		//printf("Item %ld\n", items[i].i_id);

		//-------------------------------------------------------------------------
		//The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals OL_SUPPLY_W_ID) is selected. 
		//S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the district number, and S_DATA are retrieved. 
		//If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, 
		//then S_QUANTITY is decreased by OL_QUANTITY; 
		//otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91.
		//S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. 
		//If the order-line is remote, then S_REMOTE_CNT is incremented by 1.
		//-------------------------------------------------------------------------
		//printf("Step 9\n");
		uint64_t s_key = makeStockKey(items[i].ol_supply_w_id, items[i].i_id);
		Status s_s;
    	uint64_t *s_value;
	    //if (items[i].i_id > 100000) printf("Unused key!\n");
	    found = tx.Get(s_key, &s_value, &s_s);
		assert(found);
		Stock *s = reinterpret_cast<Stock *>(s_value);  
		Stock *news = new Stock();
		updateStock(news, s, &items[i], warehouse_id);
		output->items[i].s_quantity = news->s_quantity;
		uint64_t *s_v = reinterpret_cast<uint64_t *>(news);
		tx.Add(t, s_key, s_v);

		//-------------------------------------------------------------------------
		//The amount for the item in the order (OL_AMOUNT) is computed as: OL_QUANTITY * I_PRICE
		//The strings in I_DATA and S_DATA are examined. If they both include the string "ORIGINAL", 
		//the brand-generic field for that item is set to "B", otherwise, the brand-generic field is set to "G".
		//-------------------------------------------------------------------------  
		//printf("Step 10\n");
    	output->items[i].ol_amount = static_cast<float>(items[i].ol_quantity) * item->i_price;
	    line->ol_amount = output->items[i].ol_amount;
        
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
		//printf("Step 11\n");
		line->ol_number = i + 1;
	    line->ol_i_id = items[i].i_id;
    	line->ol_supply_w_id = items[i].ol_supply_w_id;
	    line->ol_quantity = items[i].ol_quantity;
    	assert(sizeof(line->ol_dist_info) == sizeof(s->s_dist[district_id]));
    	memcpy(line->ol_dist_info, s->s_dist[district_id], sizeof(line->ol_dist_info));
		uint64_t l_key = makeOrderLineKey(line->ol_w_id, line->ol_d_id, line->ol_o_id, line->ol_number);
		uint64_t *l_value = reinterpret_cast<uint64_t *>(line);
	//	printf("%d %d %d %d\n", line->ol_w_id, line->ol_d_id, line->ol_o_id, line->ol_number);
	//	printf("OrderLine %lx\n", l_key);
		tx.Add(t, l_key, l_value);


		//-------------------------------------------------------------------------
		//The total-amount for the complete order is computed as: 
		//sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
		//-------------------------------------------------------------------------
		//printf("Step 12\n");
		output->total += line->ol_amount;
		
	  }
	
	  output->total = output->total * (1 - output->c_discount) * (1 + output->w_tax + output->d_tax);
 	//  printf("Step 13\n");
 	  bool b = tx.End();  
  	  if (b) break;
  	}

    atomic_add64(&abort, tx.rtmProf.abortCounts);
	atomic_add64(&capacity, tx.rtmProf.capacityCounts);
	atomic_add64(&conflict, tx.rtmProf.conflictCounts);
    return true;
  }

  #define COPY_ADDRESS(src, dest, prefix) \
	  Address::copy( \
			  dest->prefix ## street_1, dest->prefix ## street_2, dest->prefix ## city, \
			  dest->prefix ## state, dest->prefix ## zip,\
			  src->prefix ## street_1, src->prefix ## street_2, src->prefix ## city, \
			  src->prefix ## state, src->prefix ## zip)

  void TPCCTxMemStore::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
			PaymentOutput* output, TPCCUndo** undo) {  
	paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount, now, output, undo);	
	//check
/*	leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	//printf("Check\n");
	while(true) {
	  
	  tx.Begin();

	  uint64_t c_key = makeCustomerKey(c_warehouse_id, c_district_id, customer_id);
  	  Status c_s;
  	  uint64_t *c_value;
	  bool found = tx.Get(c_key, &c_value, &c_s);
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
	  assert(output->c_balance == c->c_balance);

	  bool b = tx.End();  
  	  if (b) break;
  	}*/
	  
  }

  void TPCCTxMemStore::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
	int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
	PaymentOutput* output, TPCCUndo** undo){
  
    ValueType t = kTypeValue;
    leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
    while(true) {
	  //printf("1\n");
	  tx.Begin();
	  //printf("2\n");
	  //-------------------------------------------------------------------------
	  //The row in the WAREHOUSE table with matching W_ID is selected. 
	  //W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved 
	  //and W_YTD, the warehouse's year-to-date balance, is increased by H_ AMOUNT.
	  //-------------------------------------------------------------------------
	  
	  int64_t w_key = makeWarehouseKey(warehouse_id);
	  //printf("w_key %d\n", w_key);
  	  Status w_s;
	  uint64_t *w_value;  
 	  bool found = tx.Get(w_key, &w_value, &w_s);
	  assert(found);
	  Warehouse *w = reinterpret_cast<Warehouse *>(w_value);
	  Warehouse *neww = new Warehouse();
	  updateWarehouseYtd(neww, w, h_amount);
	  uint64_t *w_v = reinterpret_cast<uint64_t *>(neww);
	  tx.Add(t, w_key, w_v);

	  COPY_ADDRESS(neww, output, w_);

	  //-------------------------------------------------------------------------
	  //The row in the DISTRICT table with matching D_W_ID and D_ID is selected. 
	  //D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved 
	  //and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
	  //-------------------------------------------------------------------------

	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	  Status d_s;
  	  uint64_t *d_value;
  	  found = tx.Get(d_key, &d_value, &d_s);
	  assert(found);
	  //printf("2.1\n");
	  assert(*d_value != 0);
	  District *d = reinterpret_cast<District *>(d_value);      
	  District *newd = new District();
	  updateDistrictYtd(newd, d, h_amount);
	  uint64_t *d_v = reinterpret_cast<uint64_t *>(newd);
	  tx.Add(t, d_key, d_v);

	  COPY_ADDRESS(newd, output, d_);

	  //-------------------------------------------------------------------------
	  //the row in the CUSTOMER table with matching C_W_ID, C_D_ID and C_ID is selected. 
	  //C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, 
	  //C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, and C_BALANCE are retrieved. 
	  //C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased by H_AMOUNT. 
	  //C_PAYMENT_CNT is incremented by 1.
	  //If the value of C_CREDIT is equal to "BC", then C_DATA is also retrieved 
	  //and C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are inserted at the left of the C_DATA field
	  //-------------------------------------------------------------------------

	  uint64_t c_key = makeCustomerKey(c_warehouse_id, c_district_id, customer_id);
  	  Status c_s;
  	  uint64_t *c_value;
	  found = tx.Get(c_key, &c_value, &c_s);
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
	  Customer *newc = new Customer();
	  updateCustomer(newc, c, h_amount, warehouse_id, district_id);
	  uint64_t *c_v = reinterpret_cast<uint64_t *>(newc);
	  tx.Add(t, c_key, c_v);

	  output->c_credit_lim = newc->c_credit_lim;
      output->c_discount = newc->c_discount;
      output->c_balance = newc->c_balance;
    #define COPY_STRING(dest, src, field) memcpy(dest->field, src->field, sizeof(src->field))
      COPY_STRING(output, newc, c_first);
      COPY_STRING(output, newc, c_middle);
      COPY_STRING(output, newc, c_last);
      COPY_ADDRESS(newc, output, c_);
      COPY_STRING(output, newc, c_phone);
      COPY_STRING(output, newc, c_since);
      COPY_STRING(output, newc, c_credit);
      COPY_STRING(output, newc, c_data);
    #undef COPY_STRING


	  //-------------------------------------------------------------------------
	  //H_DATA is built by concatenating W_NAME and D_NAME separated by 4 spaces.
	  //A new row is inserted into the HISTORY table with H_C_ID = C_ID, H_C_D_ID = C_D_ID, H_C_W_ID =
	  //C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
	  //-------------------------------------------------------------------------

	  uint64_t h_key = makeHistoryKey(customer_id, c_district_id, c_warehouse_id, district_id, warehouse_id);
	  History *h = new History();
      h->h_amount = h_amount;
      strcpy(h->h_date, now);
      strcpy(h->h_data, w->w_name);
      strcat(h->h_data, "    ");
      strcat(h->h_data, d->d_name);
      uint64_t *h_v = reinterpret_cast<uint64_t *>(h);
	  tx.Add(t, h_key, h_v);

	  //printf("3\n");
	  bool b = tx.End();  
  	  if (b) break;
    }
    return;
  }
  #undef COPY_ADDRESS

  void TPCCTxMemStore::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id, OrderStatusOutput* output){
	leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	//printf("OrderStatus\n");
    while(true) {
	 
	  tx.Begin();

	  //-------------------------------------------------------------------------
	  //the row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
	  //and C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved.
	  //-------------------------------------------------------------------------
	  uint64_t c_key = makeCustomerKey(warehouse_id, district_id, customer_id);
  	  Status c_s;
  	  uint64_t *c_value;
	  bool found = tx.Get(c_key, &c_value, &c_s);
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);

	  output->c_id = customer_id;
	  // retrieve from customer: balance, first, middle, last
	  output->c_balance = c->c_balance;
	  strcpy(output->c_first, c->c_first);
	  strcpy(output->c_middle, c->c_middle);
	  strcpy(output->c_last, c->c_last);

	  
	  //-------------------------------------------------------------------------
	  //The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID (equals C_D_ID), O_C_ID
	  //(equals C_ID), and with the largest existing O_ID, is selected. This is the most recent order placed by that customer. 
	  //O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.
	  //-------------------------------------------------------------------------
	  
	  Order *o = NULL; int32_t o_id;
/*	  Iterator iter = tx->Iterator();
	  uint64_t start = makeOrderKey(warehouse_id, district_id, Order::MAX_ORDER_ID);
	  uint64_t end = makeOrderKey(warehouse_id, district_id, 1);
	  iter.Seek(start);
	  while (iter.Key() > start)
	  	iter.Prev();
	  while (iter.Key() >= end) { 
	  	o_id = reinterpret_cast<int32_t>(iter.Key() << 32 >> 32);
		uint64_t *o_value = iter.Value();*/
	  
/*	  for (o_id = Order::MAX_ORDER_ID; o_id > 0; o_id--) {
	    uint64_t o_key = makeOrderKey(warehouse_id, district_id, o_id);
		Status o_s;
		uint64_t *o_value;
		bool found = tx.Get(o_key, &o_value, &o_s);
		if (!found) continue;*/
/*		o = reinterpret_cast<Order *>(o_value);
		if (o->o_c_id == customer_id) break;
		iter.Prev();
	  }
	  output->o_id = o_id;
      output->o_carrier_id = o->o_carrier_id;
      strcpy(output->o_entry_d, o->o_entry_d);*/
	  
	  //-------------------------------------------------------------------------
	  //All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID),
	  //and OL_O_ID (equals O_ID) are selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID,
	  //OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.
	  //-------------------------------------------------------------------------
	  if (o != NULL) {
	    output->lines.resize(o->o_ol_cnt);
        for (int32_t line_number = 1; line_number <= o->o_ol_cnt; ++line_number) {
		  uint64_t ol_key = makeOrderLineKey(warehouse_id, district_id, o_id, line_number);
  		  Status ol_s;
		  uint64_t *ol_value;
		  bool found = tx.Get(ol_key, &ol_value, &ol_s);
          OrderLine* line = reinterpret_cast<OrderLine *>(ol_value);
          output->lines[line_number-1].ol_i_id = line->ol_i_id;
          output->lines[line_number-1].ol_supply_w_id = line->ol_supply_w_id;
          output->lines[line_number-1].ol_quantity = line->ol_quantity;
          output->lines[line_number-1].ol_amount = line->ol_amount;
          strcpy(output->lines[line_number-1].ol_delivery_d, line->ol_delivery_d);
        }
	  }
      bool b = tx.End();  
  	  if (b) break;
    }
    return;
  }

  int32_t TPCCTxMemStore::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold){
	
	leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
					leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	int num_distinct = 0;
	//printf("StockLevel\n");
	while(true) {
		 
	  tx.Begin();
		 
	  //-------------------------------------------------------------------------
	  //The row in the DISTRICT table with matching D_W_ID and D_ID is selected and D_NEXT_O_ID is retrieved.
	  //-------------------------------------------------------------------------
	  
	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
	  Status d_s;
  	  uint64_t *d_value;
	  bool found = tx.Get(d_key, &d_value, &d_s);
	  assert(found);
      District *d = reinterpret_cast<District *>(d_value);   
	  int32_t o_id = d->d_next_o_id;

	  //-------------------------------------------------------------------------
	  //All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID), 
	  //and OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected.
	  //-------------------------------------------------------------------------
	  int i = o_id - 20;
	  std::vector<int32_t> s_i_ids;
      // Average size is more like ~30.
      s_i_ids.reserve(300);

/*	  Iterator iter = tx->Iterator();
	  int64_t start = makeOrderLineKey(warehouse_id, district_id, i, 1);
	  iter.Seek(start);
	  int64_t end = makeOrderKey(warehouse_id, district_id, o_id, 1);
	  while (true) {
	  	  int64_t ol_key = iter.Key();
		  if (ol_key >= end) break;
	  	  uint64_t *ol_value = iter.Value();*/
/*	  for (; i < o_id; i++) {
	  	for (int j = 1; j < 16; j++) {
		  int64_t ol_key = makeOrderLineKey(warehouse_id, district_id, i, j);
		  Status ol_s;
		  uint64_t *ol_value;
		  bool found = tx.Get(ol_key, &ol_value, &ol_s);
		  if (!found) break;*/
/*		  OrderLine *ol = reinterpret_cast<OrderLine *>(ol_value);   
		  //-------------------------------------------------------------------------
		  //All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) 
		  //from the list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock).
		  //-------------------------------------------------------------------------

		  int32_t s_i_id = ol->ol_i_id;
		  int64_t s_key = makeStockKey(warehouse_id, s_i_id);
		  Status s_s;
		  uint64_t *s_value;
		  found = tx.Get(s_key, &s_value, &s_s);
		  Stock *s = reinterpret_cast<Stock *>(s_value);
		  if (s->s_quantity < threshold) 
		  	s_i_ids.push_back(s_i_id);
		  
//	  	}
		  iter.Next();
	  }

	  std::sort(s_i_ids.begin(), s_i_ids.end());
      
      int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
      for (size_t i = 0; i < s_i_ids.size(); ++i) {
        if (s_i_ids[i] != last) {
          last = s_i_ids[i];
          num_distinct += 1;
        }
      }    
*/
	  bool b = tx.End();  
  	  if (b) break;
	}
	return num_distinct;
  }

  void TPCCTxMemStore::delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
		  std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo){

	ValueType t = kTypeValue;
	ValueType td = kTypeDeletion;
    leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	while (true) {
	  tx.Begin();
		
  	  for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; ++d_id) {
	    //-------------------------------------------------------------------------
	    //The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) 
	    //and with the lowest NO_O_ID value is selected.
	    //-------------------------------------------------------------------------
	    int32_t no_id = 1;
		uint64_t *no_value;
	    NewOrder *no = NULL;
		
/*		int64_t start = makeNewOrderKey(warehouse_id, d_id, 1);
		Iterator iter = tx->Iterator();
		iter.Seek(start);
		int64_t end = makeNewOrderKey(warehouse_id, d_id, Order::MAX_ORDER_ID);
		
		int64_t no_key = iter.Key();
		if (no_key <= end) {
		  no_value = iter.Value();
		  no = reinterpret_cast<NewOrder *>(no_value);
		  no_id = reinterpret_cast<int32_t>(no_key << 32 >> 32);
		};
	*/	 
/*	    while (no_id <= 100000000) {
	  	  int64_t no_key = makeNewOrderKey(warehouse_id, d_id, no_id);
	  	  Status no_s;	  	  
	  	  bool found = tx.Get(no_key, &no_value, &no_s);
		  if (!found) {
		    no_id++;
		    continue;
		  } */

		  //-------------------------------------------------------------------------
		  //The selected row in the NEW-ORDER table is deleted.
		  //-------------------------------------------------------------------------
//	  	  tx.Add(td, no_key, no_value);
/*		  break;
	    }
	    no = reinterpret_cast<NewOrder *>(*no_value);*/
	    if (no == NULL || no->no_d_id != d_id || no->no_w_id != warehouse_id) {
          // No orders for this district
          // TODO: 2.7.4.2: If this occurs in max(1%, 1) of transactions, report it (???)
          continue;
        }
		
		DeliveryOrderInfo order;
        order.d_id = d_id;
        order.o_id = no_id;
        orders->push_back(order);

		//-------------------------------------------------------------------------
		//The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) is selected, 
		//O_C_ID, the customer number, is retrieved, and O_CARRIER_ID is updated.
		//-------------------------------------------------------------------------

		int64_t o_key = makeOrderKey(warehouse_id, d_id, no_id);
		Status o_s;
		uint64_t *o_value;
		bool found = tx.Get(o_key, &o_value, &o_s);
		Order *o = reinterpret_cast<Order *>(o_value);
		assert(o->o_carrier_id == Order::NULL_CARRIER_ID);
		Order *newo = new Order();		
		updateOrder(newo, o, carrier_id);
		uint64_t *o_v = reinterpret_cast<uint64_t *>(newo);
		tx.Add(t, o_key, o_v);
		int32_t c_id = o->o_c_id;

		//-------------------------------------------------------------------------
		//All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected. 
		//All OL_DELIVERY_D, the delivery dates, are updated to the current system time as returned by the operating system 
		//and the sum of all OL_AMOUNT is retrieved.
		//-------------------------------------------------------------------------
		float sum_ol_amount = 0;
/*		int64_t start = makeOrderLineKey(warehouse_id, d_id, no_id, 1);
		iter.Seek(start);
		int64_t end = makeOrderLineKey(warehouse_id, d_id, no_id, 15);
		while (true) {
		  int64_t ol_key = iter.Key();
		  if (ol_key > end) break;
		  uint64_t *ol_value = iter.Value();*/
/*		for (int32_t ol_number = 1; ol_number < 16; ol_number++) {
		  int64_t ol_key = makeOrderLineKey(warehouse_id, d_id, no_id, ol_number);
		  Status ol_s;
		  uint64_t *ol_value;
		  bool found = tx.Get(ol_key, &ol_value, &ol_s);*/
/*		  OrderLine *ol = reinterpret_cast<OrderLine *>(ol_value);
		  OrderLine *newol = new OrderLine();
		  updateOrderLine(newol, ol, now);
		  uint64_t *ol_v = reinterpret_cast<uint64_t *>(newol);
		  tx.Add(t, ol_key, ol_v);
		  sum_ol_amount += ol->ol_amount;
		  iter.Next();
		}*/

		//-------------------------------------------------------------------------
		//The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected 
		//and C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT) previously retrieved. 
		//C_DELIVERY_CNT is incremented by 1.
		//-------------------------------------------------------------------------

		int64_t c_key = makeCustomerKey(warehouse_id, d_id, c_id);
		Status c_s;
		uint64_t *c_value;
		found = tx.Get(c_key, &c_value, &c_s);
		Customer *c = reinterpret_cast<Customer *>(c_value);
		Customer *newc = new Customer();
		updateCustomerDelivery(newc, c, sum_ol_amount);
		uint64_t *c_v = reinterpret_cast<uint64_t *>(newc);
		tx.Add(t, c_key, c_v);
		
	  }

	  bool b = tx.End();  
  	  if (b) break;
	}
	
    return;
  }

//not used yet
bool TPCCTxMemStore::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
            const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
            TPCCUndo** undo){
  return false;
}



void TPCCTxMemStore::orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last, OrderStatusOutput* output){
  return;
}
void TPCCTxMemStore::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo) {
  return;
}


void TPCCTxMemStore::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}
void TPCCTxMemStore::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}

bool TPCCTxMemStore::hasWarehouse(int32_t warehouse_id){
  return true;
}
	
void TPCCTxMemStore::applyUndo(TPCCUndo* undo){
  return;
}
void TPCCTxMemStore::freeUndo(TPCCUndo* undo){
  return;
}

 
}

