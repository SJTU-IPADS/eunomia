// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "tpcc/tpccskiplist.h"
#include "memstore/memstore_skiplist.h"

#include <algorithm>
#include <vector>
#define SEPERATE 0

#define PROFILE 0
#define ABORTPRO 0
#define SLDBTX	0
#define CHECKTPCC 0


#define WARE 0
#define DIST 1
#define CUST 2
#define HIST 3
#define NEWO 4
#define ORDE 5
#define ORLI 6
#define ITEM 7
#define STOC 8
#if USESECONDINDEX
#define CUST_INDEX 0
#define ORDER_INDEX 1
#else 
#define CUST_INDEX 9
#define ORDER_INDEX 10
#endif

namespace leveldb {

  __thread Warehouse* TPCCSkiplist::warehouse_dummy = NULL;
  __thread District* TPCCSkiplist::district_dummy = NULL;
  __thread Customer* TPCCSkiplist::customer_dummy = NULL;
  __thread Order* TPCCSkiplist::order_dummy = NULL;
  __thread OrderLine* TPCCSkiplist::orderline_dummy = NULL;
  __thread Stock* TPCCSkiplist::stock_dummy = NULL;
  __thread History* TPCCSkiplist::history_dummy = NULL;
  __thread NewOrder* TPCCSkiplist::neworder_dummy = NULL;
  __thread std::vector<uint64_t>* TPCCSkiplist::vector_dummy = NULL;
  __thread uint64_t* TPCCSkiplist::array_dummy = NULL; 
  __thread uint64_t TPCCSkiplist::secs = 0;
  static int64_t makeWarehouseKey(int32_t w_id) {
  	int64_t id = static_cast<int64_t>(w_id);
	return id;
  }
  
  static int64_t makeDistrictKey(int32_t w_id, int32_t d_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    int32_t did = d_id + (w_id * District::NUM_PER_WAREHOUSE);
    assert(did >= 0);
	int64_t id = static_cast<int64_t>(did);
    return id;
  }

  static int64_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    int32_t cid = (w_id * District::NUM_PER_WAREHOUSE + d_id)
            * Customer::NUM_PER_DISTRICT + c_id;
    assert(cid >= 0);
	int64_t id = static_cast<int64_t>(cid);
    return id;
  }

  static void convertString(char *newstring, char *oldstring) {
  	for (int i=0; i<8; i++)
		newstring[7 -i] = oldstring[i];
	
  	for (int i=8; i<16; i++)
		newstring[23 -i] = oldstring[i];
#if 0	
	for (int i=0; i<16; i++)
		printf("%lx ", oldstring[i]);
	printf("\n");
	for (int i=0; i<16; i++)
		printf("%lx ", newstring[i]);
	printf("\n");		
#endif
  }

  static uint64_t makeCustomerIndex(int32_t w_id, int32_t d_id, char* c_last, char* c_first) {
	int ptr_end = strcspn(c_last, "\0");
	memset(c_last+ptr_end, 0, 17 - ptr_end);
	ptr_end = strcspn(c_first, "\0");
	memset(c_first+ptr_end, 0, 17 - ptr_end);
	uint64_t *seckey = new uint64_t[5];
  	int32_t did = d_id + (w_id * District::NUM_PER_WAREHOUSE);
	seckey[0] = did;
	convertString((char *)(&seckey[1]), c_last);
	convertString((char *)(&seckey[3]), c_first);
#if 0	
	printf("%d %d %s %s \n", w_id, d_id, c_last, c_first);
	for (int i= 0;i<5; i++)
		printf("%lx ",seckey[i]);
	printf("\n");
#endif	
	return (uint64_t)seckey;
  }

  static bool compareCustomerIndex(uint64_t key, uint64_t bound){
	uint64_t *k = (uint64_t *)key;
	uint64_t *b = (uint64_t *)bound;
	for (int i=0; i<5; i++) {
		if (k[i] > b[i]) return false;
		if (k[i] < b[i]) return true;
	}
	return true;
	
  }
  
  static int64_t makeHistoryKey(int32_t h_c_id, int32_t h_c_d_id, int32_t h_c_w_id, int32_t h_d_id, int32_t h_w_id) {
  	int32_t cid = (h_c_w_id * District::NUM_PER_WAREHOUSE + h_c_d_id)
            * Customer::NUM_PER_DISTRICT + h_c_id;
	int32_t did = h_d_id + (h_w_id * District::NUM_PER_WAREHOUSE);
	int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
  	return id;
  }

  static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID + 1);
    int32_t upper_id = w_id * District::NUM_PER_WAREHOUSE + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	assert(id > 0);
	
    
    return id;
  }

  static int64_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID + 1);
    int32_t upper_id = w_id * District::NUM_PER_WAREHOUSE + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	assert(id > 0);
	
    
    return id;
  }

  static int64_t makeOrderIndex(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
  	int32_t upper_id = (w_id * District::NUM_PER_WAREHOUSE + d_id)
            * Customer::NUM_PER_DISTRICT + c_id;
	int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
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
	int64_t id = static_cast<int64_t>(olid);
    return id;
  }

  static int64_t makeItemKey(int32_t i_id) {
  	int64_t id = static_cast<int64_t>(i_id);
	return id;
  }

  static int64_t makeStockKey(int32_t w_id, int32_t s_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= s_id && s_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
    int32_t sid = s_id + (w_id * Stock::NUM_STOCK_PER_WAREHOUSE);
    assert(sid >= 0);
	int64_t id = static_cast<int64_t>(sid);
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
	assert (oldol->ol_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
  }
  
  TPCCSkiplist::TPCCSkiplist() {
  	secs = 0;
#if USESECONDINDEX
	store = new DBTables(9);
#else
//	store = new DBTables(11);
	store = new DBTables(11);
#endif
	//insert an end value
#if SEPERATE
	store->AddTable(WARE, HASH, NONE);
	store->AddTable(DIST, HASH, NONE);
	store->AddTable(CUST, HASH, SBTREE);
	store->AddTable(HIST, HASH, NONE);
	store->AddTable(NEWO, BTREE, NONE);
	store->AddTable(ORDE, HASH, IBTREE);
	store->AddTable(ORLI, BTREE, NONE);
	store->AddTable(ITEM, HASH, NONE);
	store->AddTable(STOC, HASH, NONE);
#else
#if 1
	for (int i=0; i<9; i++)
		if (i == CUST) store->AddTable(i, BTREE, SBTREE);
		else if (i == ORDE) store->AddTable(i, BTREE, IBTREE);
		else store->AddTable(i, BTREE, NONE);

#else
	for (int i=0; i<9; i++)
		if (i == CUST) {
			int a = store->AddTable(i, CUCKOO, SBTREE);
			if (a != CUST_INDEX) printf("Customer index Wrong!\n");
		}
		else if (i == ORDE) store->AddTable(i, CUCKOO, IBTREE);
		else store->AddTable(i, CUCKOO, NONE);
#endif
#endif

#if !USESECONDINDEX
	store->AddTable(CUST_INDEX, SBTREE, NONE);
	store->AddTable(ORDER_INDEX, BTREE, NONE);
#endif


	//Add the schema 
	store->AddSchema(WARE, sizeof(uint64_t), sizeof(Warehouse));
	store->AddSchema(DIST, sizeof(uint64_t), sizeof(District));
	store->AddSchema(CUST, sizeof(uint64_t), sizeof(Customer));
	store->AddSchema(HIST, sizeof(uint64_t), sizeof(History));
	store->AddSchema(NEWO, sizeof(uint64_t), sizeof(NewOrder));
	store->AddSchema(ORDE, sizeof(uint64_t), sizeof(Order));
	store->AddSchema(ORLI, sizeof(uint64_t), sizeof(OrderLine));
	store->AddSchema(ITEM, sizeof(uint64_t), sizeof(Item));
	store->AddSchema(STOC, sizeof(uint64_t), sizeof(Stock));

	//XXX FIXME: won't serialize sec index currently
	store->AddSchema(CUST_INDEX, sizeof(uint64_t), 0);
	store->AddSchema(CUST_INDEX, sizeof(uint64_t), 0);

	Memstore::MemNode *mn;
	for (int i=0; i<9; i++) {
		//Fixme: invalid value pointer
		Memstore::MemNode *node = store->tables[i]->Put((uint64_t)1<<60, (uint64_t *)new Memstore::MemNode());		
		if (i == ORDE) mn = node;
	}
#if USESECONDINDEX
	store->secondIndexes[ORDER_INDEX]->Put((uint64_t)1<<60, (uint64_t)1<<60, mn);
#else

	uint64_t *temp = new uint64_t[2];
	temp[0] = 1; temp[1] = 0xFFFF;
	store->tables[ORDER_INDEX]->Put((uint64_t)1<<60, temp);
#endif
	fstart = new char[17];
	memset(fstart, 0, 17);
	fend = new char[17];
	fend[0] = 255;
	
	abort = 0;
    conflict = 0;
    capacity = 0;

#if PROFILE || ABORTPRO
	newordernum = 0;
	paymentnum = 0;
	stocklevelnum = 0;
	orderstatusnum = 0;
	delivernum = 0;
#endif

#if PROFILEBUFFERNODE                                                                                                                                         
  bufferMiss = 0;
  bufferHit = 0;
  bufferGet = 0;
#endif

#if PROFILEDELIVERY
  dstep1 = 0;                                                                                                                                            
  dstep2 = 0;
  dstep3 = 0;                                                                                                                                            
  dstep4 = 0;
  search = 0;
  traverse = 0;
  traverseCount = 0;
  seekCount = 0;
#endif

#if ABORTPRO
	neworderabort = 0;
	paymentabort = 0;
	stocklevelabort = 0;
	orderstatusabort = 0;
	deliverabort = 0;
#endif

#if PROFILE
    neworderreadcount = 0;
    neworderwritecount = 0;
    paymentreadcount = 0;
    paymentwritecount = 0;
    stocklevelitercount = 0;
    stocklevelreadcount = 0;
    orderstatusitercount = 0;
    orderstatusreadcount = 0;
    deliverreadcount = 0;
    deliverwritecount = 0;
    deliveritercount = 0;
    
    neworderreadmax = 0;
    neworderwritemax = 0;
    paymentreadmax = 0;
    paymentwritemax = 0;
    stocklevelitermax = 0;
    stocklevelreadmax = 0;
    orderstatusitermax = 0;
    orderstatusreadmax = 0;
    deliverreadmax = 0;
    deliverwritemax = 0;
    deliveritermax = 0;
    
    neworderreadmin = -1;
    neworderwritemin = -1;
    paymentreadmin = -1;
    paymentwritemin = -1;
    stocklevelitermin = -1;
    stocklevelreadmin = -1;
    orderstatusitermin = -1;
    orderstatusreadmin = -1;
    deliverreadmin = -1;
    deliverwritemin = -1;
    deliveritermin = -1;
#endif
  }

  TPCCSkiplist::~TPCCSkiplist() {
  	//printf("rdtsc %ld\n",secs);
  	delete store;
	
	printf("#Abort : %lu\n", abort);
	printf("#Conflict : %lu\n", conflict);
	printf("#Capacity: %lu\n", capacity);

#if ABORTPRO
	printf("Neworder Run: %ld   Abort: %ld\n", newordernum, neworderabort);
	printf("Payment Run: %ld	Abort: %ld\n", paymentnum, paymentabort);	
	printf("Orderstatus Run: %ld   Abort: %ld\n", orderstatusnum, orderstatusabort);
	printf("Stocklevel Run: %ld   Abort: %ld\n", stocklevelnum, stocklevelabort);
	printf("Deliver Run: %ld   Abort: %ld\n", delivernum, deliverabort);
#endif	
#if PROFILEBUFFERNODE
	printf("Miss %d [%lf] Hit %d [%lf] Total %d\n ", bufferMiss, (double)bufferMiss/bufferGet, 
			bufferHit, (double)bufferHit/bufferGet, bufferGet);
#endif

#if PROFILEDELIVERY
  double cpufreq = 3400000; 	
  printf("Delivery TX profile s1 %lf s2 %lf s3 %lf s4 %lf\n", dstep1/cpufreq, dstep2/cpufreq, dstep3/cpufreq, dstep4/cpufreq);
  printf("Seek Profile Search %lf Traverse %lf Avg Count %ld [%ld / %ld]\n", search/cpufreq, traverse/cpufreq, traverseCount / seekCount, traverseCount, seekCount);
#endif



#if PROFILE
	printf("neworderreadcount %f max %ld min %ld\n", (float)neworderreadcount/newordernum, neworderreadmax, neworderreadmin);
	printf("neworderwritecount %f max %ld min %ld\n", (float)neworderwritecount/newordernum, neworderwritemax, neworderwritemin);
	printf("paymentreadcount %f max %ld min %ld\n", (float)paymentreadcount/paymentnum, paymentreadmax, paymentreadmin);
	printf("paymentwritecount %f max %ld min %ld\n", (float)paymentwritecount/paymentnum, paymentwritemax, paymentwritemin);
	printf("stocklevelitercount %f max %ld min %ld\n", (float)stocklevelitercount/stocklevelnum, stocklevelitermax, stocklevelitermin);
	printf("stocklevelreadcount %f max %ld min %ld\n", (float)stocklevelreadcount/stocklevelnum, stocklevelreadmax, stocklevelreadmin);
	printf("orderstatusitercount %f max %ld min %ld\n", (float)orderstatusitercount/orderstatusnum, orderstatusitermax, orderstatusitermin);
	printf("orderstatusreadcount %f max %ld min %ld\n", (float)orderstatusreadcount/orderstatusnum, orderstatusreadmax, orderstatusreadmin);
	printf("deliverreadcount %f max %ld min %ld\n", (float)deliverreadcount/delivernum, deliverreadmax, deliverreadmin);
	printf("deliverwritecount %f max %ld min %ld\n", (float)deliverwritecount/delivernum, deliverwritemax, deliverwritemin);
	printf("deliveritercount %f max %ld min %ld\n", (float)deliveritercount/delivernum, deliveritermax, deliveritermin);
	
#endif
  }

  void TPCCSkiplist::ThreadLocalInit() {
  	warehouse_dummy = new Warehouse();
	district_dummy = new District();
	customer_dummy = new Customer();
	orderline_dummy = new OrderLine();
	order_dummy = new Order();
	neworder_dummy = new NewOrder();
	history_dummy = new History();
	stock_dummy = new Stock();
	vector_dummy = new std::vector<uint64_t>();
	array_dummy = new uint64_t[2];
	secs = 0;
  }

  
  void TPCCSkiplist::insertWarehouse(const Warehouse & warehouse) {
  	int64_t key = makeWarehouseKey(warehouse.w_id);
	//printf("insert w_key %ld\n", key);
	Warehouse *w = const_cast<Warehouse *>(&warehouse);
	uint64_t *value = reinterpret_cast<uint64_t *>(w);
  	
  	store->tables[WARE]->Put(key, value);
  	
  }

  void TPCCSkiplist::insertDistrict(const District & district) {
  	int64_t key = makeDistrictKey(district.d_w_id, district.d_id);
	//printf("insert d_key %ld\n", key);
	District *d = const_cast<District *>(&district);
	uint64_t *value = reinterpret_cast<uint64_t *>(d);
  	
  	store->tables[DIST]->Put(key, value);
  	
  }

  void TPCCSkiplist::insertCustomer(const Customer & customer) {
  	int64_t key = makeCustomerKey(customer.c_w_id, customer.c_d_id, customer.c_id);
	//printf("insert c_key %ld\n", key);
	Customer *c = const_cast<Customer *>(&customer);
	uint64_t *value = reinterpret_cast<uint64_t *>(c);
  	
  	Memstore::MemNode *node = store->tables[CUST]->Put(key, value);
	uint64_t sec = makeCustomerIndex(customer.c_w_id, customer.c_d_id, 
					const_cast<char *>(customer.c_last), const_cast<char *>(customer.c_first));
#if USESECONDINDEX
  	store->secondIndexes[CUST_INDEX]->Put(sec, key, node);
#else
#if 1
	Memstore::MemNode* mn = store->tables[CUST_INDEX]->Get(sec);
	if (mn == NULL) {
//		std::vector<uint64_t> *prikeys = new std::vector<uint64_t>();
//		prikeys->push_back(key);
		uint64_t *prikeys = new uint64_t[2];
		prikeys[0] = 1; prikeys[1] = key;
//printf("key %ld\n",key);
		store->tables[CUST_INDEX]->Put(sec, prikeys);
	}
	else {
		printf("ccccc\n");
		uint64_t *value = mn->value;
		int num = value[0];
		uint64_t *prikeys = new uint64_t[num+2];
		prikeys[0] = num + 1;
		for (int i=1; i<=num; i++) 
			prikeys[i] = value[i];
		prikeys[num+1] = key;
		store->tables[CUST_INDEX]->Put(sec, prikeys);
		//delete[] value;
		//std::vector<uint64_t> *prikeys = (std::vector<uint64_t> *)(mn->value);
		//prikeys->push_back(key);
	}
#endif
#endif
#if 0
	printf("Insert %d %d %s\n",customer.c_w_id, customer.c_d_id, 
					const_cast<char *>(customer.c_last) );
	for (int i=0; i<38; i++)
		printf("%d ",((char *)sec)[i]);
	printf("\n");
#endif
  }

  History* TPCCSkiplist::insertHistory(const History & history) {
	int64_t key = makeHistoryKey(history.h_c_id, history.h_c_d_id, history.h_c_w_id, history.h_d_id, history.h_w_id);
	//printf("insert h_key %ld\n", key);
	History *h = const_cast<History *>(&history);
	uint64_t *value = reinterpret_cast<uint64_t *>(h);
  	
  	store->tables[HIST]->Put(key, value);
  	
	return NULL;
  }

  void TPCCSkiplist::insertItem(const Item & item) {
  	int64_t key = makeItemKey(item.i_id);
	//printf("insert i_key %ld\n", key);
	Item *i =  const_cast<Item *>(&item);
	uint64_t *value = reinterpret_cast<uint64_t *>(i);
  	
  	store->tables[ITEM]->Put(key, value);
  	
  }

  void TPCCSkiplist::insertStock(const Stock & stock) {
  	int64_t key = makeStockKey(stock.s_w_id, stock.s_i_id);
	//printf("insert s_key %ld\n", key);
	Stock *st = const_cast<Stock *>(&stock);
	uint64_t *value = reinterpret_cast<uint64_t *>(st);
  	
  	store->tables[STOC]->Put(key, value);
  	
  }

  Order* TPCCSkiplist::insertOrder(const Order & order) {
  	int64_t key = makeOrderKey(order.o_w_id, order.o_d_id, order.o_id);
	//printf("insert o_key %ld\n", key);
	Order *o = const_cast<Order *>(&order);
	uint64_t *value = reinterpret_cast<uint64_t *>(o);
  	
  	Memstore::MemNode *node = store->tables[ORDE]->Put(key, value);
	uint64_t sec = makeOrderIndex(order.o_w_id, order.o_d_id, order.o_c_id, order.o_id);
#if USESECONDINDEX
	store->secondIndexes[ORDER_INDEX]->Put(sec, key, node);
#else 
	Memstore::MemNode* mn = store->tables[ORDER_INDEX]->Get(sec);
	if (mn == NULL) {
//		std::vector<uint64_t> *prikeys = new std::vector<uint64_t>();
//		prikeys->push_back(key);
		uint64_t *prikeys = new uint64_t[2];
		prikeys[0] = 1; prikeys[1] = key;
		store->tables[ORDER_INDEX]->Put(sec, prikeys);
	}
	else {
		printf("oooo\n");
		uint64_t *value = mn->value;
		int num = value[0];
		uint64_t *prikeys = new uint64_t[num+2];
		prikeys[0] = num + 1;
		for (int i=1; i<=num; i++) 
			prikeys[i] = value[i];
		prikeys[num+1] = key;
		store->tables[ORDER_INDEX]->Put(sec, prikeys);
		delete[] value;
	}
#endif
	return o;
  }

  OrderLine* TPCCSkiplist::insertOrderLine(const OrderLine & orderline) {
  	int64_t key = makeOrderLineKey(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number);
	//printf("insert ol_key %ld\n", key);
	OrderLine *ol = const_cast<OrderLine *>(&orderline);
	if (ol->ol_i_id > Stock::NUM_STOCK_PER_WAREHOUSE) printf("IIIIIIIIIII\n");
	uint64_t *value = reinterpret_cast<uint64_t *>(ol);
  	
  	store->tables[ORLI]->Put(key, value);
  	
	return ol;
  }

  NewOrder* TPCCSkiplist::insertNewOrder(int32_t w_id,int32_t d_id,int32_t o_id) {
  	int64_t key = makeNewOrderKey(w_id, d_id, o_id);
	//printf("insert no_key %ld\n", key);
	NewOrder *neworder = new NewOrder();
	neworder->no_w_id = w_id;
	neworder->no_d_id = d_id;
	neworder->no_o_id = o_id;
	uint64_t *value = reinterpret_cast<uint64_t *>(neworder);
  	
  	store->tables[NEWO]->Put(key, value);
  	
	return neworder;
  }


  bool TPCCSkiplist::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now, NewOrderOutput* output,
        TPCCUndo** undo) {
    // perform the home part
    bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }
	
#if CHECKTPCC

	leveldb::DBTX tx(store);
	//printf("Check\n");
	while(true) {
	  
	  tx.Begin();

	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	  
  	  uint64_t *d_value;
  	  bool found = tx.Get(DIST, d_key, &d_value);
	  assert(found);
	  District *d = reinterpret_cast<District *>(d_value);
	  assert(output->o_id == d->d_next_o_id - 1);
	  
	  int64_t o_key = makeOrderKey(warehouse_id, district_id, output->o_id); 
  	  uint64_t *o_value;
  	  found = tx.Get(ORDE, o_key, &o_value);
	  assert(found);
	  Order *o = reinterpret_cast<Order *>(o_value);
	  assert(o->o_c_id == customer_id);
	  
	  
//	  printf("oid%d \n", output->o_id);
	  
	  uint64_t l_key = makeOrderLineKey(warehouse_id, district_id, output->o_id, 1);
	  uint64_t *l_value;
	  found = tx.Get(ORLI, l_key, &l_value);
	  assert(found);
	  OrderLine *l = reinterpret_cast<OrderLine *>(l_value);
	  if (l->ol_quantity != items[0].ol_quantity) 
	  	printf("ol_quantity %d %d\n",l->ol_quantity, items[0].ol_quantity);
	  assert(l->ol_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
	  //assert(l->ol_quantity == items[1].ol_quantity);


	  uint64_t s_key = makeStockKey(items[3].ol_supply_w_id, items[3].i_id);
      uint64_t *s_value;
	  found = tx.Get(STOC, s_key, &s_value);
	  assert(found);
	  Stock *s = reinterpret_cast<Stock *>(s_value);
	  assert(s->s_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
	  
	  bool b = tx.End();  
  	  if (b) break;
  	}

	
	int32_t o_id_first = 0;
	int32_t o_id_second = 0;
	int32_t dnext = 0;
	int32_t num = 0;
	int32_t o_id_min = 0;
	int32_t c = 0;
	int32_t c1 = 0;
	DBTX rotx(store);
	bool f = false;
	while (!f) {
	rotx.Begin();

	//Consistency 2	
	
	uint64_t *d_value;
	int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	bool found = rotx.Get(DIST, d_key, &d_value);
	assert(found);
	District *d = reinterpret_cast<District *>(d_value);

	int32_t o_id;
	DBTX::Iterator iter(&rotx, ORDE);
	uint64_t start = makeOrderKey(warehouse_id, district_id, Order::MAX_ORDER_ID + 1);
	uint64_t end = makeOrderKey(warehouse_id, district_id, 1);
	iter.Seek(start);

    iter.Prev();
	if (iter.Valid() && iter.Key() >= end) {
		o_id = static_cast<int32_t>(iter.Key() << 32 >> 32);
		//assert(o_id == d->d_next_o_id - 1);		
		o_id_first = o_id;
		dnext = d->d_next_o_id - 1;
	}  	

	start = makeNewOrderKey(warehouse_id, district_id, Order::MAX_ORDER_ID + 1);
	end = makeNewOrderKey(warehouse_id, district_id, 1);
	DBTX::Iterator iter1(&rotx, NEWO);
	iter1.Seek(start);
	assert(iter1.Valid());
    iter1.Prev();
	
	if (iter1.Valid() && iter1.Key() >= end) {
		o_id = static_cast<int32_t>(iter1.Key() << 32 >> 32);
		//assert(o_id == d->d_next_o_id - 1);		
		o_id_second = o_id;
	}  	
	
	//Consistency 3
	
	iter1.Seek(end);
	int32_t min = static_cast<int32_t>(iter1.Key() << 32 >> 32);
	num = 0;
	while (iter1.Valid() && iter1.Key() < start) {
		num++;
		iter1.Next();
	}
	//if (o_id - min + 1 != num) printf("o_id %d %d %d",o_id, min, num);
	//assert(o_id - min + 1 == num);
	o_id_min = o_id - min +1;

	//Consistency 4

	end = makeOrderKey(warehouse_id, district_id, Order::MAX_ORDER_ID);
	start = makeOrderKey(warehouse_id, district_id, 1);
	iter.Seek(start);
	Order *o; 
	c = 0;
	while (iter.Valid() && iter.Key() <= end) {
		uint64_t *o_value = iter.Value();
		o = reinterpret_cast<Order *>(o_value);
		c += o->o_ol_cnt;
		iter.Next();
	}
	start = makeOrderLineKey(warehouse_id, district_id, 1, 1);
	end = makeOrderLineKey(warehouse_id, district_id, Order::MAX_ORDER_ID, Order::MAX_OL_CNT);
	c1 = 0;
	DBTX::Iterator iter2(&rotx, ORLI);
	iter2.Seek(start);
	while (iter2.Valid() && iter2.Key() <= end) {
		c1++;
		iter2.Next();
	}
	//assert(c == c1);
	f = rotx.End();
	}

	assert(c == c1);
	assert(dnext == o_id_first);
	assert(dnext == o_id_second);
	assert(o_id_min == num);
#endif
	
    return true;
  }

  bool TPCCSkiplist::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now,
        NewOrderOutput* output, TPCCUndo** undo) {
#if PROFILE
		  uint32_t rcount = 0;
		  uint32_t wcount = 0;
#endif
#if ABORTPRO || PROFILE
		  atomic_add64(&newordernum, 1);
#endif    

	leveldb::DBTX tx(store);
	while(true) {
	  uint64_t slstart = rdtsc();
	  tx.Begin();
	  secs += (rdtsc() - slstart);
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
  	  
	  uint64_t *w_value;  
	  slstart = rdtsc();
 	  bool found = tx.Get(WARE, w_key, &w_value);
	  secs += (rdtsc() - slstart);
#if PROFILE
	  rcount++;
#endif
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
  	  
  	  uint64_t *d_value;
	  slstart = rdtsc();
  	  found = tx.Get(DIST, d_key, &d_value);
	  secs += (rdtsc() - slstart);
#if PROFILE
	  rcount++;
#endif

	  assert(found);
	  //printf("2.1\n");
	  assert(*d_value != 0);
	  District *d = reinterpret_cast<District *>(d_value);
	  //printf("2.2\n");
	  output->d_tax = d->d_tax;
	  
	  output->o_id = d->d_next_o_id;
    //  printf("[%lx] %d %d %d\n",  pthread_self(), warehouse_id, district_id, output->o_id );
  	  //District *newd = new District();
	  updateDistrict(district_dummy, d);
	  //d->d_next_o_id = d->d_next_o_id + 1;
	  uint64_t *d_v = reinterpret_cast<uint64_t *>(district_dummy);
#if COPY_WHEN_ADD
	  slstart = rdtsc();
	  tx.Add(DIST, d_key, d_v, sizeof(District));
	  secs += (rdtsc() - slstart);
#else 
	  tx.Add(DIST, d_key, d_v);
#endif

#if PROFILE
	  wcount++;
#endif
	  //-------------------------------------------------------------------------- 
  	  //The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
	  //and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name, 
  	  //and C_CREDIT, the customer's credit status, are retrieved.
  	  //--------------------------------------------------------------------------
	  //printf("Step 3\n");
	  uint64_t c_key = makeCustomerKey(warehouse_id, district_id, customer_id);
  	  
  	  uint64_t *c_value;
	  slstart = rdtsc();
	  found = tx.Get(CUST, c_key, &c_value);
	  secs += (rdtsc() - slstart);
#if PROFILE
	  rcount++;
#endif

 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
	  //printf("3.1\n");
	  if(c == (Customer *)3)
	  	printf("key %ld\n", c_key);
	  
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
	  //Order *order = new Order();
	  order_dummy->o_w_id = warehouse_id;
	  order_dummy->o_d_id = district_id;
	  order_dummy->o_id = output->o_id;
	  order_dummy->o_c_id = customer_id;
	  //printf("w %d d %d o %d\n",warehouse_id, district_id, order->o_id);
	  //printf("New order %d\n", customer_id);
	  order_dummy->o_carrier_id = Order::NULL_CARRIER_ID;
	  order_dummy->o_ol_cnt = static_cast<int32_t>(items.size());
	  order_dummy->o_all_local = all_local ? 1 : 0;
	  strcpy(order_dummy->o_entry_d, now);
  	  assert(strlen(order_dummy->o_entry_d) == DATETIME_SIZE);
	  int64_t o_key = makeOrderKey(warehouse_id, district_id, order_dummy->o_id);
	  uint64_t *o_value = reinterpret_cast<uint64_t *>(order_dummy);
	  int64_t o_sec = makeOrderIndex(warehouse_id, district_id, customer_id, order_dummy->o_id);
#if SEC_INDEX
#if USESECONDINDEX
	  tx.Add(ORDE, ORDER_INDEX, o_key, o_sec, o_value, sizeof(Order));
#else 
	  slstart = rdtsc();
	  tx.Add(ORDE, o_key, o_value, sizeof(Order));
	  secs += (rdtsc() - slstart);
	  uint64_t *value;
	  slstart = rdtsc();
	  bool f = tx.Get(ORDER_INDEX, o_sec, &value);
	  secs += (rdtsc() - slstart);
	  if (f) {
	//	printf("[%lx] !!! %lx\n", pthread_self(),o_key );
	//	printf("[%lx] prikey %lx\n", pthread_self(),value[1]);
		//exit(0);
	  	//std::vector<uint64_t> *v = (std::vector<uint64_t> *)value;	
		//memcpy(vector_dummy, v, sizeof(v));
		//vector_dummy->push_back(o_key);
		//tx.Add(ORDER_INDEX, o_sec, (uint64_t *)(vector_dummy), sizeof(vector_dummy));		
		int num = value[0];
		uint64_t *prikeys = new uint64_t[num+2];
		prikeys[0] = num + 1;
		for (int i=1; i<=num; i++) 
			prikeys[i] = value[i];
		prikeys[num+1] = o_key;
		slstart = rdtsc();
		tx.Add(ORDER_INDEX, o_sec, prikeys, (num+2)*8);
		secs += (rdtsc() - slstart);
	  }
	  else {
	//  	printf("[%lx] %lx\n", pthread_self(), o_key );
	  //	vector_dummy->clear();
	//	vector_dummy->push_back(o_key);
	//	uint64_t *array = new uint64_t[2];
		array_dummy[0] = 1;
		array_dummy[1] = o_key; 
	//	tx.Add(ORDER_INDEX, o_sec, (uint64_t *)vector_dummy, sizeof(vector_dummy));
		slstart = rdtsc();
		tx.Add(ORDER_INDEX, o_sec, array_dummy, 16);
		secs += (rdtsc() - slstart);
#if 0
		uint64_t *temp;
		tx.Get(ORDER_INDEX, o_sec, &temp);
		printf("%lx %lx\n",temp[0], temp[1]);
#endif
	  }
#endif
#else
	  tx.Add(ORDE, o_key, o_value, sizeof(Order));
#endif
#if PROFILE
	  wcount++;
#endif
	  //printf("Step 6\n");
	  //NewOrder *no = new NewOrder();
  	  neworder_dummy->no_w_id = warehouse_id;
	  neworder_dummy->no_d_id = district_id;
	  neworder_dummy->no_o_id = output->o_id;
	  int64_t no_key = makeNewOrderKey(warehouse_id, district_id, neworder_dummy->no_o_id);
	  uint64_t *no_value = reinterpret_cast<uint64_t *>(neworder_dummy);
#if COPY_WHEN_ADD
	  slstart = rdtsc();
	  tx.Add(NEWO, no_key, no_value, sizeof(NewOrder));
	  secs += (rdtsc() - slstart);
#else
	  tx.Add(NEWO, no_key, no_value);
#endif
#if PROFILE
	  wcount++;
#endif
	  //-------------------------------------------------------------------------
  	  //For each O_OL_CNT item on the order:
  	  //-------------------------------------------------------------------------
	  //printf("Step 7\n");
  	  
	  output->items.resize(items.size());
	  output->total = 0;

	  for (int i = 0; i < items.size(); ++i) {
	  	//OrderLine *line = new OrderLine();
	    orderline_dummy->ol_o_id = output->o_id;
	    orderline_dummy->ol_d_id = district_id;
	    orderline_dummy->ol_w_id = warehouse_id;
	    memset(orderline_dummy->ol_delivery_d, 0, DATETIME_SIZE+1);
  		//-------------------------------------------------------------------------
		//The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected 
		//and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
		//If I_ID has an unused value, a "not-found" condition is signaled, resulting in a rollback of the database transaction.	
		//-------------------------------------------------------------------------
		//printf("Step 8\n");
		uint64_t i_key = makeItemKey(items[i].i_id);
		
		uint64_t *i_value;
		slstart = rdtsc();
		bool found = tx.Get(ITEM, i_key, &i_value);
		secs += (rdtsc() - slstart);
#if PROFILE
		rcount++;
#endif
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
		
    	uint64_t *s_value;
	    //if (items[i].i_id > 100000) printf("Unused key!\n");
	    slstart = rdtsc();
	    found = tx.Get(STOC, s_key, &s_value);
		secs += (rdtsc() - slstart);
#if PROFILE
		rcount++;
#endif
		assert(found);
		Stock *s = reinterpret_cast<Stock *>(s_value);  
		//Stock *news = new Stock();
		updateStock(stock_dummy, s, &items[i], warehouse_id);
/*		if (s->s_quantity > (item->ol_quantity + 10))
	  		s->s_quantity = s->s_quantity - item->ol_quantity;
		else s->s_quantity = s->s_quantity - item->ol_quantity + 91;		
		s->s_ytd = s->s_ytd - item->ol_quantity;
		s->s_order_cnt = s->s_order_cnt + 1;
		if (item->ol_supply_w_id != warehouse_id) 
	  		s->s_remote_cnt = s->s_remote_cnt + 1;*/

		
		output->items[i].s_quantity = s->s_quantity;
		uint64_t *s_v = reinterpret_cast<uint64_t *>(stock_dummy);
		slstart = rdtsc();
		tx.Add(STOC, s_key, s_v, sizeof(Stock));
		secs += (rdtsc() - slstart);
#if PROFILE
		wcount++;
#endif
		//-------------------------------------------------------------------------
		//The amount for the item in the order (OL_AMOUNT) is computed as: OL_QUANTITY * I_PRICE
		//The strings in I_DATA and S_DATA are examined. If they both include the string "ORIGINAL", 
		//the brand-generic field for that item is set to "B", otherwise, the brand-generic field is set to "G".
		//-------------------------------------------------------------------------  
		//printf("Step 10\n");
    	output->items[i].ol_amount = static_cast<float>(items[i].ol_quantity) * item->i_price;
	    orderline_dummy->ol_amount = output->items[i].ol_amount;
        
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
		orderline_dummy->ol_number = i + 1;
	    orderline_dummy->ol_i_id = items[i].i_id;
		assert (orderline_dummy->ol_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
    	orderline_dummy->ol_supply_w_id = items[i].ol_supply_w_id;
	    orderline_dummy->ol_quantity = items[i].ol_quantity;
    	assert(sizeof(orderline_dummy->ol_dist_info) == sizeof(s->s_dist[district_id]));
    	memcpy(orderline_dummy->ol_dist_info, s->s_dist[district_id], sizeof(orderline_dummy->ol_dist_info));
		uint64_t l_key = makeOrderLineKey(orderline_dummy->ol_w_id, orderline_dummy->ol_d_id, orderline_dummy->ol_o_id, orderline_dummy->ol_number);
		uint64_t *l_value = reinterpret_cast<uint64_t *>(orderline_dummy);
	//	printf("%d %d %d %d\n", line->ol_w_id, line->ol_d_id, line->ol_o_id, line->ol_number);
	//	printf("OrderLine %lx\n", l_key);
		slstart = rdtsc();
		tx.Add(ORLI, l_key, l_value, sizeof(OrderLine));
		secs += (rdtsc() - slstart);
#if PROFILE
		wcount++;
#endif

		//-------------------------------------------------------------------------
		//The total-amount for the complete order is computed as: 
		//sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
		//-------------------------------------------------------------------------
		//printf("Step 12\n");
		output->total += orderline_dummy->ol_amount;
	  }
	
	  output->total = output->total * (1 - output->c_discount) * (1 + output->w_tax + output->d_tax);
 	//  printf("Step 13\n");
 	//printf("[%lx] try to commit \n", pthread_self());
 	slstart = rdtsc();
 	  bool b = tx.End();  
	  secs += (rdtsc() - slstart);

  	  if (b) {
	  //	printf("%lx \n", o_key);
	  //	printf("[%lx] commit \n", pthread_self());
	  	break;
  	  }
	//  else printf("[%lx] abort \n", pthread_self());

#if ABORTPRO
	  atomic_add64(&neworderabort, 1);
#endif

  	}

#if PROFILEBUFFERNODE                                                                                                                                         
	  bufferMiss += tx.bufferMiss;
	  bufferHit += tx.bufferHit;
	  bufferGet += tx.bufferGet;
#endif


#if PROFILE
	atomic_add64(&abort, tx.rtmProf.abortCounts);
	atomic_add64(&capacity, tx.rtmProf.capacityCounts);
	atomic_add64(&conflict, tx.rtmProf.conflictCounts);
		

	atomic_add64(&neworderreadcount, rcount);
	atomic_add64(&neworderwritecount, wcount);

	while (rcount > neworderreadmax) neworderreadmax = rcount;
	while (wcount > neworderwritemax) neworderwritemax = wcount;
	while (rcount < neworderreadmin) neworderreadmin = rcount;
	while (wcount < neworderwritemin) neworderwritemin = wcount;
#endif	
    return true;
  }

  #define COPY_ADDRESS(src, dest, prefix) \
	  Address::copy( \
			  dest->prefix ## street_1, dest->prefix ## street_2, dest->prefix ## city, \
			  dest->prefix ## state, dest->prefix ## zip,\
			  src->prefix ## street_1, src->prefix ## street_2, src->prefix ## city, \
			  src->prefix ## state, src->prefix ## zip)

  void TPCCSkiplist::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo) {
#if PROFILE
		  uint32_t rcount = 0;
		  uint32_t wcount = 0;
#endif
#if ABORTPRO || PROFILE
		  atomic_add64(&paymentnum, 1);
#endif    
	  
		  leveldb::DBTX tx(store);
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
			uint64_t *w_value;	
			bool found = tx.Get(WARE, w_key, &w_value);
#if PROFILE
			rcount++;
#endif
			assert(found);
			Warehouse *w = reinterpret_cast<Warehouse *>(w_value);
			//Warehouse *neww = new Warehouse();
			updateWarehouseYtd(warehouse_dummy, w, h_amount);
			uint64_t *w_v = reinterpret_cast<uint64_t *>(warehouse_dummy);
			//w->w_ytd = w->w_ytd + h_amount;

			tx.Add(WARE, w_key, w_v, sizeof(Warehouse));

#if PROFILE
			wcount++;
#endif
			COPY_ADDRESS(w, output, w_);
	  
			//-------------------------------------------------------------------------
			//The row in the DISTRICT table with matching D_W_ID and D_ID is selected. 
			//D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved 
			//and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
			//-------------------------------------------------------------------------
	  
			int64_t d_key = makeDistrictKey(warehouse_id, district_id);
			uint64_t *d_value;
			found = tx.Get(DIST, d_key, &d_value);
#if PROFILE
			rcount++;
#endif
			assert(found);
			//printf("2.1\n");
			assert(*d_value != 0);
			District *d = reinterpret_cast<District *>(d_value);	  
			//District *newd = new District();
			updateDistrictYtd(district_dummy, d, h_amount);
			uint64_t *d_v = reinterpret_cast<uint64_t *>(district_dummy);
			//d->d_ytd = d->d_ytd + h_amount;

			tx.Add(DIST, d_key, d_v, sizeof(District));

#if PROFILE
			wcount++;
#endif
			COPY_ADDRESS(d, output, d_);
	  
			//-------------------------------------------------------------------------
			//the row in the CUSTOMER table with matching C_W_ID, C_D_ID and C_ID is selected. 
			//C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, 
			//C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, and C_BALANCE are retrieved. 
			//C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased by H_AMOUNT. 
			//C_PAYMENT_CNT is incremented by 1.
			//If the value of C_CREDIT is equal to "BC", then C_DATA is also retrieved 
			//and C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are inserted at the left of the C_DATA field
			//-------------------------------------------------------------------------
			char *clast = const_cast<char *>(c_last);	  		
			uint64_t c_start = makeCustomerIndex(c_warehouse_id, c_district_id, clast, fstart);
			uint64_t c_end = makeCustomerIndex(c_warehouse_id, c_district_id, clast, fend);
#if 0

			printf("start   ");
			for (int i=0; i<17; i++)
				printf("%d ",c_last[i]);
			printf("\n");
			
	//		printf("end %d %d %s\n",c_warehouse_id, c_district_id, clast);
#endif	
#if USESECONDINDEX
			DBTX::SecondaryIndexIterator iter(&tx, CUST_INDEX);
#else 
			DBTX::Iterator iter(&tx, CUST_INDEX);
#endif
			iter.Seek(c_start);
#if USESECONDINDEX
			uint64_t *c_values[100];
#endif
			uint64_t c_keys[100];
			int j = 0;
			while (iter.Valid()) {
				
#if PROFILE		
				rcount++;
#endif			
				if (compareCustomerIndex(iter.Key(), c_end)){
#if 0					
					for (int i=0; i<40; i++)
						printf("%d ",((char *)iter.Key())[i]);
					printf("\n");
#endif
#if USESECONDINDEX
					DBTX::KeyValues *kvs = iter.Value();
					int num = kvs->num;
					for (int i=0; i<num; i++)  {
						c_values[j] = kvs->values[i];
						c_keys[j] = kvs->keys[i];
//						printf("j %d\n",j);
						j++;
					}	
					delete kvs;
#else
				//	std::vector<uint64_t> *prikeys = (std::vector<uint64_t> *)(iter.Value());
				//	int num = prikeys->size();
					uint64_t *prikeys = iter.Value();
					int num = prikeys[0];
//printf("num %d\n", prikeys[0]);
					for (int i=1; i<=num; i++) {
						c_keys[j] = prikeys[i];
//printf("ckey  %ld\n",prikeys[i]);
						j++;
					}
#endif
					if (j >= 100) {
						printf("P Array Full\n");
						exit(0);
					}
				}
				else break;
				iter.Next();
					
			}
			j = (j+1)/2 - 1;
			uint64_t c_key = c_keys[j];
#if USESECONDINDEX
			uint64_t *c_value = c_values[j];
#else
			uint64_t *c_value;
			tx.Get(CUST, c_key, &c_value);
#endif
//			printf("cv %lx\n",c_value);
			
			//if (c_value == NULL) exit(0);
			assert(found);
			Customer *c = reinterpret_cast<Customer *>(c_value);
			// printf("customer %d %lx\n",j,c_value);
			//Customer *newc = new Customer();
			updateCustomer(customer_dummy, c, h_amount, warehouse_id, district_id);
			uint64_t *c_v = reinterpret_cast<uint64_t *>(customer_dummy);
/*
			c->c_balance = c->c_balance - h_amount;
			c->c_ytd_payment = c->c_ytd_payment + h_amount;
			c->c_payment_cnt = c->c_payment_cnt + 1;

			if (strcmp(c->c_credit, Customer::BAD_CREDIT) == 0) {
			  static const int HISTORY_SIZE = Customer::MAX_DATA+1;
		      char history[HISTORY_SIZE];
  			  int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
              	c->c_id, c->c_d_id, c->c_w_id, district_id, warehouse_id, h_amount);
      		  assert(characters < HISTORY_SIZE);

		      // Perform the insert with a move and copy
      		  int current_keep = static_cast<int>(strlen(c->c_data));
      		  if (current_keep + characters > Customer::MAX_DATA) {
         		current_keep = Customer::MAX_DATA - characters;
      		  }
      		  assert(current_keep + characters <= Customer::MAX_DATA);
      		  memcpy(c->c_data+characters, c->c_data, current_keep);
      		  memcpy(c->c_data, history, characters);
      		  c->c_data[characters + current_keep] = '\0';
      		  assert(strlen(c->c_data) == characters + current_keep);
			}*/
#if COPY_WHEN_ADD
			tx.Add(CUST, c_key, c_v, sizeof(Customer));
#else
			tx.Add(CUST, c_key, c_v);
#endif
#if PROFILE
			wcount++;
#endif
			output->c_credit_lim = c->c_credit_lim;
			output->c_discount = c->c_discount;
			output->c_balance = c->c_balance;
    #define COPY_STRING(dest, src, field) memcpy(dest->field, src->field, sizeof(src->field))
			COPY_STRING(output, c, c_first);
			COPY_STRING(output, c, c_middle);
			COPY_STRING(output, c, c_last);
			COPY_ADDRESS(c, output, c_);
			COPY_STRING(output, c, c_phone);
			COPY_STRING(output, c, c_since);
			COPY_STRING(output, c, c_credit);
			COPY_STRING(output, c, c_data);
    #undef COPY_STRING
	  
	  
			//-------------------------------------------------------------------------
			//H_DATA is built by concatenating W_NAME and D_NAME separated by 4 spaces.
			//A new row is inserted into the HISTORY table with H_C_ID = C_ID, H_C_D_ID = C_D_ID, H_C_W_ID =
			//C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
			//-------------------------------------------------------------------------
	  		int64_t customer_id = c_key - (c_warehouse_id * District::NUM_PER_WAREHOUSE + c_district_id)
            						* Customer::NUM_PER_DISTRICT;
			uint64_t h_key = makeHistoryKey(customer_id, c_district_id, c_warehouse_id, district_id, warehouse_id);
			//History *h = new History();
			history_dummy->h_amount = h_amount;
			strcpy(history_dummy->h_date, now);
			strcpy(history_dummy->h_data, w->w_name);
			strcat(history_dummy->h_data, "    ");
			strcat(history_dummy->h_data, d->d_name);
			uint64_t *h_v = reinterpret_cast<uint64_t *>(history_dummy);
#if COPY_WHEN_ADD
			tx.Add(HIST, h_key, h_v, sizeof(History));
#else
			tx.Add(HIST, h_key, h_v);
#endif
#if PROFILE
			wcount++;
#endif
			//printf("3\n");
			bool b = tx.End();	

			if (b) break;
	  
#if ABORTPRO
			atomic_add64(&paymentabort, 1);
#endif
		  }

#if PROFILEBUFFERNODE                                                                                                                                         
  		  bufferMiss += tx.bufferMiss;
		  bufferHit += tx.bufferHit;
		  bufferGet += tx.bufferGet;
#endif

		  
#if PROFILE
		  atomic_add64(&abort, tx.rtmProf.abortCounts);
		  atomic_add64(&capacity, tx.rtmProf.capacityCounts);
		  atomic_add64(&conflict, tx.rtmProf.conflictCounts);
		  
		  atomic_add64(&paymentreadcount, rcount);
		  atomic_add64(&paymentwritecount, wcount);
		  while (rcount > paymentreadmax) paymentreadmax = rcount;
		  while (wcount > paymentwritemax) paymentwritemax = wcount;
		  while (rcount < paymentreadmin) paymentreadmin = rcount;
		  while (wcount < paymentwritemin) paymentwritemin = wcount;
#endif
		  return;
  }

  void TPCCSkiplist::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
			int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
			PaymentOutput* output, TPCCUndo** undo) {  

	paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount, now, output, undo);	
	//check
#if CHECKTPCC
	leveldb::DBTX tx(store);
	//printf("Check\n");
	float sum; float wytd;
	//int32_t sum; int32_t wytd;
	while(true) {
	  
	  tx.Begin();
	  sum = 0;
	  int64_t c_key = makeCustomerKey(c_warehouse_id, c_district_id, customer_id);
  	  
  	  uint64_t *c_value;
	  bool found = tx.Get(CUST, c_key, &c_value);
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
	  //assert(output->c_balance == c->c_balance);

	  //Consistency 1
	  //Change h_amount range to (1000,5000) when doing this test 
	  // TPCCClient.h : 35
	  //Use int when numtx >= 1000000
	  int64_t w_key = makeWarehouseKey(warehouse_id);	  
	  uint64_t *w_value;  
 	  found = tx.Get(WARE, w_key, &w_value);
	  assert(found);
	  Warehouse *w = reinterpret_cast<Warehouse *>(w_value);
	  wytd = w->w_ytd;
	  
 	 //	printf("%f\n",wytd);
	  for (int i = 1; i<=District::NUM_PER_WAREHOUSE; i++) {
		  int64_t d_key = makeDistrictKey(warehouse_id, i);
  		  uint64_t *d_value;
	  	  found = tx.Get(DIST, d_key, &d_value);
		  District *d = reinterpret_cast<District *>(d_value);   
		  sum += d->d_ytd;
		//  printf("%f\n",d->d_ytd);
	  }
	  //printf("%f DIFF\n",wytd-sum);
	  bool b = tx.End();  
	  
  	  if (b) break;
  	}
#if 0	  
	
	if (sum - wytd >= 1000 || wytd - sum >= 1000) 
	if (sum!=wytd)
	  printf("Consistency 1, id %d sum %d  warehouse %d\n", warehouse_id, sum, wytd);	
#endif	  

#endif

  }

  void TPCCSkiplist::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
	int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
	PaymentOutput* output, TPCCUndo** undo){
#if PROFILE
    uint32_t rcount = 0;
	uint32_t wcount = 0;
#endif
#if ABORTPRO || PROFILE
	atomic_add64(&paymentnum, 1);
#endif    

    leveldb::DBTX tx(store);
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
	  uint64_t *w_value;  
 	  bool found = tx.Get(WARE, w_key, &w_value);
#if PROFILE
	  rcount++;
#endif
	  assert(found);
	  Warehouse *w = reinterpret_cast<Warehouse *>(w_value);
	  //Warehouse *neww = new Warehouse();
	  updateWarehouseYtd(warehouse_dummy, w, h_amount);
	  uint64_t *w_v = reinterpret_cast<uint64_t *>(warehouse_dummy);
	  //w->w_ytd = w->w_ytd + h_amount;
#if COPY_WHEN_ADD
	  tx.Add(WARE, w_key, w_v, sizeof(Warehouse));
#else
	  tx.Add(WARE, w_key, w_v);
#endif
#if PROFILE
	  wcount++;
#endif
	  COPY_ADDRESS(w, output, w_);

	  //-------------------------------------------------------------------------
	  //The row in the DISTRICT table with matching D_W_ID and D_ID is selected. 
	  //D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved 
	  //and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
	  //-------------------------------------------------------------------------

	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	  uint64_t *d_value;
  	  found = tx.Get(DIST, d_key, &d_value);
#if PROFILE
	  rcount++;
#endif
	  assert(found);
	  //printf("2.1\n");
	  if(*d_value == 0)
	  	printf("d_value %p\n", d_value);
	  assert(*d_value != 0);
	  District *d = reinterpret_cast<District *>(d_value);		
	  //District *newd = new District();
	  updateDistrictYtd(district_dummy, d, h_amount);
	  uint64_t *d_v = reinterpret_cast<uint64_t *>(district_dummy);
	  //d->d_ytd = d->d_ytd + h_amount;
#if COPY_WHEN_ADD
	  tx.Add(DIST, d_key, d_v, sizeof(District));
#else
	  tx.Add(DIST, d_key, d_v);
#endif
#if PROFILE
	  wcount++;
#endif
	  COPY_ADDRESS(d, output, d_);

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
  	  
  	  uint64_t *c_value;
	  found = tx.Get(CUST, c_key, &c_value);
#if PROFILE
	  rcount++;
#endif
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
	  //Customer *newc = new Customer();
	  updateCustomer(customer_dummy, c, h_amount, warehouse_id, district_id);
	  uint64_t *c_v = reinterpret_cast<uint64_t *>(customer_dummy);
	  /*

	  c->c_balance = c->c_balance - h_amount;
	  c->c_ytd_payment = c->c_ytd_payment + h_amount;
	  c->c_payment_cnt = c->c_payment_cnt + 1;

	  if (strcmp(c->c_credit, Customer::BAD_CREDIT) == 0) {
			  static const int HISTORY_SIZE = Customer::MAX_DATA+1;
		      char history[HISTORY_SIZE];
  			  int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
              	c->c_id, c->c_d_id, c->c_w_id, district_id, warehouse_id, h_amount);
      		  assert(characters < HISTORY_SIZE);

		      // Perform the insert with a move and copy
      		  int current_keep = static_cast<int>(strlen(c->c_data));
      		  if (current_keep + characters > Customer::MAX_DATA) {
         		current_keep = Customer::MAX_DATA - characters;
      		  }
      		  assert(current_keep + characters <= Customer::MAX_DATA);
      		  memcpy(c->c_data+characters, c->c_data, current_keep);
      		  memcpy(c->c_data, history, characters);
      		  c->c_data[characters + current_keep] = '\0';
      		  assert(strlen(c->c_data) == characters + current_keep);
	  }*/
#if COPY_WHEN_ADD
	  tx.Add(CUST, c_key, c_v, sizeof(Customer));
#else
	  tx.Add(CUST, c_key, c_v);
#endif
#if PROFILE
	  wcount++;
#endif
	  output->c_credit_lim = c->c_credit_lim;
      output->c_discount = c->c_discount;
      output->c_balance = c->c_balance;
    #define COPY_STRING(dest, src, field) memcpy(dest->field, src->field, sizeof(src->field))
      COPY_STRING(output, c, c_first);
      COPY_STRING(output, c, c_middle);
      COPY_STRING(output, c, c_last);
      COPY_ADDRESS(c, output, c_);
      COPY_STRING(output, c, c_phone);
      COPY_STRING(output, c, c_since);
      COPY_STRING(output, c, c_credit);
      COPY_STRING(output, c, c_data);
    #undef COPY_STRING


	  //-------------------------------------------------------------------------
	  //H_DATA is built by concatenating W_NAME and D_NAME separated by 4 spaces.
	  //A new row is inserted into the HISTORY table with H_C_ID = C_ID, H_C_D_ID = C_D_ID, H_C_W_ID =
	  //C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.
	  //-------------------------------------------------------------------------

	  uint64_t h_key = makeHistoryKey(customer_id, c_district_id, c_warehouse_id, district_id, warehouse_id);
	  //History *h = new History();
      history_dummy->h_amount = h_amount;
      strcpy(history_dummy->h_date, now);
      strcpy(history_dummy->h_data, w->w_name);
      strcat(history_dummy->h_data, "    ");
      strcat(history_dummy->h_data, d->d_name);
      uint64_t *h_v = reinterpret_cast<uint64_t *>(history_dummy);
#if COPY_WHEN_ADD
	  tx.Add(HIST, h_key, h_v, sizeof(History));
#else
	  tx.Add(HIST, h_key, h_v);
#endif
#if PROFILE
	  wcount++;
#endif
	  //printf("3\n");
	  bool b = tx.End();  

  	  if (b) break;

#if ABORTPRO
	  atomic_add64(&paymentabort, 1);
#endif
    }

#if PROFILEBUFFERNODE                                                                                                                                         
  bufferMiss += tx.bufferMiss;
  bufferHit += tx.bufferHit;
  bufferGet += tx.bufferGet;
#endif

#if PROFILE
	atomic_add64(&abort, tx.rtmProf.abortCounts);
	atomic_add64(&capacity, tx.rtmProf.capacityCounts);
	atomic_add64(&conflict, tx.rtmProf.conflictCounts);

	atomic_add64(&paymentreadcount, rcount);
	atomic_add64(&paymentwritecount, wcount);
	while (rcount > paymentreadmax) paymentreadmax = rcount;
	while (wcount > paymentwritemax) paymentwritemax = wcount;
	while (rcount < paymentreadmin) paymentreadmin = rcount;
	while (wcount < paymentwritemin) paymentwritemin = wcount;
#endif
    return;
  }
  #undef COPY_ADDRESS
  
  void TPCCSkiplist::orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last, OrderStatusOutput* output){
#if PROFILE
			uint32_t icount = 0;
			uint32_t rcount = 0;
#endif
#if PROFILE || ABORTPRO
			atomic_add64(&orderstatusnum, 1);
#endif

#if SLDBTX
			leveldb::DBTX tx(store);
#else
			leveldb::DBROTX tx(store);
#endif

			while(true) {

			tx.Begin();
	  
			//-------------------------------------------------------------------------
			//the row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
			//and C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved.
			//-------------------------------------------------------------------------
			char *clast = const_cast<char *>(c_last);
			uint64_t c_start = makeCustomerIndex(warehouse_id, district_id, clast, fstart);
			uint64_t c_end = makeCustomerIndex(warehouse_id, district_id, clast, fend);
#if 0
			
			printf("start\n");
			for (int i=0; i<38; i++)
				printf("%d ",((char *)c_start)[i]);
			printf("\n");
					
			printf("end %d %d %s\n",warehouse_id, district_id, clast);
#endif
#if USESECONDINDEX
#if SLDBTX

			DBTX::SecondaryIndexIterator citer(&tx, CUST_INDEX);
#else	
			DBROTX::SecondaryIndexIterator citer(&tx, CUST_INDEX);
#endif
#else
#if SLDBTX
			DBTX::Iterator citer(&tx, CUST_INDEX);
#else
			DBROTX::Iterator citer(&tx, CUST_INDEX);
#endif
#endif			
			citer.Seek(c_start);
			uint64_t *c_values[100];
			uint64_t c_keys[100];
			int j = 0;
			while (citer.Valid()) {
					
#if PROFILE		
				rcount++;
#endif			
				if (compareCustomerIndex(citer.Key(), c_end)){
#if 0					
								for (int i=0; i<38; i++)
									printf("%d ",((char *)citer.Key())[i]);
								printf("\n");
#endif

					
#if USESECONDINDEX
#if SLDBTX
										
					DBTX::KeyValues *kvs = citer.Value();
#else
					DBROTX::KeyValues *kvs = citer.Value();
#endif
					int num = kvs->num;
					for (int i=0; i<num; i++)  {
						c_values[j] = kvs->values[i];
						c_keys[j] = kvs->keys[i];
//						printf("j %d\n",j);
						j++;
					}	
					delete kvs;
#else
				//	std::vector<uint64_t> *prikeys = (std::vector<uint64_t> *)(citer.Value());
				//	int num = prikeys->size();
					uint64_t *prikeys = citer.Value();
					int num = prikeys[0];
					for (int i=1; i<=num; i++) {
						c_keys[j] = prikeys[i];
						j++;
					}
#endif
					if (j >= 100) {
						printf("OS Array Full\n");
						exit(0);
					}
				}
				else break;
				citer.Next();
					
			}
			j = (j+1)/2 - 1;
			uint64_t c_key = c_keys[j];
#if USESECONDINDEX
			uint64_t *c_value = c_values[j];
#else
			uint64_t *c_value;
			tx.Get(CUST, c_key, &c_value);
#endif

			//if (c_value == NULL) exit(0);
			Customer *c = reinterpret_cast<Customer *>(c_value);
	  		
	  		int64_t customer_id = c_key - (warehouse_id * District::NUM_PER_WAREHOUSE + district_id)
            						* Customer::NUM_PER_DISTRICT;
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
#if USESECONDINDEX
#if SLDBTX
				  DBTX::SecondaryIndexIterator iter(&tx, ORDER_INDEX);
#else
				  DBROTX::SecondaryIndexIterator iter(&tx, ORDER_INDEX);
#endif
#else
#if SLDBTX
				  DBTX::Iterator iter(&tx, ORDER_INDEX);
#else
				  DBROTX::Iterator iter(&tx, ORDER_INDEX);
#endif

#endif
				  uint64_t start = makeOrderIndex(warehouse_id, district_id, customer_id, Order::MAX_ORDER_ID + 1);
				  uint64_t end = makeOrderIndex(warehouse_id, district_id, customer_id, 1);
				  iter.Seek(start);
				  if(iter.Valid())
    				iter.Prev();
				  if (iter.Valid() && iter.Key() >= end) {
#if USESECONDINDEX
#if SLDBTX
					DBTX::KeyValues *kvs = iter.Value();
#else					
					DBROTX::KeyValues *kvs = iter.Value();
#endif
					o_id = static_cast<int32_t>(kvs->keys[0] << 32 >> 32);
					uint64_t *o_value = kvs->values[0];
#else
			//		std::vector<uint64_t> *prikeys = (std::vector<uint64_t> *)(iter.Value());
					uint64_t *prikeys = iter.Value();
					o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);
					uint64_t *o_value;
					tx.Get(ORDE, prikeys[1], &o_value);
				//	printf("%lx\n", prikeys[0]);
#endif


#if PROFILE
							icount++;
#endif
					o = reinterpret_cast<Order *>(o_value);
				  }
			
			
			//-------------------------------------------------------------------------
			//All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID),
			//and OL_O_ID (equals O_ID) are selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID,
			//OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.
			//-------------------------------------------------------------------------
			if (o != NULL) { 
			  //printf("Catch\n");
			  output->o_id = o_id;
			  output->o_carrier_id = o->o_carrier_id;
			  strcpy(output->o_entry_d, o->o_entry_d);
			  output->lines.resize(o->o_ol_cnt);
			  for (int32_t line_number = 1; line_number <= o->o_ol_cnt; ++line_number) {
				uint64_t ol_key = makeOrderLineKey(warehouse_id, district_id, o_id, line_number);
				
				uint64_t *ol_value;
				bool found = tx.Get(ORLI, ol_key, &ol_value);
#if PROFILE
				rcount++;
#endif
				OrderLine* line = reinterpret_cast<OrderLine *>(ol_value);
				assert (line->ol_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
				output->lines[line_number-1].ol_i_id = line->ol_i_id;
				output->lines[line_number-1].ol_supply_w_id = line->ol_supply_w_id;
				output->lines[line_number-1].ol_quantity = line->ol_quantity;
				output->lines[line_number-1].ol_amount = line->ol_amount;
				strcpy(output->lines[line_number-1].ol_delivery_d, line->ol_delivery_d);
			  }
			}
			bool b = tx.End();
			if(b)
			  break;
			}
#if PROFILE
			atomic_add64(&orderstatusreadcount, rcount);
			atomic_add64(&orderstatusitercount, icount);
			while (rcount > orderstatusreadmax) orderstatusreadmax = rcount;
			while (icount > orderstatusitermax) orderstatusitermax = icount;
			while (rcount < orderstatusreadmin) orderstatusreadmin = rcount;
			while (icount < orderstatusitermin) orderstatusitermin = icount;
#endif	  
			return;
  }

  void TPCCSkiplist::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id, OrderStatusOutput* output){
#if PROFILE
	  uint32_t icount = 0;
	  uint32_t rcount = 0;
#endif
#if PROFILE || ABORTPRO
	  atomic_add64(&orderstatusnum, 1);
#endif

#if SLDBTX

	  leveldb::DBTX tx(store);
#else
	  leveldb::DBROTX tx(store);
#endif

	 while(true) {
	
	  tx.Begin();

	 //-------------------------------------------------------------------------
	  //the row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
	  //and C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved.
	  //-------------------------------------------------------------------------
	  uint64_t c_key = makeCustomerKey(warehouse_id, district_id, customer_id);
  	  
  	  uint64_t *c_value;
	  bool found = tx.Get(CUST, c_key, &c_value);
#if PROFILE
	  rcount++;
#endif
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
#if USESECONDINDEX
#if SLDBTX
		DBTX::SecondaryIndexIterator iter(&tx, ORDER_INDEX);
#else
		DBROTX::SecondaryIndexIterator iter(&tx, ORDER_INDEX);
#endif
#else
//printf("here!!!!!!!!!!!!!!!\n");
#if SLDBTX
		DBTX::Iterator iter(&tx, ORDER_INDEX);
#else
		DBROTX::Iterator iter(&tx, ORDER_INDEX);
#endif
	  
#endif
		uint64_t start = makeOrderIndex(warehouse_id, district_id, customer_id, Order::MAX_ORDER_ID + 1);
		uint64_t end = makeOrderIndex(warehouse_id, district_id, customer_id, 1);
		iter.Seek(start);
		if(iter.Valid())
			  iter.Prev();
		else printf("!!SeekOut\n");
		if (iter.Valid() && iter.Key() >= end) {
#if USESECONDINDEX
#if SLDBTX
			  DBTX::KeyValues *kvs = iter.Value();
#else					
			  DBROTX::KeyValues *kvs = iter.Value();
#endif
			  o_id = static_cast<int32_t>(kvs->keys[0] << 32 >> 32);
			  uint64_t *o_value = kvs->values[0];
#else
		//		std::vector<uint64_t> *prikeys = (std::vector<uint64_t> *)(iter.Value());
				uint64_t *prikeys = iter.Value();
//printf("OIDOIDOID      %d    %lx\n", prikeys[0],prikeys[1]);
				o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);
			  uint64_t *o_value;
			  tx.Get(ORDE, prikeys[1], &o_value);
//			 printf("%lx size %d\n", (*prikeys)[0], sizeof(*prikeys)); 
#endif
	

#if PROFILE
			icount++;
#endif
			o = reinterpret_cast<Order *>(o_value);
	  }


	  
	  //-------------------------------------------------------------------------
	  //All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID),
	  //and OL_O_ID (equals O_ID) are selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID,
	  //OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.
	  //-------------------------------------------------------------------------
	  if (o != NULL) { 
	  	//printf("Catch\n");
	  	output->o_id = o_id;
        output->o_carrier_id = o->o_carrier_id;
        strcpy(output->o_entry_d, o->o_entry_d);
	    output->lines.resize(o->o_ol_cnt);
        for (int32_t line_number = 1; line_number <= o->o_ol_cnt; ++line_number) {
		  uint64_t ol_key = makeOrderLineKey(warehouse_id, district_id, o_id, line_number);
  		  
		  uint64_t *ol_value;
		  bool found = tx.Get(ORLI, ol_key, &ol_value);
#if PROFILE
		  rcount++;
#endif
          OrderLine* line = reinterpret_cast<OrderLine *>(ol_value);
		  
		  assert (line->ol_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
          output->lines[line_number-1].ol_i_id = line->ol_i_id;
          output->lines[line_number-1].ol_supply_w_id = line->ol_supply_w_id;
          output->lines[line_number-1].ol_quantity = line->ol_quantity;
          output->lines[line_number-1].ol_amount = line->ol_amount;
          strcpy(output->lines[line_number-1].ol_delivery_d, line->ol_delivery_d);
        }
	  }
	  else printf("No order\n");
      bool b = tx.End();
      if(b)
	break;
     }
#if PROFILE
	  atomic_add64(&orderstatusreadcount, rcount);
	  atomic_add64(&orderstatusitercount, icount);
	  while (rcount > orderstatusreadmax) orderstatusreadmax = rcount;
	  while (icount > orderstatusitermax) orderstatusitermax = icount;
	  while (rcount < orderstatusreadmin) orderstatusreadmin = rcount;
	  while (icount < orderstatusitermin) orderstatusitermin = icount;
#endif	  
      return;
  }

  int32_t TPCCSkiplist::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold){
#if PROFILE
   	  uint32_t icount = 0;
	  uint32_t rcount = 0;
#endif
#if PROFILE || ABORTPRO
	  atomic_add64(&stocklevelnum, 1);
#endif
#if SLDBTX
	  leveldb::DBTX tx(store);
#else
	  leveldb::DBROTX tx(store);
#endif
	  int num_distinct = 0; 
	  while (true) {

retry:
	
  	    tx.Begin();
		 
	    //-------------------------------------------------------------------------
	    //The row in the DISTRICT table with matching D_W_ID and D_ID is selected and D_NEXT_O_ID is retrieved.
	    //-------------------------------------------------------------------------
	  
	    int64_t d_key = makeDistrictKey(warehouse_id, district_id);
	  
  	    uint64_t *d_value;
	//	uint64_t slstart = rdtsc();
	    bool found = tx.Get(DIST, d_key, &d_value);
#if PROFILE
	    rcount++;
#endif
	    assert(found);
        District *d = reinterpret_cast<District *>(d_value);   
	//	memcpy(district_dummy, d, sizeof(d));
	//	secs += (rdtsc() - slstart);
	    int32_t o_id = d->d_next_o_id;

	    //-------------------------------------------------------------------------
	    //All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID), 
	    //and OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected.
	    //-------------------------------------------------------------------------
	    int i = o_id - 20;
	    std::vector<int32_t> s_i_ids;
        // Average size is more like ~30.
        s_i_ids.reserve(300);

#if SLDBTX
		DBTX::Iterator iter(&tx, ORLI);
#else
	    DBROTX::Iterator iter(&tx, ORLI);
#endif
	    int64_t start = makeOrderLineKey(warehouse_id, district_id, i, 1);
	//	uint64_t slstart = rdtsc();
	    iter.Seek(start);
	//	secs += (rdtsc() - slstart);
	    int64_t end = makeOrderLineKey(warehouse_id, district_id, o_id, 1);
	    while (iter.Valid()) {
	//	  slstart = rdtsc();
	  	  int64_t ol_key = iter.Key();
		  if (ol_key >= end) break;

#if AGGRESSIVEDETECT
		  if(tx.hasConflict()){
		  	goto retry;
		  }
#endif
	  	  uint64_t *ol_value = iter.Value();
		  
#if PROFILE
		  icount++;
#endif
		  OrderLine *ol = reinterpret_cast<OrderLine *>(ol_value);   
	//	  memcpy(orderline_dummy, ol, sizeof(OrderLine));
		  
		  //-------------------------------------------------------------------------
		  //All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) 
		  //from the list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock).
		  //-------------------------------------------------------------------------
		  //printf("ol %lx\n",ol);
		  //printf("ol_key %ld end %ld\n",ol_key, end);

		  int32_t s_i_id = ol->ol_i_id;
//		  secs += (rdtsc() - slstart);

#if 0
		  if (s_i_id < 1 || s_i_id > Stock::NUM_STOCK_PER_WAREHOUSE)  {
		  	printf("1992 ol_key %ld start %ld end %ld\n",ol_key, start, end);
		  	printf("1993 -------- %ld\n", ol->ol_i_id);
			uint64_t *temp;
			tx.Get(ORLI, ol_key, &temp);
			ol = reinterpret_cast<OrderLine *>(temp);
			printf("1997 TEMP %ld\n", ol->ol_i_id);
			
		  }
#endif		  
#if 1		  
		  int64_t s_key = makeStockKey(warehouse_id, s_i_id);
		  
		  uint64_t *s_value;
	//	  slstart = rdtsc();
		  found = tx.Get(STOC, s_key, &s_value);
		  Stock *s = reinterpret_cast<Stock *>(s_value);
	  
	//	  memcpy(stock_dummy, s, sizeof(Stock));
	//	  secs += (rdtsc() - slstart);
#if PROFILE
		  rcount++;
#endif
		  //Stock *s = reinterpret_cast<Stock *>(s_value);
		  //memcpy(stock_dummy, s, sizeof(Stock));
		  if (s->s_quantity < threshold) 
		  	s_i_ids.push_back(s_i_id);
#endif			  
	//  	  slstart = rdtsc();
		  iter.Next();
	//	  secs += (rdtsc() - slstart);
#if AGGRESSIVEDETECT
		  if(tx.hasConflict()){
		    goto retry;
		}
#endif

	    }

	    std::sort(s_i_ids.begin(), s_i_ids.end());
      
        int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
        for (size_t i = 0; i < s_i_ids.size(); ++i) {
          if (s_i_ids[i] != last) {
            last = s_i_ids[i];
            num_distinct += 1;
          }
        }    

	    bool b = tx.End();  
		if (b) break;
		
#if ABORTPRO
		atomic_add64(&stocklevelabort, 1);
#endif
		
	  }
#if PROFILE
	  atomic_add64(&stocklevelreadcount, rcount);
	  atomic_add64(&stocklevelitercount, icount);
	  while (rcount > stocklevelreadmax) stocklevelreadmax = rcount;
	  while (icount > stocklevelitermax) stocklevelitermax = icount;
	  while (rcount < stocklevelreadmin) stocklevelreadmin = rcount;
	  while (icount < stocklevelitermin) stocklevelitermin = icount;
#endif
  	 
	  return num_distinct;
	
  }

  void TPCCSkiplist::delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
		  std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo){
#if PROFILE
	uint32_t wcount = 0;
	uint32_t rcount = 0;
	uint32_t icount = 0;
#endif 
#if ABORTPRO || PROFILE
	atomic_add64(&delivernum, 1);
#endif	

    leveldb::DBTX tx(store);
	while (true) {
	  tx.Begin();
	  orders->clear();
  	  for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; ++d_id) {

#if PROFILEDELIVERY
	  uint64_t sstart = rdtsc();
#endif
	    //-------------------------------------------------------------------------
	    //The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) 
	    //and with the lowest NO_O_ID value is selected.
	    //-------------------------------------------------------------------------
	    int32_t no_id = 1;
		uint64_t *no_value;
	    NewOrder *no = NULL;
		//printf("w %d d %d\n", warehouse_id, d_id);
		int64_t start = makeNewOrderKey(warehouse_id, d_id, 1);
		DBTX::Iterator iter(&tx, NEWO);

#if PROFILEDELIVERY
	 	
		sstart = rdtsc();
		
		seekCount ++;

		iter.SeekProfiled(start);

     		dstep1 += rdtsc() - sstart;
		sstart = rdtsc();

#else
		iter.Seek(start);
#endif
		int64_t end = makeNewOrderKey(warehouse_id, d_id, Order::MAX_ORDER_ID);
		int64_t no_key;


		if (iter.Valid()) { 

		no_key = iter.Key();
		


			if (no_key <= end) {
			  no_value = iter.Value();
#if PROFILE
			  icount++;
#endif
			  no = reinterpret_cast<NewOrder *>(no_value);
			  no_id = static_cast<int32_t>(no_key << 32 >> 32);

			  //-------------------------------------------------------------------------
			  //The selected row in the NEW-ORDER table is deleted.
			  //-------------------------------------------------------------------------
		  	  tx.Delete(NEWO, no_key);
#if PROFILE
			  wcount++;
#endif
			}
	    }
	    //else printf("Not valid\n");
	    if (no == NULL) {
          // No orders for this district
          // TODO: 2.7.4.2: If this occurs in max(1%, 1) of transactions, report it (???)
          printf("NoOrder for district %d!!\n",  d_id);
		  iter.SeekToFirst();
		  printf("Key %ld\n", iter.Key());
          continue;
        }
	
#if 0 //PROFILEDELIVERY
	dstep1 += rdtsc() - sstart;
	search += tx.searchTime;
	traverse += tx.traverseTime;
	sstart = rdtsc();
#endif

	DeliveryOrderInfo order;
        order.d_id = d_id;
        order.o_id = no_id;
        orders->push_back(order);
	//-------------------------------------------------------------------------
	//The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) is selected, 
	//O_C_ID, the customer number, is retrieved, and O_CARRIER_ID is updated.
	//-------------------------------------------------------------------------


	int64_t o_key = makeOrderKey(warehouse_id, d_id, no_id);
		
	uint64_t *o_value;
	bool found = tx.Get(ORDE, o_key, &o_value);
#if PROFILE
	rcount++;
#endif
	Order *o = reinterpret_cast<Order *>(o_value);

	assert(o->o_carrier_id == Order::NULL_CARRIER_ID);

	//Order *newo = new Order();		

	updateOrder(order_dummy, o, carrier_id);

	uint64_t *o_v = reinterpret_cast<uint64_t *>(order_dummy);
	//o->o_carrier_id = carrier_id;
	tx.Add(ORDE, o_key, o_v, sizeof(Order));
#if PROFILEDELIVERY
	dstep2 += rdtsc() - sstart;
	sstart = rdtsc();
#endif

#if PROFILE
	wcount++;
#endif
	int32_t c_id = o->o_c_id;

	//-------------------------------------------------------------------------
	//All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected. 
	//All OL_DELIVERY_D, the delivery dates, are updated to the current system time as returned by the operating system 
	//and the sum of all OL_AMOUNT is retrieved.
	//-------------------------------------------------------------------------
	float sum_ol_amount = 0;
	DBTX::Iterator iter1(&tx, ORLI);
	start = makeOrderLineKey(warehouse_id, d_id, no_id, 1);
	iter1.Seek(start);
	end = makeOrderLineKey(warehouse_id, d_id, no_id, 15);
	while (iter1.Valid()) {
	  int64_t ol_key = iter1.Key();
	  if (ol_key > end) break;
	  uint64_t *ol_value = iter1.Value();

#if PROFILE
	  icount++;
#endif
	  OrderLine *ol = reinterpret_cast<OrderLine *>(ol_value);
	  //OrderLine *newol = new OrderLine();
	  updateOrderLine(orderline_dummy, ol, now);
	  uint64_t *ol_v = reinterpret_cast<uint64_t *>(orderline_dummy);
	  //memcpy(ol->ol_delivery_d, now, 15);
	  
	  assert (orderline_dummy->ol_i_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
	  tx.Add(ORLI, ol_key, ol_v, sizeof(OrderLine));
#if PROFILE
	  wcount++;
#endif
	  sum_ol_amount += ol->ol_amount;
	  iter1.Next();

	}

#if PROFILEDELIVERY
	dstep3 += rdtsc() - sstart;
	sstart = rdtsc();
#endif

	//-------------------------------------------------------------------------
	//The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected 
	//and C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT) previously retrieved. 
	//C_DELIVERY_CNT is incremented by 1.
	//-------------------------------------------------------------------------

	int64_t c_key = makeCustomerKey(warehouse_id, d_id, c_id);
	
	uint64_t *c_value;
	found = tx.Get(CUST, c_key, &c_value);
#if PROFILE
	rcount++;
#endif
	Customer *c = reinterpret_cast<Customer *>(c_value);
	//Customer *newc = new Customer();
	updateCustomerDelivery(customer_dummy, c, sum_ol_amount);
	uint64_t *c_v = reinterpret_cast<uint64_t *>(customer_dummy);
	//c->c_balance = c->c_balance + sum_ol_amount;
	//c->c_delivery_cnt = c->c_delivery_cnt + 1;
	tx.Add(CUST, c_key, c_v, sizeof(Customer));
#if PROFILE
	wcount++;
#endif
	 

#if PROFILEDELIVERY
	dstep4 += rdtsc() - sstart;	
#endif


   }
	
  bool b = tx.End();  
  if (b) break;

#if ABORTPRO
	  atomic_add64(&deliverabort, 1);
#endif
	}

#if PROFILEBUFFERNODE                                                                                                                                         
  bufferMiss += tx.bufferMiss;
  bufferHit += tx.bufferHit;
  bufferGet += tx.bufferGet;
#endif

#if PROFILEDELIVERY
   search += tx.searchTime;
   traverse += tx.traverseTime;
   traverseCount += tx.traverseCount;

if(seekCount % 1000 == 0)
  printf("Avg Count %ld [%ld / %ld]\n", traverseCount / seekCount, traverseCount, seekCount);

#endif	
	
#if PROFILE
	atomic_add64(&deliverreadcount, rcount);
	atomic_add64(&deliveritercount, icount);
	atomic_add64(&deliverwritecount, wcount);
	while (rcount > deliverreadmax) deliverreadmax = rcount;
	while (icount > deliveritermax) deliveritermax = icount;
	while (wcount > deliverwritemax) deliverwritemax = wcount;
	while (rcount < deliverreadmin) deliverreadmin = rcount;
	while (icount < deliveritermin) deliveritermin = icount;
	while (wcount < deliverwritemin) deliverwritemin = wcount;
#endif	
    return;
  }

//not used yet
bool TPCCSkiplist::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
            const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
            TPCCUndo** undo){
  return false;
}

void TPCCSkiplist::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}
void TPCCSkiplist::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}

bool TPCCSkiplist::hasWarehouse(int32_t warehouse_id){
  return true;
}
	
void TPCCSkiplist::applyUndo(TPCCUndo* undo){
  return;
}
void TPCCSkiplist::freeUndo(TPCCUndo* undo){
  return;
}

void TPCCSkiplist::printSkiplist() {
	for (int i=0; i<store->number; i++){
		printf("========== Table %d ==========",i);
		store->tables[i]->PrintStore();
	}
}


 
}

