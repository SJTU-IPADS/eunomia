#include "db/dbtables.h"
#include "db/dbtx.h"
#include <stdio.h>
#include <vector>
int main(int argc, char**argv){


         leveldb::DBTables *store = new leveldb::DBTables(1);
         store->AddTable(0, BTREE, 0);
	store->RCUInit(1);
	store->ThreadLocalInit(0);

	uint64_t *keys = new uint64_t[2];
	keys[0] =1 ; keys[1] = 4;
	store->tables[0]->Put(1,keys);

	 leveldb::DBTX tx(store);
#if 0
	 tx.Begin();
	uint64_t keys[2];
	keys[0] = 1; keys[1]=4;
	tx.Add(0,1, keys, sizeof(keys));
	tx.End();
#endif
	tx.Begin();
	uint64_t *value;
	tx.Get(0, 1, &value);
	
	int num = value[0];
	uint64_t *prikeys = new uint64_t[num+2];
	prikeys[0] = num + 1;
	for (int i=1; i<=num; i++) 
		prikeys[i] = value[i];
	prikeys[num+1] = 6;
	tx.Add(0, 1, prikeys, (num+2)*8);
	tx.End();
	tx.Begin();
	tx.Get(0, 1, &value);
	tx.End();
	printf("%ld %ld %ld \n",value[0],value[1],value[2]);
	 

return 0;
}
