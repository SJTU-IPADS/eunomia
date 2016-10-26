#include <iostream>
#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>
#include "time.h"

#include "bench.h"
#include "articles.h"

#include "db/dbtx.h"
#include "db/dbrotx.h"
#include "db/dbtables.h"
#include "util/rtm.h"
#include "port/atomic.h"

using namespace std;
using namespace util;
#define ARTICLES_SIZE 1000;
#define USERS_SIZE 200;
#define BATCH_SIZE 500;
#define MAX_COMMENTS_PER_ARTICLE 1000;

#define USER 0
#define ARTI 1
#define COMM 2

static unsigned g_txn_workload_mix[] = {10, 35, 35, 20};

struct _dummy {}; // exists so we can inherit from it, so we can use a macro in
// an init list...

class articles_worker_mixin : private _dummy {

public:
	DBTables *store;
	int64_t users_num;
	int64_t articles_num;
	articles_worker_mixin(DBTables *s) :
		_dummy() // so hacky...

	{
		store = s;
		users_num = USERS_SIZE;
//	users_num *= nthreads;
		articles_num = ARTICLES_SIZE;
//	articles_num *= nthreads;

	}
};

class articles_worker : public bench_worker, public articles_worker_mixin {
public:
	DBTX tx;
	DBROTX rotx;
	articles_worker(unsigned int worker_id,
					unsigned long seed, abstract_db *db,
					spin_barrier *barrier_a, spin_barrier *barrier_b,
					DBTables *store)
		: bench_worker(worker_id, true, seed, db,
					   barrier_a, barrier_b),
		articles_worker_mixin(store),
		tx(store),
		rotx(store)
	{
		obj_key0.reserve(2 * CACHELINE_SIZE);
		obj_key1.reserve(2 * CACHELINE_SIZE);
		obj_v.reserve(2 * CACHELINE_SIZE);
	}

	txn_result txn_update_user();
	txn_result txn_get_comments();
	txn_result txn_get_article();
	txn_result txn_add_comment();
	static txn_result
	TxnUpdateUser(bench_worker *w, bool first_run) {
		txn_result r =  static_cast<articles_worker *>(w)->txn_update_user();
		return r;
	}
	static txn_result
	TxnGetComments(bench_worker *w, bool first_run) {
		txn_result r =  static_cast<articles_worker *>(w)->txn_get_comments();
		return r;
	}
	static txn_result
	TxnGetArticle(bench_worker *w, bool first_run) {
		txn_result r =  static_cast<articles_worker *>(w)->txn_get_article();
		return r;
	}
	static txn_result
	TxnAddComment(bench_worker *w, bool first_run) {
		txn_result r =  static_cast<articles_worker *>(w)->txn_add_comment();
		return r;
	}

	virtual workload_desc_vec
	get_workload() const {
		workload_desc_vec w;

		unsigned m = 0;
		for(size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
			m += g_txn_workload_mix[i];
		ALWAYS_ASSERT(m == 100);
		if(g_txn_workload_mix[0])
			w.push_back(workload_desc("UpdateUser", double(g_txn_workload_mix[0]) / 100.0, TxnUpdateUser));
		if(g_txn_workload_mix[1])
			w.push_back(workload_desc("GetComments", double(g_txn_workload_mix[1]) / 100.0, TxnGetComments));
		if(g_txn_workload_mix[2])
			w.push_back(workload_desc("GetArticle", double(g_txn_workload_mix[2]) / 100.0, TxnGetArticle));
		if(g_txn_workload_mix[3])
			w.push_back(workload_desc("AddComment", double(g_txn_workload_mix[3]) / 100.0, TxnAddComment));
		return w;
	}
protected:

	virtual void
	on_run_setup() OVERRIDE {
		printf("%ld wid %d\n", pthread_self(), worker_id);
		store->ThreadLocalInit(worker_id - 8);
	}
private:
	string obj_key0;
	string obj_key1;
	string obj_v;
};


articles_worker::txn_result
articles_worker::txn_update_user() {
	int64_t u_id = r.next() % users_num + 1;
	string last = r.next_string(3);

	uint64_t *u_value;
	tx.Begin();
	tx.Get(USER, u_id, &u_value);
	users::value *v_u = (users::value *)u_value;
	users::value v_u_new(*v_u);
	v_u_new.u_last.assign(last);
	tx.Add(USER, u_id, (uint64_t *)(&v_u_new), sizeof(v_u_new));

	return txn_result(tx.End(), 0);
}

articles_worker::txn_result
articles_worker::txn_get_comments() {
	int64_t a_id = r.next() % articles_num + 1;
	uint64_t max = MAX_COMMENTS_PER_ARTICLE;
	int64_t start = a_id * max;
	int64_t end = a_id * max + max;

	rotx.Begin();

	DBROTX::Iterator iter(&rotx, COMM);
	iter.Seek(start);
	while(iter.Valid() && iter.Key() < end) {
		uint64_t *c_value = iter.Value();
		comments::value *v_c = (comments::value *)c_value;
		iter.Next();
	}
	rotx.End();
	return txn_result(true, 0);
}

articles_worker::txn_result
articles_worker::txn_add_comment() {
	int64_t a_id = r.next() % articles_num + 1;
	uint64_t max = MAX_COMMENTS_PER_ARTICLE;
	tx.Begin();
	uint64_t *a_value;
	int64_t num;
	tx.Get(ARTI, a_id, &a_value);
	articles::value *v_a = (articles::value *)a_value;
	num = v_a->a_num_comments;
	if(num < max) {
		articles::value v_a_new(*v_a);
		v_a_new.a_num_comments++;
		tx.Add(ARTI, a_id, (uint64_t *)(&v_a_new), sizeof(v_a_new));
		int64_t c_id = a_id * max + num;
		comments::value c_v;
		c_v.c_a_id = a_id;
		c_v.c_u_id = r.next() % users_num + 1;
		c_v.c_text.assign(r.next_string(100));
		tx.Add(COMM, c_id, (uint64_t *)&c_v, sizeof(c_v));
	}

	return txn_result(tx.End(), 0);
}

articles_worker::txn_result
articles_worker::txn_get_article() {
	int64_t a_id = r.next() % articles_num + 1;
	tx.Begin();
	uint64_t *a_value;
	int64_t num;
	tx.Get(ARTI, a_id, &a_value);
	articles::value *v_a = (articles::value *)a_value;
	num = v_a->a_num_comments;
	return txn_result(tx.End(), num);
}

class articles_users_loader : public bench_loader, public articles_worker_mixin {
public:
	articles_users_loader(unsigned long seed,
						  abstract_db *db,
						  DBTables* store)
		: bench_loader(seed, db),
		  articles_worker_mixin(store) {
	}

protected:
	virtual void
	load() {

		for(int i = 1; i <= users_num; i++) {
			users::value *v = new users::value();
			v->u_first.assign(r.next_string(3));
			v->u_last.assign(r.next_string(3));
			v->u_password.assign(r.next_string(3));
			v->u_email.assign(r.next_string(3));
			store->TupleInsert(USER, i, (uint64_t*)v, sizeof(users::value));
		}
	}
};

class articles_articles_loader : public bench_loader, public articles_worker_mixin {
public:
	articles_articles_loader(unsigned long seed,
							 abstract_db *db,
							 DBTables* store)
		: bench_loader(seed, db),
		  articles_worker_mixin(store) {
	}

protected:
	virtual void
	load() {
		uint64_t max = MAX_COMMENTS_PER_ARTICLE;
		//cout << "max " <<  max << endl;
		max++;
		for(int i = 1; i <= articles_num; i++) {
			articles::value *v = new articles::value();
			v->a_title.assign(r.next_string(100));
			v->a_text.assign(r.next_string(100));
			v->a_num_comments = r.next() % max;
			store->TupleInsert(ARTI, i, (uint64_t *)v, sizeof(articles::value));

			for(int j = 0; j < v->a_num_comments; j++) {
				int64_t c_id = i * (max - 1) + j;
				comments::value *c_v = new comments::value();
				c_v->c_a_id = i;
				c_v->c_u_id = r.next() % users_num + 1;
				c_v->c_text.assign(r.next_string(100));
				store->TupleInsert(COMM, c_id, (uint64_t *)c_v, sizeof(comments::value));
				//int64_t c_index_id = i << 32 | j;
			}
		}
	}
};

class articles_bench_runner : public bench_runner {
public:
	DBTables* store;
	articles_bench_runner(abstract_db * db)
		: bench_runner(db) {
		printf("Init Table\n");
		store = new DBTables(3, nthreads);
		for(int i = 0; i < 3; i++) {
			store->AddTable(i, BTREE, NONE);
		}
		store->AddSchema(USER, sizeof(int64_t), sizeof(users::value));
		store->AddSchema(ARTI, sizeof(int64_t), sizeof(articles::value));
		store->AddSchema(COMM, sizeof(int64_t), sizeof(comments::value));

	}

	virtual void initPut() {
	}
	virtual void print_persisted_info() {
	}

	virtual void final_check() {}
	virtual void sync_log() {
		store->Sync();
	}
	virtual vector<bench_loader *>
	make_loaders() {
		vector<bench_loader *> ret;
		ret.push_back(new articles_users_loader(9324, db,  store));
		ret.push_back(new articles_articles_loader(235443, db, store));
		return ret;
	}

	virtual vector<bench_worker *>
	make_workers() {
		vector<bench_worker *> ret;
		fast_random r(23984543);
		const int blockstart = 8;
		for(size_t i = 0; i < nthreads; i++) {
			ret.push_back(
				new articles_worker(
					blockstart + i,
					r.next(), db,
					&barrier_a, &barrier_b,
					store));
		}
		return ret;
	}
};

void
articles_do_test(int argc, char **argv) {
	abstract_db *db = NULL;
	optind = 1;
	articles_bench_runner r(db);
	r.run();
}
