#!/bin/bash
if [ $# != 2 ]; then
	echo "./simple_run.sh <num_threads> <num_warehouses>"
	exit 1
fi

num_threads=$1
total_txns=1000000
op_per_thread=$(expr $total_txns / $num_threads)
#op_per_thread=100000
scale_factor=$2
echo "../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $op_per_thread --retry-aborted-transactions --verbose --txn-flags 1 --scale-factor $scale_factor --bench-opts "--workload-mix 100,0,0,0,0""
../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $op_per_thread --retry-aborted-transactions --verbose --txn-flags 1 --scale-factor $scale_factor --bench-opts "--workload-mix 100,0,0,0,0"
