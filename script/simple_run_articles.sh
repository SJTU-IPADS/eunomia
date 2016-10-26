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
echo "../dbtest --bench articles --num-threads $num_threads --ops-per-worker $op_per_thread --retry-aborted-transactions --verbose --txn-flags 1 --scale-factor $scale_factor"
#pin -t $PIN_ROOT/source/tools/ManualExamples/obj-intel64/inscount0.so -- ../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $m --retry-aborted-transactions --verbose
../dbtest --bench articles --num-threads $num_threads --ops-per-worker $op_per_thread --retry-aborted-transactions --verbose --txn-flags 1 --scale-factor $scale_factor
