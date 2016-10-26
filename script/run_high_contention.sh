#!/bin/bash
for thread in 1 2 4 8 12 16 20
do
	echo "Threads = $thread Warehouses = 1"
	./simple_run.sh $thread 1 &> thread.$thread.csv
	#./simple_run_fix_workload.sh $thread 2 &> thread.$thread.csv
	cat thread.$thread.csv | grep "runtime"
	#cat thread.$thread.csv | grep "reason"
	cat thread.$thread.csv | grep "phase"
	cat thread.$thread.csv | grep "agg_nosync_mixed_throughput"
	cat thread.$thread.csv | grep "total_abort_num"
	cat thread.$thread.csv | grep "DBTX"
	cat thread.$thread.csv | grep "ORLI"
	cat thread.$thread.csv | grep "ITEM"
	cat thread.$thread.csv | grep "STOC"
	cat thread.$thread.csv | grep "spec_time"
	cat thread.$thread.csv | grep "spec_hit"
	cat thread.$thread.csv | grep "Conflict Counts"
	cat thread.$thread.csv | grep "Abort reason"
	cat thread.$thread.csv | grep "should_protect"
	cat thread.$thread.csv | grep "split_ops"
	cat thread.$thread.csv | grep "bm_found"
	cat thread.$thread.csv | grep "bm_time"
	#rm thread.$thread.csv
done
