#!/bin/bash
for warehouse in 1 2 4 8 12 16 20
do
	echo "Threads = $warehouse Warehouses = $warehouse"
	./simple_run.sh $warehouse $warehouse &> temp.$warehouse
	cat temp.$warehouse | grep "runtime"
	cat temp.$warehouse | grep "mixed_throughput"
	cat temp.$warehouse | grep "total_abort_num"
	cat temp.$warehouse | grep "DBTX"
	cat temp.$warehouse | grep "ORLI"
	cat temp.$warehouse | grep "STOC"
	cat temp.$warehouse | grep "Conflict Count"
	#rm temp.$warehouse
done
