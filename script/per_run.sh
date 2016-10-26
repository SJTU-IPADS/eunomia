for i in origin padding dummy opt
do
	for j in 1 2 4 6 8
	do
		for k in 1 2 3
		do
			m=`expr 5000000 / $j `
			perf stat -e r1c9 -e r2c9 -e r4c9 -e r8c9 ./exec/dbtest_$i --bench tpcc --num-threads $j --scale-factor $j --ops-per-worker $m --retry-aborted-transactions > per_${i}_${j}_$k  2>&1
		done
	done
done
