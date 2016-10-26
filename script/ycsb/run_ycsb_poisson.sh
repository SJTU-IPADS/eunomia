#!/bin/bash
for thr in 1 2 4 8 12 16 20
do
	#for theta in 0.5 0.7 0.9 0.99
	echo "./run_ycsb.sh $thr 1 0.2 6"
	./run_ycsb.sh $thr 1 0.5 6 | tee logs/poisson/ycsb_poisson_euno_${thr}
done
for thr in 1 2 4 8 12 16 20
do
	#for theta in 0.5 0.7 0.9 0.99
	echo "./run_ycsb.sh $thr 0 0.2 6"
	./run_ycsb.sh $thr 0 0.5 6 | tee logs/poisson/ycsb_poisson_base_${thr}
done
