#!/bin/bash
for thr in 1 2 4 8 12 16 20
do
	#for theta in 0.5 0.7 0.9 0.99
	echo "./run_ycsb.sh $thr 1 0.2 5"
	./run_ycsb.sh $thr 1 0.2 5 | tee logs/selfsimilar/ycsb_self_euno_${thr}
done
for thr in 1 2 4 8 12 16 20
do
	#for theta in 0.5 0.7 0.9 0.99
	echo "./run_ycsb.sh $thr 0 0.2 5"
	./run_ycsb.sh $thr 0 0.2 5 | tee logs/selfsimilar/ycsb_self_base_${thr}
done
