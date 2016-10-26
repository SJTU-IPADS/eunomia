#!/bin/bash
for thr in 1 2 4 8 12 16 20
do
	#for theta in 0.5 0.7 0.9 0.99
   	for read_rate in 0 0.2 0.5 0.8	
	do
		echo "./run_ycsb_zipf.sh $thr 1 $read_rate 4 20 0.9"
		./run_ycsb_zipf.sh $thr 1 $read_rate 4 20 0.9 | tee read_logs/ycsb_zipf_euno_${thr}_${read_rate}
		echo "./run_ycsb_zipf.sh $thr 0 $read_rate 4 20 0.9"
		./run_ycsb_zipf.sh $thr 0 $read_rate 4 20 0.9 | tee read_logs/ycsb_zipf_orig_${thr}_${read_rate}
	done
done
