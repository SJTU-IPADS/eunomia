#!/bin/bash
for dist_type in 0 1 2 3
do
	for tree_type in 0 1
	do
		for read_rate in 0 0.2 0.5 0.8
		do
			for thr in 1 2 4 8 12 16 20
			do
				echo "./run_ycsb.sh $thr $tree_type $read_rate $dist_type"
				./run_ycsb.sh $thr $tree_type ${read_rate} ${dist_type}| tee ycsb_log/ycsb_${thr}_${tree_type}_${read_rate}_${dist_type}
			done
		done
	done
done
