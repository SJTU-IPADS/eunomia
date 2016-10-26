#!/bin/bash
theta=$1
for thread in 1 2 4 8 12 16 20
do
	./run_ycsb_zipf.sh $thread 0 0.5 4 20 $theta
done
