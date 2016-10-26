#!/bin/bash
echo "This scripts is to run a representative example of ycsb benchmark"
#if [ $# -lt 4 ]; then
	#echo "./run_ycsb.sh <thread_num> <type> <read_rate> <dist_type> [contention_size] [theta]"
	#echo "dist_type: (0) Sequential Dist (1) Uniform Dist (2) Normal Dist (3)Cauchy Dist (4)Zipf Dist (5)SelfSimilar Dist"
	#exit 0
#fi
echo "Baseline Version"
../ycsb_test --benchmark=mix --threads=16 --euno=0 --read-rate=0.5 --func=4 --cont-size=20 --theta=0.9 --num=10000000
	
echo "Eunomia Version"
../ycsb_test --benchmark=mix --threads=16 --euno=1 --read-rate=0.5 --func=4 --cont-size=20 --theta=0.9 --num=10000000
