#!/bin/bash
if [ $# -lt 4 ]; then
	echo "./run_ycsb.sh <thread_num> <type> <read_rate> <dist_type> <h_value>" #[contention_size] [theta]
	echo "dist_type: (0) Sequential Dist (1) Uniform Dist (2) Normal Dist (3) Cauchy Dist (4) Zipf Dist (5) SelfSimilar Dist (6) Poisson Distribution"
	exit 0
fi
../../ycsb_test --benchmark=mix --num=8000000 --threads=$1 --euno=$2 --read-rate=$3 --func=$4 --hvalue=$5
