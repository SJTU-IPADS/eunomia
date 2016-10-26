#!/bin/bash
log_file=ycsb_log.zipf.csv
rm -rf $log_file
for thr in 1 2 4 8 12 16 20
do
	declare -a array
	i=0
	for theta in 0.5 0.7 0.9 0.99
	do
			throughput=$(cat ycsb_zipf_euno_${thr}_${theta} \
				| grep -oP  "(?<=Thread\[0\] Throughput) [0-9]+") 
			array[$i]=$throughput
			i=$(($i+1))
			#echo $throughput
	done

	for i in $(seq 0 3)
	do
		#echo "array[$i] ${array[$i]}"
		echo -n "${array[$i]}," | tee -a $log_file
	done
	echo "" | tee -a $log_file
done
