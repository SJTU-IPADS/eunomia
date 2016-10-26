for i in  1 2 3 4 5 6 7 8 #12 16 20 24 28 32 #thread number
do
	for j in persistent #simple alloc rtm fb nodebuf nogc noss sep
	do
		echo "$i $j," >> prof.csv
		echo "eval_${j}_${i}_"
		python get_th.py eval_${j}_${i}_  5  >> prof.csv
	done
	
	#for j in profile padding
	#do
	#	python get_pth.py nper_${j}_${i}_ 3 >> nprof.csv
	#done
done
