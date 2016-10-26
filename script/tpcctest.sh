echo "mix"

for i in  1 2 4 6 8 #12 16 20 24 28 32 
do
   for j in 1 2 3 4 5
   do
	echo "../neworder_bench --benchmark=readonly --numtx=5000000 --numwarehouse=$i"
	../neworder_bench --benchmark=readonly --numtx=5000000 --numwarehouse=$i | tee -a ../eval/readonly_btree_noss_$i
   done
done
