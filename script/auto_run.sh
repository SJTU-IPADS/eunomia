#!/bin/bash
for i in 2 4 6 8 10 12 14 16 18 20
do
	./simple_run.sh $i &> thread_$i_break.dat
done	
