CPP=g++-4.8
CPPFLAGS=-O2 -DRTM_ENABLED
#CPPFLAGS=-g2 -DRTM_ENABLED


workingset : workingset.o   
	$(CPP) -o $@ $< -lstdc++ -lpthread -lrt

cost : cost.o   
	$(CPP) -o $@ $< -lstdc++ -lpthread -lrt

%.o : %.cc
	$(CPP) $(CPPFLAGS) -c -mrtm $< 

clean :
	rm -f *.o workingset cost
