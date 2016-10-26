#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <asm-generic/errno-base.h>
#include <errno.h>


#define WORKLOAD (1024*1024*1024) //total workload 4GB
#define CHUNCKSIZE (256*1024*1024) //Sync Unit
#define WRITESIZE 4096	//Write Unit



//Micro Sec
int diff_timespec(const struct timespec &end, const struct timespec &start)
{
    int diff = (end.tv_sec > start.tv_sec)?(end.tv_sec-start.tv_sec)*1000:0;
    assert(diff || end.tv_sec == start.tv_sec);
    if (end.tv_nsec > start.tv_nsec) {
        diff += (end.tv_nsec-start.tv_nsec)/1000000;
    } else {
        diff -= (start.tv_nsec-end.tv_nsec)/1000000;
    }
    return diff;
}



int main(int arg, char** argc) {

	bool random = false;

	
	if(arg > 1 && strcmp(argc[1], "-r") == 0) {
		random = true;
	}


	char *data = (char *)malloc(WORKLOAD);

	for (int i=0; i< WORKLOAD; i++) {
		if(random)
			data[i] = rand() % 256;
		else 
		data[i] = 'a';
	}

	long length = CHUNCKSIZE;

	char *path = "speedtest";
	
	int fd = open(path, O_RDWR|O_CREAT|O_EXCL, S_IRWXU|S_IRWXG|S_IRWXO);

	//If it's aready exist, first delete it then create
	if(fd == -1 && errno == EEXIST) {
		int err = remove(path);
		if(err < 0){
			perror("LOG ERROR: remove log file\n");
			exit(1);
		}
		fd = open(path, O_RDWR|O_CREAT|O_EXCL, S_IRWXU|S_IRWXG|S_IRWXO);
	}
	
	if(fd < 0) {
		perror("LOG ERROR: create log file\n");
		exit(1);
	}
	
	ftruncate(fd, WORKLOAD);

	char *start = (char *)mmap (0, WORKLOAD, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

	if(MAP_FAILED == start) {
		perror("LOG ERROR: mmap in LOG\n");
		exit(1);
	}
	

	if (madvise(start, length, MADV_SEQUENTIAL) == -1) {
        	perror("LOG ERROR: madvise in LOG\n");
	        exit(1);
	}

	
   
	printf("Init Done\n");	

	timespec starttime;
	if (clock_gettime(CLOCK_REALTIME, &starttime) == -1) {
        	perror("Get Time ERROR\n");
	        exit(1);
	}
		

	
	for(int i = 0; i < WORKLOAD; i += CHUNCKSIZE) {

		for(int j = 0; j < CHUNCKSIZE; j+= WRITESIZE)
			memcpy(start + i + j, data + i + j, WRITESIZE);

		if(msync(start + i, CHUNCKSIZE, MS_SYNC) == -1){
				perror("LOG ERROR: msync in enlarge");
			    exit(1);
		}
		
	}
		
	if (munmap(start, WORKLOAD) == -1) {
		perror("LOG ERROR: munmap in close");
		exit(1);
	}

	timespec endtime;
	if (clock_gettime(CLOCK_REALTIME, &endtime) == -1) {
        	perror("Get Time ERROR\n");
	        exit(1);
	}

	double speed = (WORKLOAD * 1000.0)/diff_timespec(endtime, starttime)/1000000.0;
	printf("Speed %lf MB/s \n", speed);

	
	fsync(fd);
	close(fd);

	
	int err = remove(path);
	if(err < 0){
		perror("LOG ERROR: remove log file\n");
		exit(1);
	}
	
}
