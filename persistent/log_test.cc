#include "log.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "log.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#define COUNT 4*4096

int main() {

	char * path = "test.txt";
	printf("path %s\n", path);
	Log log(path);

	for(int i = 0; i < COUNT; i++) {
		log.writeLog((char *)&i, sizeof(i));
	}

	log.closelog();


	int fd = open(path, O_RDWR|O_CREAT);
	
	if(fd < 0) {
		perror("TEST ERROR: open log file\n");

		exit(1);
	}

	int tmp = 0;
	int i = 0;
	for(i = 0; i < COUNT; i++) {
		
		read(fd, (void *)&tmp, sizeof(int));
//		printf("%d\n", tmp);
		if(tmp != i)
			printf("ERROR i %d tmp %d\n", i , tmp);
	}

	printf("FINAL i %d tmp %d\n", i , tmp);
	
	return 1;
}
