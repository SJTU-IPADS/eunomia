
#include <assert.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <asm-generic/errno-base.h>
#include <errno.h>
#include <stdio.h>
#include "log.h"

Log::Log(const char* p, bool sync): path(p)
{ 
	printf("Create log %s\n", path);
	fd = open(path, O_RDWR|O_CREAT, S_IRWXU|S_IRWXG|S_IRWXO);

	
	//If it's aready exist, first delete it then create
   if(fd == -1 && errno == EEXIST) {
		   int err = remove(path);
		   if(err < 0)
				   perror("LOG ERROR: remove log file\n");
		   fd = open(path, O_RDWR|O_CREAT|O_EXCL, S_IRWXU|S_IRWXG|S_IRWXO);
   }

   if(fd < 0) {
		   perror("LOG ERROR: create log file\n");
		   exit(1);
   }
}

Log::~Log()
{
	closelog();
}


void Log::writeLog(char *data, int len)
{
	//printf("write len %d\n", len);
	ssize_t res = write(fd, (void *)data, len);
	if(res < 0)
		 perror("LOG ERROR: write file\n");
}

void Log::closelog()
{
	close(fd);	
}

