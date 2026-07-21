# define _XOPEN_SOURCE 700
# include <stdlib.h>
# include <stdio.h>
# include <semaphore.h>
# include <pthread.h>
# include <stdbool.h>
# include <unistd.h>
# include <sys/types.h>
# include <stdatomic.h>
# include <errno.h>
# include <string.h>
# include <signal.h>
# define PTHREAD_BARRIER_SERIAL_THREAD -1 
# define SAFE_PTHREAD(call)\
do{\
int rc=(call);\
if(rc!=0){\
fprintf(stderr,"Fatal [%s %d]:%s failed :%s\n",__FILE__,__LINE__,#call,strerror(rc));\
exit(EXIT_FAILURE);\
}\
}while(0)


typedef int buffer_item;
typedef unsigned int uint;

void init_buffer(int sz);
void rm_buf();
int put_item(buffer_item item);
int rm_item(buffer_item* item);

void wakeup_all_blockedthreads(int producers,int consumers);
extern atomic_bool running;
extern sig_atomic_t flag;



