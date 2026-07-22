# include "header.h"
# define NAT_TERM 0
# define SIG_TERM 1
# define MY_RAND_MAX 30
//abv defn of barrier in thread lib prevent error squiggles

void* producer(void* param);
void* consumer(void* param);

int *ids1=NULL,*ids2=NULL; //for cleanup associated with atexit

pthread_t *tid_p=NULL,*tid_c=NULL;
//initialised to NULL to prevent segfaults in premature termination : graceful shutdown

static pthread_barrier_t start_line; //producers and consumer methods have access
atomic_bool running;
sig_atomic_t flag=1;
static bool barrier_initialised=false;
void cleanup()
{
    free(tid_p);
    free(tid_c);
    free(ids1);
    free(ids2);
    if(barrier_initialised)
    SAFE_PTHREAD(pthread_barrier_destroy(&start_line)); //we might destroy uninitialized barrier during premature termination
    rm_buf();
}
void handler(int signal)
{
    (void)signal;
    const char buf[]="From insider the custom handler.\n";
    write(STDOUT_FILENO,&buf,sizeof(buf)-1);
    flag=0;
    atomic_store(&running,false);
    
}

int main(int argc,char* argv[])
{
    atexit(cleanup);
    struct sigaction s;
    s.sa_handler=handler;
    s.sa_flags=0;
    sigemptyset(&s.sa_mask);
    if(sigaction(SIGINT,&s,NULL)==-1)
    {
        perror("sigaction");
        exit(EXIT_FAILURE);
    }
    if(argc!=5)
    {
        fprintf(stdout,"Usage: ./main <sleep_before_exit> <no. of producers> <no. of consumers> <buffer_sz>\n");
        return 1;
    }
    int sleep_time=atoi(argv[1]);
    int producers=atoi(argv[2]);
    int consumers=atoi(argv[3]);
    int buf_sz=atoi(argv[4]);

    atomic_store(&running,true);
    init_buffer(buf_sz);

    pthread_attr_t attr;
    SAFE_PTHREAD(pthread_attr_init(&attr));
    SAFE_PTHREAD(pthread_attr_setscope(&attr,PTHREAD_SCOPE_SYSTEM)); //default for linux
    
    int total=producers+consumers;
    SAFE_PTHREAD(pthread_barrier_init(&start_line,NULL,(uint)total)); //NULL def. attribut
    barrier_initialised=true;

    tid_p=(pthread_t*)malloc((size_t)producers*sizeof(pthread_t));
    tid_c=(pthread_t*)malloc((size_t)consumers*sizeof(pthread_t));
    if(tid_p==NULL || tid_c == NULL)
    {
        perror("Malloc main\n");
        exit(EXIT_FAILURE);
    }
    //creating threads
    ids1=(int*)malloc((size_t)producers*sizeof(int));
    if(ids1==NULL)
    {
        perror("Malloc ids1\n");
        exit(EXIT_FAILURE);
    }
    for(int i=0;i<producers;i++)
    {
        ids1[i]=100+i;
        SAFE_PTHREAD(pthread_create(&tid_p[i],&attr,producer,&ids1[i]));
    }
    ids2=(int*)malloc((size_t)consumers*sizeof(int));
    if(ids2==NULL)
    {
        perror("Malloc ids2\n");
        exit(EXIT_FAILURE);
    }
    for(int i=0;i<consumers;i++)
    {
        ids2[i]=500+i;
        SAFE_PTHREAD(pthread_create(&tid_c[i],&attr,consumer,&ids2[i]));
    }
    printf("MAIN GOING TO SLEEP\n");
    sleep((uint)sleep_time);//when signal arrives it will be awaken as TASK_INTERRUPTIBLE and wont be restarted
    printf("Main has awaken from sleep\n");
    atomic_store(&running,false);
    wakeup_all_blockedthreads(producers,consumers);
    for(int i=0;i<producers;i++)
    {
        
        void *retval;
        pthread_join(tid_p[i],&retval);
        if(retval==(void*)NAT_TERM)
        {
            printf("producer %d had ended due to natural termination\n",ids1[i]);
        }
        else if(retval==(void*)SIG_TERM)
        {
            printf("producer %d had ended due to signal termination\n",ids1[i]);
        }
    }
    for(int i=0;i<consumers;i++)
    {
        void *retval;
        pthread_join(tid_c[i],&retval);
        if(retval==(void*)NAT_TERM)
        {
            printf("consumer %d had ended due to natural termination \n",ids2[i]);
        }
        else if(retval==(void*)SIG_TERM)
        {
            printf("consumer %d had ended due to signal termination\n",ids2[i]);
        }
    }
    exit(EXIT_SUCCESS);

}
void* consumer(void* param)
{
    int ucid=*(int*)param;
    int status=pthread_barrier_wait(&start_line);
    if(status==PTHREAD_BARRIER_SERIAL_THREAD) //this special value returned to exactly one thread rest receive 0 guranteeing messagae printed once
    {
        printf("All producers and consumers ready!. Starting execution...\n");
        
        printf("Last thread to be ready was consumer:%d\n",ucid);
    }
    else if(status !=0)
    {
        fprintf(stderr,"barrier failed:%s\n",strerror(status));
        exit(EXIT_FAILURE);
    }
    buffer_item item;
    while(atomic_load(&running))
    {
        sleep((uint)(rand()%MY_RAND_MAX));
        if(rm_item(&item))
        {
           if(!flag)
           pthread_exit((void*)SIG_TERM);
           pthread_exit((void*)NAT_TERM);
        }
        else
        {
            printf("consumer with %lu tid and serial id :%d consumed:%d\n",(unsigned long)pthread_self(),ucid,item);
        }
      
    }
    return NULL;
}

void* producer(void* param)
{
    int upid=*(int*)param;
    int status=pthread_barrier_wait(&start_line);
    if(status==PTHREAD_BARRIER_SERIAL_THREAD)
    {
        printf("All producers and consumers ready!. Starting execution...\n");
        printf("Last thread to be ready was producer:%d\n",upid);
    }
    else if(status !=0)
    {
        fprintf(stderr,"barrier failed:%s\n",strerror(status));
        exit(EXIT_FAILURE);
    }
    while(atomic_load(&running))
    {
        sleep((uint)(rand()%MY_RAND_MAX));
        buffer_item item= rand()%MY_RAND_MAX;
        if(put_item(item))
        {
           if(!flag)
           pthread_exit((void*)SIG_TERM);
           pthread_exit((void*)NAT_TERM); 
        }
        else
        {
            printf("producer with %lu tid and serial id :%d produced:%d\n",(unsigned long)pthread_self(),upid,item);
        }
    }
    return NULL;


}