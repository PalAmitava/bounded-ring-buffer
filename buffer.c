# include "header.h"
# define BUFFER_SIZE 10

static buffer_item *buffer;
static int in;
static int out;
static int cap=0;
static sem_t empty;
static sem_t full;
static pthread_mutex_t mutex;


static inline void safe_sem_wait(sem_t* sem)
{
    //sem_wait(sem) if sem=0 its blocking
    while(sem_wait(sem)==-1)
    {
        if(errno==EINTR)
        {
            //say os interruption for some signal delivery the sys call is aborted we restart it
            //i.e we wait for the semaphore no rush into critical section
            continue; 
        }
        perror("Fatal:sem_wait failed\n");//like sem doesnt exists or passed pointer to memory region not a sem
        exit(EXIT_FAILURE); //even if dev mistake we crash program
        
    }
    
}
static inline void safe_sem_post(sem_t* sem)
{
    //this wont block its indivisible or atomic
    if(sem_post(sem)==-1)
    {
        perror("Fatal:sem_post failed\n");//like sem doesnt exists or passed wrong pointer
        //or called safe_sem_post so many times that it exceeded SEM_VALUE_MAX
        exit(EXIT_FAILURE);
    }
}
void init_buffer(int sz)
{
    cap=sz;
    buffer=(buffer_item*)malloc((size_t)cap*sizeof(buffer_item));
    if(buffer==NULL)
    {
        perror("init_buffer[Malloc]\n");
        exit(EXIT_FAILURE);
    }
    in=0;
    out=0;
    SAFE_PTHREAD(pthread_mutex_init(&mutex,NULL));
    if(sem_init(&empty,0,(uint)cap)==-1){ perror("Sem_init[empty]\n"); exit(EXIT_FAILURE);}
    if(sem_init(&full,0,0)==-1){ perror("Sem_init[full]\n"); exit(EXIT_FAILURE);}
     //2nd argument is flg =0 which tells sem shared by threads of same process that created the sem
}

void rm_buf()
{
    free(buffer);
    sem_destroy(&empty);
    sem_destroy(&full);
    pthread_mutex_destroy(&mutex);
}

int put_item(buffer_item item)
{
    safe_sem_wait(&empty);
    //might be awaken simply to terminate
    if(atomic_load(&running)==false) return 1;
    SAFE_PTHREAD(pthread_mutex_lock(&mutex)); //no point waiting for empty while blocked on mutex
    buffer[in]=item;
    in=(in+1)%cap;
    SAFE_PTHREAD(pthread_mutex_unlock(&mutex)); //released in opp. ordr deadlock avoidded
    safe_sem_post(&full);

    return 0;
}

int rm_item(buffer_item* item)
{
    safe_sem_wait(&full);
    
    if(atomic_load(&running)==false) return 1;
    SAFE_PTHREAD(pthread_mutex_lock(&mutex));

    *item=buffer[out];
    out=(out+1)%cap;

    SAFE_PTHREAD(pthread_mutex_unlock(&mutex));
    safe_sem_post(&empty);
    return 0;
}


void wakeup_all_blockedthreads(int producers,int consumers)
{
    for(int i=0;i<producers;i++)
    {
        if(sem_post(&empty)==-1)//assuming worst case all producers blocked on empty each sem_post wakes each up
                                //else sem_value simply increases might EOVERFLOW or sem simply d.n.e
        {
            break;

        }
    }
    for(int i=0;i<consumers;i++)
    {
        if(sem_post(&full)==-1) 
        {
            break;
        }
    }

}
//mutex is not a counting semaphore only thread that locked it can unlock it and not the main thread 


