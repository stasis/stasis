#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <lladd/transactional.h>
#include <unistd.h>
#include <pthread.h>

#include <math.h>
#include <sys/time.h>
#include <time.h>

pthread_cond_t never;
pthread_mutex_t mutex;

int alwaysCommit;

#define MAX_SECONDS 100

#define COUNTER_RESOLUTION 240

int buckets[COUNTER_RESOLUTION];

// if we're using linux's crazy version of the pthread header, 
// it probably forgot to include PTHREAD_STACK_MIN 

#ifndef PTHREAD_STACK_MIN
#include <limits.h>
#endif

int activeThreads = 0;
int max_active = 0;

/*double avg_var = 0;
double max_var = 0;
double avg_mean = 0;
double max_mean = 0;*/

//static pthread_mutex_t hash_mutex = PTHREAD_MUTEX_INITIALIZER;
static int count;
static recordid hash;

static void * go (void * arg_ptr) {
  //  pthread_mutex_lock(&hash_mutex);

  pthread_mutex_lock(&mutex);
  activeThreads++;
  if(activeThreads > max_active) {
    max_active = activeThreads;
  }
  pthread_mutex_unlock(&mutex);

  int k = *(int*)arg_ptr;
  int j;
  int xid;// = Tbegin();

  double sum_x_squared = 0;
  double sum = 0;

  double log_multiplier = COUNTER_RESOLUTION / log(MAX_SECONDS * 1000000000.0);

  struct timeval timeout_tv;
  struct timespec timeout;

  gettimeofday(&timeout_tv, NULL);

  timeout.tv_sec = timeout_tv.tv_sec;
  timeout.tv_nsec = 1000 * timeout_tv.tv_usec;

  timeout.tv_nsec = (int)(1000000000.0 * ((double)random() / (double)RAND_MAX));

  timeout.tv_sec++;

  //  struct timeval start;

  pthread_mutex_lock(&mutex);
  pthread_cond_timedwait(&never, &mutex, &timeout);
  pthread_mutex_unlock(&mutex);
  

  /*  gettimeofday(&start, NULL);
  assert(timeout.tv_sec <= start.tv_sec);
  assert(timeout.tv_nsec <= start.tv_nsec || timeout.tv_sec < start.tv_sec);*/

  

  for(j = k * count; j < (k+1) *(count) ; j++) {

    
    //    struct timeval start,
    struct timeval endtime_tv;
    struct timespec endtime;
    //    gettimeofday(&start, NULL);


    //    start = timeout;


    /*    gettimeofday(&start, NULL);
    assert(timeout.tv_sec <= start.tv_sec);
    assert(timeout.tv_nsec <= start.tv_nsec || timeout.tv_sec < start.tv_sec);
    */
    if(alwaysCommit) {
      xid = Tbegin();
    }
    ThashInsert(xid, hash, (byte*)&j, sizeof(int), (byte*)&j, sizeof(int));
    if(alwaysCommit) {
      Tcommit(xid);
    }

    gettimeofday(&endtime_tv, NULL);

    endtime.tv_sec = endtime_tv.tv_sec;
    endtime.tv_nsec = 1000 * endtime_tv.tv_usec;

    double microSecondsPassed = 1000000000.0 * (double)(endtime.tv_sec - timeout.tv_sec);


    microSecondsPassed = (microSecondsPassed + (double)endtime.tv_nsec) - (double)timeout.tv_nsec;

    assert(microSecondsPassed > 0.0);


    sum += microSecondsPassed;
    sum_x_squared += (microSecondsPassed * microSecondsPassed) ;

    int bucket = (log_multiplier * log(microSecondsPassed));
    
    if(bucket >= COUNTER_RESOLUTION) { bucket = COUNTER_RESOLUTION - 1; }
    
    timeout.tv_sec++;
    pthread_mutex_lock(&mutex);
    buckets[bucket]++;
    pthread_cond_timedwait(&never, &mutex, &timeout);
    pthread_mutex_unlock(&mutex);

    //    printf("(%d)", k);
  }

  
  /*
  for(j = k * count; j < (k+1) *(count) ; j++) {
    int tmp = -100;
    TlogicalHashLookup(xid, hash, &j, sizeof(int), &tmp, sizeof(int));
    assert(j == tmp);
    } */

  //  double count_d = count;
  //  double mean     = sum / count_d;
  //  double variance = sqrt((sum_x_squared / count_d) - (mean * mean));

  

  //  pthread_mutex_unlock(&hash_mutex);

  pthread_mutex_lock(&mutex);
  activeThreads--;

  /*  avg_mean += mean;
      avg_var  += variance;

  if(mean > max_mean ) { max_mean = mean; }
  if(variance > max_var) { max_var = variance; }  */

  pthread_mutex_unlock(&mutex);


  return NULL;
}


int main(int argc, char** argv) {

  assert(argc == 4);

  int thread_count = atoi(argv[1]);
  count = atoi(argv[2]);
  alwaysCommit = atoi(argv[3]);
  

  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt");

  int l;

  for(l = 0; l < COUNTER_RESOLUTION; l++) {
    buckets[l] = 0;
  }

  pthread_t * workers = malloc(sizeof(pthread_t) * thread_count);

  Tinit();
  int xid = Tbegin();
  hash = ThashCreate(xid, sizeof(int), sizeof(int));
  
  Tcommit(xid);

  int k;

  /* threads have static thread sizes.  Ughh. */
  pthread_attr_t attr;
  pthread_attr_init(&attr);

  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&never, NULL);

  pthread_attr_setstacksize (&attr, PTHREAD_STACK_MIN);
  //  pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
  pthread_mutex_lock(&mutex);


  for(k = 0; k < thread_count; k++) {
    int * k_copy = malloc(sizeof(int));
    *k_copy = k ;
    pthread_create(&workers[k], &attr, go, k_copy);

  }

  pthread_mutex_unlock(&mutex);

  for(k = 0; k < thread_count; k++) {
    pthread_join(workers[k],NULL);
  }

  double log_multiplier = (COUNTER_RESOLUTION / log(MAX_SECONDS * 1000000000.0));



  for(k = 0; k < COUNTER_RESOLUTION; k++) {
    printf("%3.4f\t%d\n", exp(((double)k)/log_multiplier)/1000000000.0, buckets[k]);
  }

  /*  printf("mean:     (max, avg)  %f, %f\n", max_mean, avg_mean / (double)thread_count);

  printf("variance: (max, avg)  %f, %f\n", max_var, avg_var / (double)thread_count); */

  Tdeinit();
}
