#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <lladd/transactional.h>
#include <unistd.h>
#include <pthread.h>

#include <math.h>
#include <sys/time.h>
#include <time.h>



// if we're using linux's crazy version of the pthread header, 
// it probably forgot to include PTHREAD_STACK_MIN 

#ifndef PTHREAD_STACK_MIN
#include <limits.h>
#endif

int activeThreads = 0;
int max_active = 0;
int alwaysCommit;

int commitCount = 0;
int putCount = 0;

/*
double avg_var = 0;
double max_var = 0;
double avg_mean = 0;
double max_mean = 0;*/

pthread_mutex_t mutex;

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
  int xid = Tbegin();

  //  double sum_x_squared = 0;
  //  double sum = 0;

  for(j = k * count; j < (k+1) *(count) ; j++) {

    //    struct timeval start, endtime;

    //    gettimeofday(&start, NULL);

    ThashInsert(xid, hash, (byte*)&j, sizeof(int), (byte*)&j, sizeof(int));
    putCount++;
    /*    gettimeofday(&endtime, NULL);

    double microSecondsPassed = 1000000 * (endtime.tv_sec - start.tv_sec);

    microSecondsPassed = (microSecondsPassed + endtime.tv_usec) - start.tv_usec;

    sum += microSecondsPassed;
    sum_x_squared += (microSecondsPassed * microSecondsPassed) ;

    */
    //    printf("(%d)", k);

    if(alwaysCommit) {
      //      printf("Commit");
      commitCount++;
      Tcommit(xid);
      xid = Tbegin();
      
    }
  }

  Tcommit(xid);
  
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

  assert(argc == 3 || argc == 4);

  int thread_count = atoi(argv[1]);
  count = atoi(argv[2]);

  alwaysCommit = (argc==4);

  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt");

  pthread_t * workers = malloc(sizeof(pthread_t) * thread_count);

  Tinit();
  int xid = Tbegin();
  //  hash = ThashCreate(xid, sizeof(int), sizeof(int));
  hash = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  
  Tcommit(xid);

  int k;

  /* threads have static thread sizes.  Ughh. */
  pthread_attr_t attr;
  pthread_attr_init(&attr);

  pthread_mutex_init(&mutex, NULL);

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

  Tdeinit();

  printf("Committed %d times, put %d times.\n", commitCount, putCount);
}
