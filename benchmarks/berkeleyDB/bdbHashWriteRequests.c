#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <pthread.h>

// if we're using linux's crazy version of the pthread header, 
// it probably forgot to include PTHREAD_STACK_MIN 

#ifndef PTHREAD_STACK_MIN
#include <limits.h>
#endif

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <db.h>

#include "genericBerkeleyDBCode.c"

#define MAX_SECONDS 100
#define COUNTER_RESOLUTION 240

int buckets[COUNTER_RESOLUTION];

int activeThreads = 0;
int max_active = 0;

pthread_cond_t never;
pthread_mutex_t mutex;

void addTimespec(struct timespec * ts, long nsec) {
  ts->tv_nsec += nsec;
  
  //               0123456789
  if(ts->tv_nsec > 1000000000) {
    ts->tv_nsec -= 1000000000;
    ts->tv_sec ++;
  }
}


double thread_requests_per_sec = 10.0;
int alwaysCommit;

int num_xact;
int insert_per_xact;
void * runThread(void * arg);
int
main(int argc, char *argv[])
{
	extern int optind;

	int ch, ret;

	assert(argc == 3 || argc == 4);

	alwaysCommit = (argc == 4);

	/* threads have static thread sizes.  Ughh. */
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setstacksize (&attr, 4 * PTHREAD_STACK_MIN);

	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&never, NULL);

	pthread_mutex_lock(&mutex);

	initDB(&attr, DB_HASH);
	
	int l;
	
	for(l = 0; l < COUNTER_RESOLUTION; l++) {
	  buckets[l] = 0;
	}

	int r;
	int num_threads         = atoi(argv[1]);
	thread_requests_per_sec = (double) atoi(argv[2]);
	
	printf("%d %f\n", num_threads, thread_requests_per_sec);

	if(alwaysCommit) {
	  num_xact        = 10.0 * thread_requests_per_sec;//atoi(argv[2]);
	  insert_per_xact = 1;
	} else {
	  abort();
	  num_xact        = 1;
	  insert_per_xact = atoi(argv[2]);
	}





	pthread_t * threads = malloc(num_threads * sizeof(pthread_t));
	int i ;
	for(i = 0; i < num_threads; i++) {
	  if ((ret = pthread_create(&(threads[i]), &attr, runThread, (void *)i)) != 0){
	    fprintf(stderr,
		    "txnapp: failed spawning worker thread: %s\n",
		    strerror(ret));
	    exit (1);
	  }
	}     
	
	pthread_mutex_unlock(&mutex);

	for(i = 0; i < num_threads; i++) {
	  pthread_join(threads[i], NULL);
	}

	free(threads);

	int k;
	double log_multiplier = (COUNTER_RESOLUTION / log(MAX_SECONDS * 1000000000.0));
	int total = 0;
	for(k = 0; k < COUNTER_RESOLUTION; k++) {
	  printf("%3.4f\t%d\n", exp(((double)k)/log_multiplier)/1000000000.0, buckets[k]);
	  total += buckets[k];
	}
	printf("Total requests: %d\n", total);

	db->close(db, 0);
	dbenv->close(dbenv, 0);

	return (0);
}


void * runThread(void * arg) {
  int offset = (int) arg;
  
  pthread_mutex_lock(&mutex);
  activeThreads++;
  if(activeThreads > max_active) {
    max_active = activeThreads;
  }
  pthread_mutex_unlock(&mutex);

  int r;

  //  double sum_x_squared = 0;
  //  double sum = 0;

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
  

  for(r = 0; r < num_xact; r ++) {

    struct timeval endtime_tv;
    struct timespec endtime;

    run_xact(dbenv, db_cats, offset*(1+r)*insert_per_xact, insert_per_xact);

    gettimeofday(&endtime_tv, NULL);

    endtime.tv_sec = endtime_tv.tv_sec;
    endtime.tv_nsec = 1000 * endtime_tv.tv_usec;

    double microSecondsPassed = 1000000000.0 * (double)(endtime.tv_sec - timeout.tv_sec);

    microSecondsPassed = (microSecondsPassed + (double)endtime.tv_nsec) - (double)timeout.tv_nsec;

    assert(microSecondsPassed > 0.0);

    //    sum += microSecondsPassed;
    //    sum_x_squared += (microSecondsPassed * microSecondsPassed) ;

    int bucket = (log_multiplier * log(microSecondsPassed));
    
    if(bucket >= COUNTER_RESOLUTION) { bucket = COUNTER_RESOLUTION - 1; }
    
    addTimespec(&timeout, 1000000000.0 / thread_requests_per_sec);
    pthread_mutex_lock(&mutex);
    //    timeout.tv_sec++;
    buckets[bucket]++;
    pthread_cond_timedwait(&never, &mutex, &timeout);
    pthread_mutex_unlock(&mutex);

  }

  pthread_mutex_lock(&mutex);
  activeThreads--;
  pthread_mutex_unlock(&mutex);


  //  printf("%d done\n", offset);
}

