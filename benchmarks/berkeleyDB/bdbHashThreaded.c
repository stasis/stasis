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

#define	ENV_DIRECTORY	"TXNAPP"

//#define DEBUG_BDB 1

#include "genericBerkeleyDBCode.c"


int activeThreads = 0;
int max_active = 0;

pthread_mutex_t mutex;

//int alwaysCommit;
int num_xact;
int insert_per_xact;
void * runThread(void * arg);
int
main(int argc, char *argv[])
{
	extern int optind;

	int ch, ret;

	//	assert(argc == 3 || argc == 4);
	assert(argc == 5);
	
	int alwaysCommit = atoi(argv[3]); // 1; // (argc >= 4);

	int type = atoi(argv[4]) == 1 ? DB_HASH : DB_RECNO;

	printf("type: %d always commit: %d\n", type, alwaysCommit);

	/* threads have static thread sizes.  Ughh. */
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setstacksize (&attr, 4 * PTHREAD_STACK_MIN);

	pthread_mutex_init(&mutex, NULL);
	

	pthread_mutex_lock(&mutex);

	initDB(&attr, type);







	int r;
	int num_threads = atoi(argv[1]);

	if(alwaysCommit) {
	  num_xact        = atoi(argv[2]);
	  insert_per_xact = 1;
	} else { 
	  num_xact        = 1;
	  insert_per_xact = atoi(argv[2]);
	}
	// @todo the test has been crashing for multi-threaded long transactions.
	assert(num_threads == 1 || alwaysCommit == 1);  
	
#ifdef DEBUG_BDB 
	printf("num_xact = %d\n insert_per_xact=%d\n", num_xact, insert_per_xact);
#endif

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

	db->close(db, 0);
	dbenv->close(dbenv, 0);

	printf("committed %d times, put %d times\n", commitCount, putCount);

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

  for(r = 0; r < num_xact; r ++) {
    //    run_xact(dbenv, db_cats, offset*(1+r)*insert_per_xact, insert_per_xact);
    run_xact(dbenv, db_cats, (offset * num_xact * insert_per_xact) + (r * insert_per_xact), insert_per_xact);
  }

  pthread_mutex_lock(&mutex);
  activeThreads--;
  pthread_mutex_unlock(&mutex);

}

