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

#include "genericBerkeleyDBCode.c"

int activeThreads = 0;
int max_active = 0;

pthread_mutex_t mutex;
/*

void  add_cat(DB_ENV *, DB *, char *, ...);
void  run_xact(DB_ENV *, DB *, int, int);
//void  add_color(DB_ENV *, DB *, char *, int);
//  void  add_fruit(DB_ENV *, DB *, char *, char *); 
void *checkpoint_thread(void *);
void  log_archlist(DB_ENV *);
void *logfile_thread(void *);
void  db_open(DB_ENV *, DB **, char *, int);
void  env_dir_create(void);
void  env_open(DB_ENV **);
void  usage(void);



DB_ENV *dbenv;
DB *db_cats; //, *db_color, *db_fruit; 
*/

int alwaysCommit;
int num_xact;
int insert_per_xact;
void * runThread(void * arg);
int
main(int argc, char *argv[])
{
	extern int optind;
	pthread_t ptid;
	int ch, ret;

	assert(argc == 3 || argc == 4);

	/* threads have static thread sizes.  Ughh. */

	alwaysCommit = (argc == 4);

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_mutex_init(&mutex, NULL);
	pthread_attr_setstacksize (&attr, 4 * PTHREAD_STACK_MIN);
	
	pthread_mutex_lock(&mutex);

	initDB(&attr);

	int r;
	int num_threads = atoi(argv[1]);

	if(alwaysCommit) {
	  num_xact        = atoi(argv[2]);
	  insert_per_xact = 1;
	} else { 
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
	  /*
	    for(r = 0; r < num_xact; r ++) {
	      run_xact(dbenv, db_cats, 1+r*insert_per_xact, insert_per_xact);
	    }
	  */
	}     
	
	pthread_mutex_unlock(&mutex);

	for(i = 0; i < num_threads; i++) {
	  pthread_join(threads[i], NULL);
	}

	free(threads);

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
    run_xact(dbenv, db_cats, offset*(1+r)*insert_per_xact, insert_per_xact);
  }

  pthread_mutex_lock(&mutex);
  activeThreads--;
  pthread_mutex_unlock(&mutex);

}

