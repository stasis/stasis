#define _GNU_SOURCE

#include <config.h>
#include <stasis/common.h>

#include "latches.h"

#include <pbl/pbl.h>
#include <errno.h>
#include <assert.h>

#undef pthread_mutex_t
#undef pthread_mutex_init
#undef pthread_mutex_destroy
#undef pthread_mutex_lock
#undef pthread_mutex_trylock
#undef pthread_mutex_unlock
#undef pthread_cond_timedwait
#undef pthread_cond_wait

/*

int __lladd_pthread_mutex_init(lladd_pthread_mutex_t  *mutex,  const  pthread_mutexattr_t *mutexattr, 
			       const char * file, int line, const char * name) {
  
  mutex->file = file;
  mutex->line = line;
  mutex->name = name;

  init_tuple(&(mutex->tup));
  
  mutex->lockpoints = pblHtCreate();

  return pthread_mutex_init(&(mutex->mutex), mutexattr);


}

int __lladd_pthread_mutex_lock(lladd_pthread_mutex_t *mutex, char * file, int line) {
  long blockCount = 0;
  int ret;
  char * location;
  int location_length = asprintf(&location, "%s %d", file, line);

  // DEBUG("Acquire mutex:  %s %d\n", file, line);

  while(EBUSY == (ret = pthread_mutex_trylock(&(mutex->mutex)))) {
    blockCount ++;
    pthread_yield();
    
    if(blockCount >= 30000 && ! (blockCount % 30000)) {
      printf("Spinning at %s:%d, %ld times.  Held by: %s\n", file, line, blockCount, mutex->last_acquired_at);
      fflush(NULL);
    }

  }
  

  profile_tuple * tup = pblHtLookup(mutex->lockpoints, location, location_length+1);

  mutex->last_acquired_at = location;

  if(!tup) {
    tup = malloc(sizeof(profile_tuple));
    
    init_tuple(tup);
    
    pblHtInsert(mutex->lockpoints, location, location_length+1, tup);

  }

  acquired_lock(&(mutex->tup), blockCount);
  acquired_lock(tup, blockCount);

  return ret;

}
int __lladd_pthread_mutex_unlock(lladd_pthread_mutex_t *mutex) {

  profile_tuple * tup = pblHtLookup(mutex->lockpoints, mutex->last_acquired_at, strlen(mutex->last_acquired_at)+1);

  released_lock(tup);
  released_lock(&(mutex->tup));

  free(mutex->last_acquired_at);

  return pthread_mutex_unlock(&(mutex->mutex));

}
int __lladd_pthread_mutex_destroy(lladd_pthread_mutex_t *mutex) {

  // Dump profiling info to stdout 

  profile_tuple * tup;

  printf("Free mutex %s Init: %s %d\n   ", mutex->name, mutex->file, mutex->line); 
  print_profile_tuple(&(mutex->tup));
  printf("\n  Lock points: [mean, stddev, max] \n");

  // Now, iterate over all of the lockpoints: 

  for(tup = pblHtFirst(mutex->lockpoints); tup; tup = pblHtNext(mutex->lockpoints)) {
    printf("\t%s ", (char*)pblHtCurrentKey(mutex->lockpoints)); 
    print_profile_tuple(tup);
    printf("\n");
    free(tup);
  }

  pblHtDelete(mutex->lockpoints);


  return pthread_mutex_destroy(&(mutex->mutex));

}

//   @todo The profiled version of pthread_cond_wait isn't really implemented, so it throws off the mutex statistics. 
int __lladd_pthread_cond_wait(pthread_cond_t *cond, lladd_pthread_mutex_t *mutex, 
			      char * file, int line, char * cond_name, char * mutex_name) {
  int ret;
  char * location;
  int location_length; 

  profile_tuple * tup = pblHtLookup(mutex->lockpoints, mutex->last_acquired_at, strlen(mutex->last_acquired_at)+1);

  released_lock(tup);
  released_lock(&(mutex->tup));

  free(mutex->last_acquired_at);

  ret = pthread_cond_wait(cond, &mutex->mutex);

  location_length = asprintf(&location, "%s %d", file, line);

  tup = pblHtLookup(mutex->lockpoints, location, location_length+1);

  mutex->last_acquired_at = location;

  if(!tup) {
    tup = malloc(sizeof(profile_tuple));
    
    init_tuple(tup);
    
    pblHtInsert(mutex->lockpoints, location, location_length+1, tup);

  }

  acquired_lock(&(mutex->tup), 0);
  acquired_lock(tup, 0);

  return ret;
}

int __lladd_pthread_cond_timedwait(pthread_cond_t *cond, lladd_pthread_mutex_t *mutex, void *abstime,
				   char * file, int line, char * cond_name, char * mutex_name) {
  return pthread_cond_timedwait(cond, &mutex->mutex, abstime);
}

*/

#undef rwl
#undef initlock
#undef readlock
#undef writelock
#undef readunlock
#undef writeunlock
#undef deletelock
#undef unlock
#undef downgradelock

__profile_rwl *__profile_rw_initlock (char * file, int line) {
  __profile_rwl * ret = malloc(sizeof(__profile_rwl));

  ret->file = file;
  ret->line = line;

  init_tuple(&(ret->tup));

  ret->lockpoints = pblHtCreate();
  
  ret->lock = initlock();
  ret->holder = 0;
  ret->readCount = 0;
  return ret;

}

/*static pthread_mutex_t __profile_rwl_mutex = PTHREAD_MUTEX_INITIALIZER;*/

void __profile_readlock (__profile_rwl *lock, int d, char * file, int line) {

  char * location;
  //  pthread_t self = pthread_self();
  //  int location_length = asprintf(&location, "readLock() %s:%d pid=%ld", file, line, self);
  int location_length = asprintf(&location, "readLock() %s:%d", file, line);

  profile_tuple * tup;

  /*  DEBUG("Read lock:  %s %d\n", file, line); */


  /** @todo Should we spin instead of using the more efficient rwl
      implementation, or should we see how many times we were woken
      before obtaining the lock? */

#ifdef PROFILE_LATCHES_WRITE_ONLY
  pthread_t self = pthread_self();
  if(lock->holder != self) {
    writelock(lock->lock, d);
    lock->holder = self;
  }
  lock->readCount++;
#else
  readlock(lock->lock, d);
#endif

  /*  pthread_mutex_lock(__profile_rwl_mutex); */

  tup = pblHtLookup(lock->lockpoints, location, location_length+1);

  lock->last_acquired_at = location;

  if(!tup) {
    tup = malloc(sizeof(profile_tuple));

    init_tuple(tup);

    pblHtInsert(lock->lockpoints, location, location_length+1, tup);

  }

  acquired_lock(&(lock->tup), -1);
  acquired_lock(tup, -1);

  /*  pthread_mutex_unlock(__profile_rwl_mutex);*/


}

void __profile_writelock (__profile_rwl *lock, int d, char * file, int line) {

  char * location;
  //  pthread_t self = pthread_self();
  int location_length = asprintf(&location, "writeLock() %s:%d", file, line);

  profile_tuple * tup;

  /*  DEBUG("Write lock:  %s %d\n", file, line); */


  /** @todo Should we spin instead of using the more efficient rwl
      implementation, or should we see how many times we were woken
      before obtaining the lock? */
  writelock(lock->lock, d);

  /*  pthread_mutex_lock(__profile_rwl_mutex); */

  tup = pblHtLookup(lock->lockpoints, location, location_length+1);

  lock->last_acquired_at = location;

  if(!tup) {
    tup = malloc(sizeof(profile_tuple));

    init_tuple(tup);

    pblHtInsert(lock->lockpoints, location, location_length+1, tup);

  }

  acquired_lock(&(lock->tup), -1);
  acquired_lock(tup, -1);

  /*  pthread_mutex_unlock(__profile_rwl_mutex);*/



}
void __profile_readunlock (__profile_rwl *lock) {

  profile_tuple * tup = pblHtLookup(lock->lockpoints, lock->last_acquired_at, strlen(lock->last_acquired_at)+1);

  released_lock(tup);
  released_lock(&(lock->tup));

#ifdef PROFILE_LATCHES_WRITE_ONLY
  pthread_t self = pthread_self();
  assert(lock->holder == self);
  lock->readCount--;
  if(!lock->readCount) { 
    lock->holder = 0;
    free(lock->last_acquired_at);  // last_acquired_at gets leaked by readunlock.
    writeunlock(lock->lock);
  }
#else
  readunlock(lock->lock);
#endif
}
void __profile_writeunlock (__profile_rwl *lock) {

  profile_tuple * tup = pblHtLookup(lock->lockpoints, lock->last_acquired_at, strlen(lock->last_acquired_at)+1);

  released_lock(tup);
  released_lock(&(lock->tup));

  free(lock->last_acquired_at);

  writeunlock(lock->lock);

}

void __profile_unlock (__profile_rwl * lock) {
#ifdef PROFILE_LATCHES_WRITE_ONLY
  if(!lock->readCount) { 
#else
  if(lock->lock->writers) {
#endif
    __profile_writeunlock(lock);
  } else {
    __profile_readunlock(lock);
  }
}

void __profile_downgradelock (__profile_rwl * lock) {
  profile_tuple * tup = pblHtLookup(lock->lockpoints, lock->last_acquired_at, strlen(lock->last_acquired_at)+1);

  released_lock(tup);
  released_lock(&(lock->tup));

  free(lock->last_acquired_at);

  downgradelock(lock->lock);
}

void __profile_deletelock (__profile_rwl *lock) {

  profile_tuple * tup;

#ifdef PROFILE_LATCHES_VERBOSE
  printf("Free rwl init: %s %d\t   ", lock->file, lock->line);
  print_profile_tuple(&(lock->tup));
  printf("\n");
  printf("Lock points: [mean, stddev, max] \n");  
  
  for(tup = pblHtFirst(lock->lockpoints); tup; tup = pblHtNext(lock->lockpoints)) {
    printf("\t%s ", (char*)pblHtCurrentKey(lock->lockpoints)); 
    print_profile_tuple(tup);
    printf("\n");
    free(tup);
  } 
#else
  for(tup = pblHtFirst(lock->lockpoints); tup; tup = pblHtNext(lock->lockpoints)) {
    free(tup);
  } 
#endif
  pblHtDelete(lock->lockpoints);

  deletelock(lock->lock);
}
