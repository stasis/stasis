/*
 *	File	: rw.c
 *
 *	Title	: Demo Readers/Writer.
 *
 *	Short	: A solution to the multi-reader's, one writer problem.
 *
 *	Long	:
 *
 *	Author	: Andrae Muys
 *
 *	Date	: 18 September 1997
 *
 *	Revised	: 4-7-04  Shamelessly stolen and adapted by Rusty Sears.
 *                Found the contents of rw.c at this url:
 *                http://www.cs.nmsu.edu/~jcook/Tools/pthreads/rw.c
 *
 *  Revised : (date?): Converted to thin wrapper over pthread_rwlock.
 *  Revised : 5-26 Added rwlc locks, which can wait for condition variables (assuming they hold a write lock)
 */
#ifndef __LIBDFA_RW_H
#define __LIBDFA_RW_H

#include <stasis/common.h>
#include <pthread.h>
#include <stdio.h>

BEGIN_C_DECLS


#define HAVE_PTHREAD_RWLOCK
#ifdef HAVE_PTHREAD_RWLOCK
typedef pthread_rwlock_t rwl;

static inline rwl* initlock(void) {
  rwl* ret = (rwl*)malloc(sizeof(*ret));
  int err = pthread_rwlock_init(ret, 0);
  if(err) { perror("couldn't init rwlock"); abort(); }
  return ret;
}
static inline void readlock(rwl *lock, int d) {
  pthread_rwlock_rdlock(lock);
}
static inline int tryreadlock(rwl *lock, int d) {
  return 0 == pthread_rwlock_tryrdlock(lock);
}
static inline void writelock(rwl *lock, int d) {
  pthread_rwlock_wrlock(lock);
}
static inline int trywritelock(rwl *lock, int d) {
  return 0 == pthread_rwlock_trywrlock(lock);
}
static inline void assertlocked(rwl * lock) { }
static inline void assertunlocked(rwl * lock) { }
static inline void unlock(rwl *lock) {
  pthread_rwlock_unlock(lock);
}
static inline void readunlock(rwl *lock) { unlock(lock); }
static inline void writeunlock(rwl *lock) { unlock(lock); }
static inline void deletelock(rwl *lock) {
  pthread_rwlock_destroy(lock);
  free(lock);
}
#else
typedef struct {
	pthread_mutex_t *mut;
	int writers;
	int readers;
	int waiting;
	pthread_cond_t *writeOK, *readOK;
} rwl;

rwl *initlock (void);
void readlock (rwl *lock, int d);
int tryreadlock(rwl *lock, int d);
void writelock (rwl *lock, int d);
int trywritelock(rwl *lock, int d);
/** aborts if called when no thread holds this latch. */
void assertlocked(rwl *lock);
/** aborts if called when a thread holds this latch. */
void assertunlocked(rwl *lock);
void downgradelock(rwl * lock);
void unlock(rwl * lock);
/** @deprecated in favor of unlock() */
void readunlock (rwl *lock);
/** @deprecated in favor of unlock() */
void writeunlock (rwl *lock);
void deletelock (rwl *lock);
/*
typedef struct {
	rwl *lock;
	int id;
	long delay;
} rwargs;

rwargs *newRWargs (rwl *l, int i, long d);
void *reader (void *args);
void *writer (void *args);
*/
#endif
END_C_DECLS

/** A rwl with support for condition variables. */
typedef struct rwlc {
  rwl * rw;
  pthread_mutex_t mut;
  int is_writelocked;
} rwlc;

static inline rwlc* rwlc_initlock(void) {
  rwlc* ret = (rwlc*)malloc(sizeof(*ret));
  ret->rw = initlock();
  int err = pthread_mutex_init(&ret->mut, 0);
  ret->is_writelocked = 0;
  if(err) { perror("couldn't init rwlclock's mutex"); abort(); }
  return ret;
}
static inline void rwlc_readlock(rwlc *lock) {
  readlock(lock->rw, 0);
}
static inline int rwlc_tryreadlock(rwlc *lock) {
  return tryreadlock(lock->rw, 0);
}
static inline void rwlc_writelock(rwlc *lock) {
  pthread_mutex_lock(&lock->mut); // need to get this here, since the lock order is dictated by pthread_cond_wait's API.
  writelock(lock->rw, 0);
  lock->is_writelocked = 1;
}
static inline int rwlc_trywritelock(rwlc *lock) {
  int ret = pthread_mutex_trylock(&lock->mut);
  if(ret == EBUSY) { return 0; }
  ret = trywritelock(lock->rw, 0); // will fail if someone is holding a readlock.
  if(!ret) {
    pthread_mutex_unlock(&lock->mut);
  } else {
    lock->is_writelocked = 1;
  }
  return ret;
}
static inline void rwlc_assertlocked(rwlc * lock) {
  assertlocked(lock->rw);
}
static inline void rwlc_assertunlocked(rwlc * lock) {
  assertunlocked(lock->rw);
}
static inline void rwlc_readunlock(rwlc *lock) { readunlock(lock->rw); }
static inline void rwlc_cond_wait(pthread_cond_t * cond, rwlc *lock) {
  if(!lock->is_writelocked) { abort(); }
  lock->is_writelocked = 0;
  writeunlock(lock->rw);
  pthread_cond_wait(cond, &lock->mut);
  // already have mutex; reacquire the writelock.
  writelock(lock->rw, 0);
  lock->is_writelocked = 1;
}
static inline int rwlc_cond_timedwait(pthread_cond_t * cond, rwlc *lock, struct timespec * ts) {
  if(!lock->is_writelocked) { abort(); }
  lock->is_writelocked = 0;
  writeunlock(lock->rw);
  int ret = pthread_cond_timedwait(cond, &lock->mut, ts);
  // already have mutex; reacquire the writelock.
  writelock(lock->rw, 0);
  lock->is_writelocked = 1;
  return ret;
}
static inline void rwlc_writeunlock(rwlc *lock) {
  lock->is_writelocked = 0;
  writeunlock(lock->rw);
  pthread_mutex_unlock(&lock->mut);
}
static inline void rwlc_unlock(rwlc *lock) {
  if(lock->is_writelocked) {
    rwlc_writeunlock(lock);
  } else {
    rwlc_readunlock(lock);
  }
}
static inline void rwlc_deletelock(rwlc *lock) {
  deletelock(lock->rw);
  pthread_mutex_destroy(&lock->mut);
  free(lock);
}
#endif /* rw.h */
