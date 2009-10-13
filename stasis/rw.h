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
 *                Found the code at this url:
 *                http://www.cs.nmsu.edu/~jcook/Tools/pthreads/rw.c
 */
#include <pthread.h>
#include <stasis/common.h>
#include <stdio.h>

#ifndef __LIBDFA_RW_H
#define __LIBDFA_RW_H

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

#endif /* rw.h */
