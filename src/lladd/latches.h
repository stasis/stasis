#include <config.h>
#include <lladd/common.h>

/** @todo threading should be moved into its own header file. */
#include <pthread.h>

/*#include <pbl/pbl.h> -- Don't want everything that touches threading to include pbl... */
#include <lladd/stats.h>

#ifndef __LATCHES_H
#define __LATCHES_H

/**
   A data structure for profiling latching behavior.
   All time values recorded in this struct are in microseconds.
 */
typedef struct {
  const char * file;
  int line;
  const char * name;
  pthread_mutex_t mutex;
  profile_tuple tup;
  char * last_acquired_at;
  /*  pblHashTable_t * lockpoints; */
  void * lockpoints;
} lladd_pthread_mutex_t;

#include <libdfa/rw.h>

/**
   Keeps some profiling information along with a read/write lock. 
*/

typedef struct {
  const char * file;
  int line;
  rwl * lock;
  profile_tuple tup;
  char * last_acquired_at;
  /* pblHashTable_t * lockpoints; */
  void * lockpoints;
} __profile_rwl;

#ifdef PROFILE_LATCHES

#define pthread_mutex_t lladd_pthread_mutex_t

#define pthread_mutex_init(x, y) __lladd_pthread_mutex_init((x), (y), __FILE__, __LINE__, #x)
#define pthread_mutex_destroy(x) __lladd_pthread_mutex_destroy((x))
#define pthread_mutex_lock(x) __lladd_pthread_mutex_lock((x), __FILE__, __LINE__)
#define pthread_mutex_unlock(x) __lladd_pthread_mutex_unlock((x))
#define pthread_mutex_trylock(x) NO_PROFILING_EQUIVALENT_TO_PTHREAD_TRYLOCK

int __lladd_pthread_mutex_init(lladd_pthread_mutex_t  *mutex,  const  pthread_mutexattr_t *mutexattr, const char * file, int line, const char * mutex_name);
int __lladd_pthread_mutex_lock(lladd_pthread_mutex_t *mutex, char * file, int line);
int __lladd_pthread_mutex_unlock(lladd_pthread_mutex_t *mutex);
int __lladd_pthread_mutex_destroy(lladd_pthread_mutex_t *mutex);

#define initlock() __profile_rw_initlock(__FILE__, __LINE__)
#define readlock(x, y) __profile_readlock((x),(y), __FILE__, __LINE__)
#define writelock(x, y) __profile_writelock((x), (y), __FILE__, __LINE__)
#define readunlock(x) __profile_readunlock((x))
#define writeunlock(x) __profile_writeunlock((x))
#define deletelock(x) __profile_deletelock((x))

#define rwl __profile_rwl

rwl *__profile_rw_initlock (char * file, int line);
void __profile_readlock (rwl *lock, int d, char * file, int line);
void __profile_writelock (rwl *lock, int d, char * file, int line);
void __profile_readunlock (rwl *lock);
void __profile_writeunlock (rwl *lock);
void __profile_deletelock (rwl *lock);


#endif  

#endif /* __LATCHES_H */
