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
#include <stasis/common.h>

#ifndef __LIBDFA_RW_H
#define __LIBDFA_RW_H

BEGIN_C_DECLS

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

END_C_DECLS

#endif /* rw.h */
