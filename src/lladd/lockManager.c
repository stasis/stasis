#include <pbl/pbl.h>
#include <lladd/lockManager.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <malloc.h>
#include <errno.h>
#include <stdio.h>
#include <assert.h>

#include <lladd/hash.h>


#define MUTEX_COUNT 32
// These next two correspond to MUTEX count, and are the appropriate values to pass into hash().
#define MUTEX_BITS  5
#define MUTEX_EXT   32 

static pthread_mutex_t mutexes[MUTEX_COUNT];

static pthread_mutex_t xid_table_mutex = PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t * getMutex(recordid rid) {
  return &mutexes[hash(&rid, sizeof(recordid), MUTEX_BITS, MUTEX_EXT)];
}

static pblHashTable_t * xidLockTable;
static pblHashTable_t * ridLockTable;

typedef struct {
  pthread_cond_t writeOK;
  pthread_cond_t readOK;
  int readers;
  int writers;
  int waiting;
  int active;
} lock;

void lockManagerInit() {
  int i = 0;
  for(i = 0; i < MUTEX_COUNT; i++) {
    pthread_mutex_init(&mutexes[i], NULL);
  }
  xidLockTable = pblHtCreate();
  ridLockTable = pblHtCreate();
  
}
/** @todo startTransaction needs a mutex!! */
void startTransaction(int xid) {
  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtCreate();
  pblHtInsert(xidLockTable, &xid, sizeof(int), xidLocks);
  pthread_mutex_unlock(&xid_table_mutex);
}

lock* createLock(recordid rid) {
  lock * ret = malloc(sizeof(lock));

  if(!ret) { return NULL; }

  //  pthread_mutex_init(&ret->mut, NULL);
  pthread_cond_init(&ret->writeOK, NULL);
  pthread_cond_init(&ret->readOK, NULL);
  ret->readers = 0;
  ret->writers = 0;
  ret->waiting = 0;

  pblHtInsert(ridLockTable, &rid, sizeof(recordid), ret);
  return ret;
}

void destroyLock(recordid rid, lock * l) {
  pthread_cond_destroy(&l->writeOK);
  pthread_cond_destroy(&l->readOK);
  free (l);
  pblHtRemove(ridLockTable, &rid, sizeof(recordid));
}

#define LM_READLOCK 1
#define LM_WRITELOCK 2

int lockManagerReadLockRecord(int xid, recordid rid) {

  pthread_mutex_lock(&xid_table_mutex);
  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));
  if((int)pblHtLookup(xidLocks, &rid, sizeof(recordid)) >= LM_READLOCK) {
    pthread_mutex_unlock(&xid_table_mutex);
    return 0;
  }
  pthread_mutex_unlock(&xid_table_mutex);
  pthread_mutex_t * mut = getMutex(rid);

  pthread_mutex_lock(mut);

  lock * ridLock = pblHtLookup(ridLockTable, &rid, sizeof(recordid));

  if(!ridLock) {
    ridLock = createLock(rid);
  }

  ridLock->active++;

  if(ridLock->writers || ridLock->waiting) {
    struct timeval tv;
    int tod_ret = gettimeofday (&tv, NULL);
    tv.tv_sec++; // Wait up to one second to obtain a lock before detecting deadlock.
    struct timespec ts; 
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
    if(tod_ret != 0) {
      perror("Could not get time of day");
      return LLADD_INTERNAL_ERROR;
    }
    do {
      int wait_ret = pthread_cond_timedwait(&ridLock->readOK, mut, &ts);
      if(wait_ret == ETIMEDOUT) {
	ridLock->active--;
	pthread_mutex_unlock(mut);
	return LLADD_DEADLOCK;
      }
    } while(ridLock->writers);
  } 
  ridLock->readers++;
  ridLock->active--;
  pthread_mutex_unlock(mut);
  pblHtInsert(xidLocks, &rid, sizeof(recordid), (void*)LM_READLOCK);
  return 0;
}
int lockManagerWriteLockRecord(int xid, recordid rid) {
  pthread_mutex_lock(&xid_table_mutex);
  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));


  int currentLockLevel = (int)pblHtLookup(xidLocks, &rid, sizeof(recordid));
  int me = 0;
  pthread_mutex_unlock(&xid_table_mutex);

  if(currentLockLevel >= LM_WRITELOCK) {
    return 0;
  } else if(currentLockLevel == LM_READLOCK) {
    me = 1;
  }

    pthread_mutex_t * mut = getMutex(rid);

  pthread_mutex_lock(mut);
  lock * ridLock = pblHtLookup(ridLockTable, &rid, sizeof(recordid));
  if(!ridLock) {
    ridLock = createLock(rid);
  }

  ridLock->active++;
  ridLock->waiting++;
  if(ridLock->writers || (ridLock->readers - me)) {
    struct timeval tv;
    int tod_ret = gettimeofday(&tv, NULL);
    tv.tv_sec++;
    struct timespec ts; 
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
    if(tod_ret != 0) {
      perror("Could not get time of day");
      return LLADD_INTERNAL_ERROR;
    }
    while(ridLock->writers || (ridLock->readers - me)) { 
      int lockret = pthread_cond_timedwait(&ridLock->writeOK, mut, &ts);
      if(lockret == ETIMEDOUT) {
	ridLock->waiting--;
	ridLock->active--;
	pthread_mutex_unlock(mut);
	return LLADD_DEADLOCK;
      }
    }
  }
  ridLock->waiting--;
  if(currentLockLevel == 0) {
    ridLock->readers++;
  }
  ridLock->writers++;
  ridLock->active--;
  pthread_mutex_unlock(mut);
  pblHtInsert(xidLocks, &rid, sizeof(recordid), (void*)LM_WRITELOCK);
  return 0;
}

int lockManagerUnlockRecord(int xid, recordid rid) {
  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));


  int currentLevel = (int)pblHtLookup(xidLocks, &rid, sizeof(recordid));

  if(currentLevel) {
    pblHtRemove(xidLocks, &rid, sizeof(recordid));
  }

  pthread_mutex_unlock(&xid_table_mutex);
  pthread_mutex_t * mut = getMutex(rid);
  pthread_mutex_lock(mut);
  lock * ridLock = pblHtLookup(ridLockTable, &rid, sizeof(recordid));
  assert(ridLock);
  ridLock->active++;

  if(currentLevel == LM_WRITELOCK) {
    ridLock->writers--;
    ridLock->readers--;
  } else if(currentLevel == LM_READLOCK) {
    ridLock->readers--;
  } else if(currentLevel == 0) {
    assert(0); // Someone tried to release a lock they didn't own!
  } else {
    fprintf(stderr, "Unknown lock type encountered!");
    ridLock->active--;
    pthread_mutex_unlock(mut);
    return LLADD_INTERNAL_ERROR;
  }

  ridLock->active--;

  if(!(ridLock->active || ridLock->waiting || ridLock->readers || ridLock->writers)) {
    destroyLock(rid, ridLock);
  }

  pthread_mutex_unlock(mut);

  return 0;
}

int lockManagerReleaseAll(int xid) {

  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));

  pthread_mutex_unlock(&xid_table_mutex);
  void * data;
  int ret = 0;
  for(data = pblHtFirst(xidLocks); data; data = pblHtNext(xidLocks)) {
    recordid rid = *(recordid*)pblHtCurrentKey(xidLocks);
    int tmpret = lockManagerUnlockRecord(xid, rid);
    // Pass any error(s) up to the user.
    // (This logic relies on the fact that currently it only returns 0 and LLADD_INTERNAL_ERROR)
    if(tmpret) {  
      ret = tmpret;
    }
    pblHtRemove(xidLocks, &rid, sizeof(recordid));
  }
  return ret;
}
