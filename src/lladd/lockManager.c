#include <pbl/pbl.h>
#include <lladd/lockManager.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <malloc.h>
#include <errno.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <lladd/hash.h>


#define MUTEX_COUNT 32
// These next two correspond to MUTEX count, and are the appropriate values to pass into hash().
#define MUTEX_BITS  5
#define MUTEX_EXT   32 

static pthread_mutex_t mutexes[MUTEX_COUNT];

static pthread_mutex_t xid_table_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t rid_table_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t * getMutex(byte * dat, int datLen) {
  return &mutexes[hash(dat, datLen, MUTEX_BITS, MUTEX_EXT)];
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

void lockManagerInitHashed() {
  int i = 0;
  for(i = 0; i < MUTEX_COUNT; i++) {
    pthread_mutex_init(&mutexes[i], NULL);
  }
  xidLockTable = pblHtCreate();
  ridLockTable = pblHtCreate();
  
}
pblHashTable_t * lockManagerBeginTransactionUnlocked(int xid) {
  pblHashTable_t * xidLocks = pblHtCreate();
  pblHtInsert(xidLockTable, &xid, sizeof(int), xidLocks);
  return xidLocks;
}
int lockManagerBeginTransaction(int xid) {
  pthread_mutex_lock(&xid_table_mutex);
  lockManagerBeginTransactionUnlocked(xid);
  pthread_mutex_unlock(&xid_table_mutex);
  return 0;
}

lock* createLock(byte * dat, int datLen) {
  lock * ret = malloc(sizeof(lock));

  if(!ret) { return NULL; }

  //  pthread_mutex_init(&ret->mut, NULL);
  pthread_cond_init(&ret->writeOK, NULL);
  pthread_cond_init(&ret->readOK, NULL);
  ret->active  = 0;
  ret->readers = 0;
  ret->writers = 0;
  ret->waiting = 0;
  
  pblHtInsert(ridLockTable, dat, datLen, ret);
  return ret;
}

void destroyLock(byte * dat, int datLen, lock * l) {
  pthread_cond_destroy(&l->writeOK);
  pthread_cond_destroy(&l->readOK);
  free (l);
  pblHtRemove(ridLockTable, dat, datLen);
}

#define LM_READLOCK 1
#define LM_WRITELOCK 2

int lockManagerReadLockHashed(int xid, byte * dat, int datLen) {
  if(xid == -1) { return 0; }
  pthread_mutex_lock(&xid_table_mutex);
  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));
  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }
  int currentLockLevel = (int)pblHtLookup(xidLocks, dat, datLen);
  //  printf("xid %d read lock (%d)\n", xid, currentLockLevel);
  if(currentLockLevel >= LM_READLOCK) {
    pthread_mutex_unlock(&xid_table_mutex);
    return 0;
  }
  assert(!currentLockLevel);
  pthread_mutex_unlock(&xid_table_mutex);
  pthread_mutex_t * mut = getMutex(dat, datLen);

  pthread_mutex_lock(mut);
  pthread_mutex_lock(&rid_table_mutex);
  lock * ridLock = pblHtLookup(ridLockTable, dat, datLen);

  if(!ridLock) {
    ridLock = createLock(dat, datLen);
  }
  pthread_mutex_unlock(&rid_table_mutex);
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

	//	printf("Deadlock!\n"); fflush(stdout);
	//	abort();
	return LLADD_DEADLOCK;
      }
    } while(ridLock->writers);
  } 
  if(currentLockLevel < LM_READLOCK) {
    ridLock->readers++;
    pblHtRemove(xidLocks, dat, datLen);
    pblHtInsert(xidLocks, dat, datLen, (void*)LM_READLOCK);
  }
  ridLock->active--;
  pthread_mutex_unlock(mut);
  return 0;
}
int lockManagerWriteLockHashed(int xid, byte * dat, int datLen) {

  if(xid == -1) { return 0; }
  pthread_mutex_lock(&xid_table_mutex);
  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));

  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }

  int currentLockLevel = (int)pblHtLookup(xidLocks, dat, datLen);

  //  printf("xid %d write lock (%d)\n", xid, currentLockLevel);

  int me = 0;
  pthread_mutex_unlock(&xid_table_mutex);

  if(currentLockLevel >= LM_WRITELOCK) {
    return 0;
  } else if(currentLockLevel == LM_READLOCK) {
    me = 1;
  }

    pthread_mutex_t * mut = getMutex(dat, datLen);

  pthread_mutex_lock(mut);
  pthread_mutex_lock(&rid_table_mutex);
  lock * ridLock = pblHtLookup(ridLockTable, dat, datLen);
  if(!ridLock) {
    ridLock = createLock(dat, datLen);
  }
  pthread_mutex_unlock(&rid_table_mutex);

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
	//	printf("Deadlock!\n"); fflush(stdout);
	//	abort();
	return LLADD_DEADLOCK;
      }
    }
  }
  ridLock->waiting--;
  if(currentLockLevel == 0) {
    ridLock->readers++;
    ridLock->writers++;
  } else if (currentLockLevel == LM_READLOCK) {
    ridLock->writers++;
    pblHtRemove(xidLocks, dat, datLen);
  }
  if(currentLockLevel != LM_WRITELOCK) {
    pblHtInsert(xidLocks, dat, datLen, (void*)LM_WRITELOCK);
  }

  ridLock->active--;
  pthread_mutex_unlock(mut);
  return 0;
}

int lockManagerUnlockHashed(int xid, byte * dat, int datLen) {


  if(xid == -1) { return 0; }
  //  printf("xid %d unlock\n", xid);

  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));

  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }

  int currentLevel = (int)pblHtLookup(xidLocks, dat, datLen);

  if(currentLevel) {
    pblHtRemove(xidLocks, dat, datLen);
  }

  pthread_mutex_unlock(&xid_table_mutex);
  pthread_mutex_t * mut = getMutex(dat, datLen);
  pthread_mutex_lock(mut);
  pthread_mutex_lock(&rid_table_mutex);
  lock * ridLock = pblHtLookup(ridLockTable, dat, datLen);
  assert(ridLock);
  ridLock->active++;
  pthread_mutex_unlock(&rid_table_mutex);
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
    //   printf("destroyed lock");
    destroyLock(dat, datLen, ridLock);
  } else {
    //    printf("(%d %d %d %d)", ridLock->active, ridLock->waiting, ridLock->readers, ridLock->writers);
  }

  pthread_mutex_unlock(mut);

  return 0;
}

int lockManagerCommitHashed(int xid, int datLen) {
  if(xid == -1) { return 0; }
  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtLookup(xidLockTable, &xid, sizeof(int));

  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }

  pthread_mutex_unlock(&xid_table_mutex);
  void * data;
  int ret = 0;
  byte * dat = malloc(datLen);
  for(data = pblHtFirst(xidLocks); data; data = pblHtNext(xidLocks)) {
    memcpy(dat, pblHtCurrentKey(xidLocks), datLen);
    int tmpret = lockManagerUnlockHashed(xid, dat, datLen);
    // Pass any error(s) up to the user.
    // (This logic relies on the fact that currently it only returns 0 and LLADD_INTERNAL_ERROR)
    if(tmpret) {  
      ret = tmpret;
    }
    pblHtRemove(xidLocks, dat, datLen);
  }
  free(dat);
  return ret;
}

int lockManagerReadLockRecord(int xid, recordid rid) {
  return lockManagerReadLockHashed(xid, (byte*)&rid, sizeof(recordid));
}
int lockManagerWriteLockRecord(int xid, recordid rid) {
  return lockManagerWriteLockHashed(xid, (byte*)&rid, sizeof(recordid));
}
int lockManagerUnlockRecord(int xid, recordid rid) {
  return lockManagerUnlockHashed(xid, (byte*)&rid, sizeof(recordid));
}
int lockManagerCommitRecords(int xid) {
  return lockManagerCommitHashed(xid, sizeof(recordid));
}

int lockManagerReadLockPage(int xid, int p) {
  return lockManagerReadLockHashed(xid, (byte*)&p, sizeof(int));
}
int lockManagerWriteLockPage(int xid, int p) {
  return lockManagerWriteLockHashed(xid, (byte*)&p, sizeof(int));
}
int lockManagerUnlockPage(int xid, int p) {
  return lockManagerUnlockHashed(xid, (byte*)&p, sizeof(int));
}
int lockManagerCommitPages(int xid) {
  return lockManagerCommitHashed(xid, sizeof(int));
}

LockManagerSetup globalLockManager;

void setupLockManagerCallbacksPage() {
  globalLockManager.init = &lockManagerInitHashed;
  globalLockManager.readLockPage    = &lockManagerReadLockPage;
  globalLockManager.writeLockPage   = &lockManagerWriteLockPage;
  globalLockManager.unlockPage      = &lockManagerUnlockPage;
  globalLockManager.readLockRecord  = NULL;
  globalLockManager.writeLockRecord = NULL;
  globalLockManager.unlockRecord    = NULL;
  globalLockManager.commit          = &lockManagerCommitPages;
  globalLockManager.abort           = &lockManagerCommitPages;
  globalLockManager.begin           = &lockManagerBeginTransaction;

  globalLockManager.init();
}

void setupLockManagerCallbacksRecord () {
  globalLockManager.init = &lockManagerInitHashed;
  globalLockManager.readLockPage    = NULL;
  globalLockManager.writeLockPage   = NULL;
  globalLockManager.unlockPage      = NULL;
  globalLockManager.readLockRecord  = &lockManagerReadLockRecord;
  globalLockManager.writeLockRecord = &lockManagerWriteLockRecord;
  globalLockManager.unlockRecord    = &lockManagerUnlockRecord;
  globalLockManager.commit          = &lockManagerCommitRecords;
  globalLockManager.abort           = &lockManagerCommitRecords;
  globalLockManager.begin           = &lockManagerBeginTransaction;
  globalLockManager.init();
}


void setupLockManagerCallbacksNil () {
  globalLockManager.init            = NULL;
  globalLockManager.readLockPage    = NULL;
  globalLockManager.writeLockPage   = NULL;
  globalLockManager.unlockPage      = NULL;
  globalLockManager.readLockRecord  = NULL;
  globalLockManager.writeLockRecord = NULL;
  globalLockManager.unlockRecord    = NULL;
  globalLockManager.commit          = NULL;
  globalLockManager.abort           = NULL;
  globalLockManager.begin           = NULL;
}

