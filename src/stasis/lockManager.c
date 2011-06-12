#include <pbl/pbl.h>
#include <stasis/lockManager.h>
#include <stasis/latches.h>
#include <stasis/hash.h>

#include <sys/time.h>
#include <time.h>

#include <assert.h>
#include <pthread.h>

static pthread_mutex_t stasis_lock_manager_ht_mut = PTHREAD_MUTEX_INITIALIZER;

static int pblHtInsert_r(pblHashTable_t * h, void * key, size_t keylen, void * val) {
  pthread_mutex_lock(&stasis_lock_manager_ht_mut);
  int ret = pblHtInsert(h, key, keylen, val);
  pthread_mutex_unlock(&stasis_lock_manager_ht_mut);
  return ret;
}
static void * pblHtLookup_r(pblHashTable_t * h, void * key, size_t keylen) {
  pthread_mutex_lock(&stasis_lock_manager_ht_mut);
  void * ret = pblHtLookup(h, key, keylen);
  pthread_mutex_unlock(&stasis_lock_manager_ht_mut);
  return ret;
}
static int pblHtRemove_r(pblHashTable_t * h, void * key, size_t keylen) {
  pthread_mutex_lock(&stasis_lock_manager_ht_mut);
  int ret = pblHtRemove(h, key, keylen);
  pthread_mutex_unlock(&stasis_lock_manager_ht_mut);
  return ret;
}
static void * pblHtFirst_r(pblHashTable_t *h) {
  pthread_mutex_lock(&stasis_lock_manager_ht_mut);
  void * ret = pblHtFirst(h);
  pthread_mutex_unlock(&stasis_lock_manager_ht_mut);
  return ret;
}
static void * pblHtNext_r(pblHashTable_t *h) {
  pthread_mutex_lock(&stasis_lock_manager_ht_mut);
  void * ret = pblHtNext(h);
  pthread_mutex_unlock(&stasis_lock_manager_ht_mut);
  return ret;
}
static void * pblHtCurrentKey_r(pblHashTable_t *h) {
  pthread_mutex_lock(&stasis_lock_manager_ht_mut);
  void * ret = pblHtCurrentKey(h);
  pthread_mutex_unlock(&stasis_lock_manager_ht_mut);
  return ret;
}

#define MUTEX_COUNT 32
// These next two correspond to MUTEX count, and are the appropriate values to pass into hash().
#define MUTEX_BITS  5
#define MUTEX_EXT   32

static pthread_mutex_t mutexes[MUTEX_COUNT];

static pthread_mutex_t xid_table_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t * getMutex(byte * dat, int datLen) {
  return &mutexes[stasis_linear_hash(dat, datLen, MUTEX_BITS, MUTEX_EXT)];
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
  pblHtInsert_r(xidLockTable, &xid, sizeof(int), xidLocks);
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

  pblHtInsert_r(ridLockTable, dat, datLen, ret);
  return ret;
}

void destroyLock(byte * dat, int datLen, lock * l) {
  pthread_cond_destroy(&l->writeOK);
  pthread_cond_destroy(&l->readOK);
  free (l);
  pblHtRemove_r(ridLockTable, dat, datLen);
}

#define LM_READLOCK 1
#define LM_WRITELOCK 2

int lockManagerReadLockHashed(int xid, byte * dat, int datLen) {
  if(xid == -1) { return 0; }
  pthread_mutex_lock(&xid_table_mutex);
  pblHashTable_t * xidLocks = pblHtLookup_r(xidLockTable, &xid, sizeof(int));
  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }
  long currentLockLevel = (long)pblHtLookup_r(xidLocks, dat, datLen);
  //  printf("xid %d read lock (%d)\n", xid, currentLockLevel);
  if(currentLockLevel >= LM_READLOCK) {
    pthread_mutex_unlock(&xid_table_mutex);
    return 0;
  }
  assert(!currentLockLevel);
  pthread_mutex_unlock(&xid_table_mutex);
  pthread_mutex_t * mut = getMutex(dat, datLen);

  pthread_mutex_lock(mut);
  lock * ridLock = pblHtLookup_r(ridLockTable, dat, datLen);

  if(!ridLock) {
    ridLock = createLock(dat, datLen);
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
  if(currentLockLevel < LM_READLOCK) {
    ridLock->readers++;
    pblHtRemove_r(xidLocks, dat, datLen);
    pblHtInsert_r(xidLocks, dat, datLen, (void*)LM_READLOCK);
  }
  ridLock->active--;
  pthread_mutex_unlock(mut);
  return 0;
}
int lockManagerWriteLockHashed(int xid, byte * dat, int datLen) {

  if(xid == -1) { return 0; }
  pthread_mutex_lock(&xid_table_mutex);
  pblHashTable_t * xidLocks = pblHtLookup_r(xidLockTable, &xid, sizeof(int));

  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }

  long currentLockLevel = (long)pblHtLookup_r(xidLocks, dat, datLen);

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
  lock * ridLock = pblHtLookup_r(ridLockTable, dat, datLen);
  if(!ridLock) {
    ridLock = createLock(dat, datLen);
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
    ridLock->writers++;
  } else if (currentLockLevel == LM_READLOCK) {
    ridLock->writers++;
    pblHtRemove_r(xidLocks, dat, datLen);
  }
  if(currentLockLevel != LM_WRITELOCK) {
    pblHtInsert_r(xidLocks, dat, datLen, (void*)LM_WRITELOCK);
  }

  ridLock->active--;
  pthread_mutex_unlock(mut);
  return 0;
}

static int decrementLock(void * dat, int datLen, int currentLevel) {
  //  pthread_mutex_unlock(&xid_table_mutex);
  pthread_mutex_t * mut = getMutex(dat, datLen);
  pthread_mutex_lock(mut);
  lock * ridLock = pblHtLookup_r(ridLockTable, dat, datLen);
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
    //   printf("destroyed lock");
    destroyLock(dat, datLen, ridLock);
  } else {
    //    printf("(%d %d %d %d)", ridLock->active, ridLock->waiting, ridLock->readers, ridLock->writers);
  }
  pthread_mutex_unlock(mut);
  return 0;
}

int lockManagerUnlockHashed(int xid, byte * dat, int datLen) {


  if(xid == -1) { return 0; }
  //  printf("xid %d unlock\n", xid);

  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtLookup_r(xidLockTable, &xid, sizeof(int));

  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }

  pthread_mutex_unlock(&xid_table_mutex);

  long currentLevel = (long)pblHtLookup_r(xidLocks, dat, datLen);

  assert(currentLevel);
  pblHtRemove_r(xidLocks, dat, datLen);
  decrementLock(dat, datLen, currentLevel);

  return 0;
}

int lockManagerCommitHashed(int xid, int datLen) {
  if(xid == -1) { return 0; }
  pthread_mutex_lock(&xid_table_mutex);

  pblHashTable_t * xidLocks = pblHtLookup_r(xidLockTable, &xid, sizeof(int));
  pblHtRemove_r(xidLockTable, &xid, sizeof(int));
  if(!xidLocks) {
    xidLocks = lockManagerBeginTransactionUnlocked(xid);
  }

  pthread_mutex_unlock(&xid_table_mutex);
  long currentLevel;
  int ret = 0;
  for(currentLevel = (long)pblHtFirst_r(xidLocks); currentLevel; currentLevel  = (long)pblHtNext_r(xidLocks)) {
    void * currentKey = pblHtCurrentKey_r(xidLocks);
    int tmpret = decrementLock(currentKey, datLen, currentLevel);
    // Pass any error(s) up to the user.
    // (This logic relies on the fact that currently it only returns 0 and LLADD_INTERNAL_ERROR)
    if(tmpret) {
      ret = tmpret;
    }
  }
  pblHtDelete(xidLocks);
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

int lockManagerReadLockPage(int xid, pageid_t p) {
  return lockManagerReadLockHashed(xid, (byte*)&p, sizeof(p));
}
int lockManagerWriteLockPage(int xid, pageid_t p) {
  return lockManagerWriteLockHashed(xid, (byte*)&p, sizeof(p));
}
int lockManagerUnlockPage(int xid, pageid_t p) {
  return lockManagerUnlockHashed(xid, (byte*)&p, sizeof(p));
}
int lockManagerCommitPages(int xid) {
  return lockManagerCommitHashed(xid, sizeof(pageid_t));
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
