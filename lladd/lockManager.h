#include <lladd/transactional.h>

typedef struct {
  void (*init)();
  int (*readLockPage)   (int xid, int page);
  int (*writeLockPage)  (int xid, int page);
  int (*unlockPage)     (int xid, int page);
  int (*readLockRecord) (int xid, recordid rid);
  int (*writeLockRecord)(int xid, recordid rid);
  int (*unlockRecord)   (int xid, recordid rid);
  int (*begin)          (int xid);
  int (*commit)         (int xid);
  int (*abort)          (int xid);
} LockManagerSetup;

extern LockManagerSetup globalLockManager;

void lockManagerInit();

int lockManagerReadLockRecord(int xid, recordid rid);
int lockManagerWriteLockRecord(int xid, recordid rid);

int lockManagerUnlockRecord(int xid, recordid rid);
int lockManagerCommit(int xid);

void setupLockManagerCallbacksRecord();
void setupLockManagerCallbacksPage();
void setupLockManagerCallbacksNil();
