#include <lladd/transactional.h>

void lockManagerInit();

int lockManagerReadLockRecord(int xid, recordid rid);
int lockManagerWriteLockRecord(int xid, recordid rid);

int lockManagerUnlockRecord(int xid, recordid rid);
int lockManagerReleaseAll(int xid);
