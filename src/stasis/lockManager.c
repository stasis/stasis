/*
 * lockManager.c
 *
 *  Created on: Jun 12, 2011
 *      Author: sears
 */

#include <stasis/lockManager.h>

LockManagerSetup globalLockManager;

void setupLockManagerCallbacksNil (void) {
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

