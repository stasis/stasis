#include <lladd/operations.h>

#ifndef __LINEAR_HASH_H
#define __LINEAR_HASH_H

/**
   @file 

   @ingroup OPERATIONS

   $Id$
*/



recordid ThashInstantAlloc(int xid, int keySize, int valSize) ;

void ThashInstantInsert(int xid, recordid hashRid, 
		 const void * key, int keySize, 
		 const void * val, int valSize);
void ThashInstantDelete(int xid, recordid hashRid, 
		 const void * key, int keySize, int valSize);
void ThashInstantUpdate(int xid, recordid hashRid, const void * key, int keySize, const void * val, int valSize);
void TlogicalHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
void TlogicalHashInsert(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
int  TlogicalHashDelete(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
int TlogicalHashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize);
typedef struct {
  long current_hashBucket;
  recordid current_rid;
} linearHash_iterator;
typedef struct {
  byte * key;
  byte * value;
} linearHash_iteratorPair;

linearHash_iterator * TlogicalHashIterator(int xid, recordid hashRid);
void TlogicalHashIteratorFree(linearHash_iterator * it);
linearHash_iteratorPair TlogicalHashIteratorNext(int xid, recordid hashRid, linearHash_iterator * it, int keySize, int valSize);


Operation getLinearInsert();
Operation getLinearInsert();
Operation getLinearDelete();
Operation getUndoLinearInsert();
Operation getUndoLinearDelete();

/*int ThashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize);
void ThashInit();
void ThashDeinit();
int ThashOpen(int xid, recordid hashRid);
int ThashClose(int xid, recordid hashRid) ; */

#endif
