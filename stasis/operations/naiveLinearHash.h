/**
   @file 

   A non-reentrant, experimental hashtable implementation.  This hash
   implementation provides the base of linearHash.h, and probably
   is only of interest to LLADD's developers.

   @ingroup OPERATIONS

   $Id$
*/

#ifndef __NAIVE_LINEAR_HASH_H
#define __NAIVE_LINEAR_HASH_H



recordid ThashAlloc(int xid, int keySize, int valSize) ;

void TnaiveHashInsert(int xid, recordid hashRid, 
		 void * key, int keySize, 
		 void * val, int valSize);
int TnaiveHashDelete(int xid, recordid hashRid, 
		 void * key, int keySize, int valSize);
void TnaiveHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
int TnaiveHashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize);
void ThashInit();
void ThashDeinit();
int ThashOpen(int xid, recordid hashRid, int keySize, int valSize);
int ThashClose(int xid, recordid hashRid) ;
void lockBucket(pageid_t bucket);
void unlockBucket(pageid_t bucket);
int lockBucketForKey(const byte * key, int keySize, recordid * headerRidB);
#endif
