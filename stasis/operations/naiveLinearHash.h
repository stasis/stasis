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



recordid TnaiveHashCreate(int xid, int keySize, int valSize) ;

void TnaiveHashInsert(int xid, recordid hashRid, 
		 void * key, int keySize, 
		 void * val, int valSize);
int TnaiveHashDelete(int xid, recordid hashRid, 
		 void * key, int keySize, int valSize);
void TnaiveHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
int TnaiveHashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize);
void TnaiveHashInit();
void TnaiveHashDeinit();
int TnaiveHashOpen(int xid, recordid hashRid, int keySize, int valSize);
int TnaiveHashClose(int xid, recordid hashRid) ;
#endif
