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
		 const void * key, int keySize);
void ThashInstantUpdate(int xid, recordid hashRid, const void * key, int keySize, const void * val, int valSize);
void TlogicalHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
void TlogicalHashInsert(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
int  TlogicalHashDelete(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize);
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
