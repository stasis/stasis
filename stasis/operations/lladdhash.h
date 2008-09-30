/**
 * A durable, recoverable hashtable
 * Based on Peter Graf's pblhash, <http://mission.base.com/peter/source/>
 * (actually based on jbhash.c, which was based on pblhash)
 *
 *
 * $Id$
 */

/**
   @file

   Do not use this hashtable.  Instead, use the one provided by
   linearHashNTA.h

   @deprecated This hash implementation is fundamentally flawed.

   A persistant hash, based on logical operations.

   lladdhash:  /yad-hash/ n.  LLADD's hash table, based on logical operations.

   @todo CORRECTNESS PROBLEM.  It allows logical operations to span
   more than one record, which is incompatible with our logical
   operations.  Also, Blob handling + LLADD hash's implementation
   result in poor performance when the bucket size is large, and
   transactions are short.

   @ingroup OPERATIONS

   $Id$

*/

#ifndef __LLADDHASH_H__
#define __LLADDHASH_H__

int isNullRecord(recordid x);

typedef struct lladdHash_t lladdHash_t;

/** Allocate a new hash */

recordid      lHtCreate(int xid, int size);
int           lHtDelete(int xid, lladdHash_t *ht);

lladdHash_t * lHtOpen(int xid, recordid rid) ;
void          lHtClose(int xid, lladdHash_t * lht);
int           lHtValid(int xid, lladdHash_t *ht);

recordid lHtLookup( int xid, lladdHash_t *ht, const void *key, int keylen);
/**
   
    @return ZERO_RECORDID if the entry did not already exist, the
    recordid of the old value of key otherwise.

 */
recordid lHtInsert(int xid, lladdHash_t *ht, const void *key, int keylen, recordid dat);
/**
   The recommended code sequence for deletion of a value is this:

   recordid old = lHtRemove(xid, ht, key, keylen);
   if(old != ZERO_RECORDID) { Tdealloc(xid, old); }

   If you are certain that the value exists in the hashtable, then it
   is safe to skip the (old != ZERO_RECORDID) check.

   @return the recordid of the entry if it existed, ZERO_RECORDID otherwise.
*/
recordid lHtRemove( int xid, lladdHash_t *ht, const void *key, int keylen);

/*
int lHtFirst( int xid, lladdHash_t *ht, void *buf );
int lHtNext( int xid, lladdHash_t *ht, void *buf );
int lHtCurrent( int xid, lladdHash_t *ht, void *buf);
int lHtCurrentKey(int xid, lladdHash_t *ht, void *buf);
int lHtPosition( int xid, lladdHash_t *ht, const void *key, int key_length );
*/

#endif
