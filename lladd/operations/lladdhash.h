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

   A persistant hash, based on logical operations.

   lladdhash:  /yad-hash/ n.  LLADD's hash table, based on logical operations.

   @todo CORRECTNESS PROBLEM.  It allows logical operations to span
   more than one record, which is incompatible with our logical
   operations.  Also, Blob handling + LLADD hash's implementation
   result in poor performance when the bucket size is large, and
   transactions are short.

   $Id$

*/

#ifndef __LLADDHASH_H__
#define __LLADDHASH_H__

/*#include "../transactional.h"*/

#include <lladd/operations.h>

#define MAX_LLADDHASHES 1000

typedef struct {
  recordid store;
  size_t keylen;
  size_t datlen;
  recordid next;
} lladdHashItem_t;

typedef struct {
  int size;
  recordid hashmap_record;
  /*  recordid store; */
  int store;
  lladdHashItem_t *iterItem;
  unsigned int iterIndex;
  void *iterData;
  recordid* hashmap;
} lladdHash_t;

/** Allocate a new hash */

lladdHash_t * lHtCreate(int xid, int size);
int lHtValid(int xid, lladdHash_t *ht);
int lHtLookup( int xid, lladdHash_t *ht, const void *key, size_t keylen, void *buf );
int lHtFirst( int xid, lladdHash_t *ht, void *buf );
int lHtNext( int xid, lladdHash_t *ht, void *buf );
int lHtCurrent( int xid, lladdHash_t *ht, void *buf);
int lHtCurrentKey(int xid, lladdHash_t *ht, void *buf);
int lHtDelete(int xid, lladdHash_t *ht);
int lHtPosition( int xid, lladdHash_t *ht, const void *key, size_t key_length );
/* These two are the only ones that result in a log entry... */
/*
  int _lHtInsert(int xid, lladdHash_t *ht, const void *key, size_t keylen, void * dat, size_t datlen);
  int _lHtRemove( int xid, lladdHash_t *ht, const void *key, size_t keylen, void *buf );
*/

int lHtInsert(int xid, lladdHash_t *ht, const void *key, size_t keylen, void * dat, size_t datlen);
int lHtRemove( int xid, lladdHash_t *ht, const void *key, size_t keylen, void *buf, size_t buflen);

Operation getLHInsert();
Operation getLHRemove();


#endif
