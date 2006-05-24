#include <stdlib.h>
#include <lladd/lhtable.h>
#include <lladd/hash.h>
#include <pbl/pbl.h>
#include <assert.h>
#include <string.h>

/**
   @todo Look up the balls + bins stuff, and pick FILL_FACTOR in a
   principled way...
*/
#define FILL_FACTOR (0.5)

/**

  @file

  In-memory hashtable implementation.  It uses linear hashing 
  to incrementally grow the bucket list. 
 
  Functions that end with "_r" are reentrant; those that do not are
  not.

  This file also contains compatibility routines so that code making
  use of the pbl hashtable implementation will continue to work.

*/
//===================================================== Static helper functions
static void extendHashTable(struct LH_ENTRY(table) * table) { 
  // @todo implement extendHashTable...
}


static struct LH_ENTRY(pair_t) * 
findInLinkedList(const void * key, int len, 
		 struct LH_ENTRY(pair_t)* list, 
		 struct LH_ENTRY(pair_t)** predecessor) { 
  *predecessor = 0;
  while(list) { 
    if(len == list->keyLength && !memcmp(key, list->key, len)) { 
      return list;
    } else {
      *predecessor = list;
      list = list->next;
    }
  }
  return 0;
}

//======================================================== The public interface

struct LH_ENTRY(table) {
  struct LH_ENTRY(pair_t) * bucketList;
  unsigned int bucketListLength;
  unsigned char bucketListBits;
  unsigned int bucketListNextExtension;
  unsigned int occupancy;
};


struct LH_ENTRY(table) * LH_ENTRY(create)(int initialSize) {
  struct LH_ENTRY(table) * ret = malloc(sizeof(struct LH_ENTRY(table)));
  ret->bucketList = calloc(initialSize, sizeof(struct LH_ENTRY(pair_t)));
  hashGetParamsForSize(initialSize, 
		       &(ret->bucketListBits),
		       &(ret->bucketListNextExtension));
  ret->bucketListLength = initialSize;
  ret->occupancy = 0;
  return ret;
}

LH_ENTRY(value_t) * LH_ENTRY(insert) (struct LH_ENTRY(table) * table,
				      const  LH_ENTRY(key_t) * key, int len,
					     LH_ENTRY(value_t) * value) { 
  // @todo 32 vs. 64 bit..
  long bucket = hash(key, len, 
		     table->bucketListBits, table->bucketListNextExtension);
  struct LH_ENTRY(pair_t) * thePair = 0;
  struct LH_ENTRY(pair_t) * junk;
  LH_ENTRY(value_t) * ret = 0;
  if(table->bucketList[bucket].key == 0) { 
    // The bucket's empty
    // Sanity checks...
    assert(table->bucketList[bucket].keyLength == 0);
    assert(table->bucketList[bucket].value     == 0);
    assert(table->bucketList[bucket].next      == 0);
    thePair = &(table->bucketList[bucket]);
    thePair->key = malloc(len);
    thePair->keyLength = len;
    memcpy(((void*)thePair->key), key, len);
    thePair->value = value;
    table->occupancy++;
  } else { 
    if((thePair = findInLinkedList(key, len, &(table->bucketList[bucket]), 
				   &junk))) { 
      // In this bucket.
      ret = thePair->value;
      thePair->value = value;
      // Don't need to update occupancy.
    } else { 
      // Not in this bucket, but the bucket isn't empty.
      thePair = malloc(sizeof(struct LH_ENTRY(pair_t)));
      thePair->key = malloc(len);
      memcpy((void*)thePair->key, key, len);
      thePair->keyLength = len;
      thePair->value = value;
      thePair->next = table->bucketList[bucket].next;
      table->bucketList[bucket].next = thePair;
      table->occupancy++;
    }
    // Need to check for + insert into linked list...
  }

  { // more sanity checks
    // Did we set thePair correctly?
    assert(thePair->value == value);
    assert(thePair->keyLength == len);
    assert(!memcmp(thePair->key, key, len));
    struct LH_ENTRY(pair_t) * pairInBucket = 0;
    // Is thePair in the bucket?
    assert((pairInBucket = findInLinkedList(key, len, 
					    &(table->bucketList[bucket]), 
					    &junk)));
    assert(pairInBucket == thePair);
    // Exactly one time?
    assert(!findInLinkedList(key, len, pairInBucket->next, &junk));
  }

  if(FILL_FACTOR < (  ((double)table->occupancy) / 
                      ((double)table->bucketListLength)
		    )) { 
    extendHashTable(table);
  }

  return ret;
}

LH_ENTRY(value_t) * LH_ENTRY(remove) (struct LH_ENTRY(table) * table,
				      const  LH_ENTRY(key_t) * key, int len) { 
  // @todo 32 vs. 64 bit..
  long bucket = hash(key, len, 
		     table->bucketListBits, table->bucketListNextExtension);
  struct LH_ENTRY(pair_t) * predecessor;
  struct LH_ENTRY(pair_t) * thePair;
  LH_ENTRY(value_t) * ret;
  thePair = findInLinkedList(key, len, 
			     &(table->bucketList[bucket]), 
			     &predecessor);
  if(!thePair) {                          // Not found; return null.
    ret = 0;
  } else if(thePair && !predecessor) {    // Found, in bucketlist.
    assert(thePair == &(table->bucketList[bucket]));
    free((void*)thePair->key);

    if(!thePair->next) {   
      // End of list; need to copy next into bucketlist, and free it.
      thePair->key = 0;
      thePair->keyLength = 0;
      ret = thePair->value;
      thePair->value = 0;
    } else { 
      // Freeing item in table->bucketList.  Copy its next pair to
      // bucketList, and free that item.
      struct LH_ENTRY(pair_t) * oldNext = thePair->next;
      *thePair = *(thePair->next);
      free(oldNext);
    }
  } else {  // Found, in spillover bucket.
    free((void*)thePair->key);
    predecessor->next = thePair->next;
    ret = thePair->value;
    free(thePair);
  }
  return ret;
}

LH_ENTRY(value_t) * LH_ENTRY(find)(struct LH_ENTRY(table) * table,
				   const  LH_ENTRY(key_t) * key, int len) { 
  // @todo 32 vs. 64 bit..
  int bucket = hash(key, len, 
		    table->bucketListBits, table->bucketListNextExtension);
  struct LH_ENTRY(pair_t) * predecessor;
  struct LH_ENTRY(pair_t) * thePair;
  thePair = findInLinkedList(key, len, 
			     &(table->bucketList[bucket]), 
			     &predecessor);
  if(!thePair) { 
    return 0;
  } else {
    return thePair->value;
  }
}

void LH_ENTRY(openlist)(const struct LH_ENTRY(table) * table, 
			struct LH_ENTRY(list)  * list) { 
  list->table = table;
  list->currentPair = 0;
  list->nextPair = 0;
  list->currentBucket = -1;

}

const struct LH_ENTRY(pair_t)* LH_ENTRY(readlist)(struct LH_ENTRY(list)  * list) { 
  assert(list->currentBucket != -2);
  while(!list->nextPair) {
    list->currentBucket++;
    if(list->currentBucket == list->table->bucketListLength) { 
      break;
    }
    if(list->table->bucketList[list->currentBucket].key) {
      list->nextPair = &(list->table->bucketList[list->currentBucket]);
    }
  }
  list->currentPair = list->nextPair;
  if(list->currentPair) { 
    list->nextPair = list->currentPair->next;
  }
  return list->currentPair;
}

void LH_ENTRY(closelist)(struct LH_ENTRY(list) * list) {
  assert(list->currentBucket != -2);
  list->currentBucket = -2;
}

void LH_ENTRY(destroy) (struct LH_ENTRY(table) * t) {
  struct LH_ENTRY(list) l;
  const struct LH_ENTRY(pair_t) * p;
  
  LH_ENTRY(openlist)(t, &l);
  while((p = LH_ENTRY(readlist)(&l))) { 
    LH_ENTRY(remove)(t, p->key, p->keyLength);
    // We always remove the head of the list, which breaks
    // the iterator.  Reset the iterator to the beginning of the bucket.
    l.nextPair = 0;
    l.currentPair = 0;
    l.currentBucket--;
  }
  LH_ENTRY(closelist)(&l);
  free(t->bucketList);
  free(t);
}


#ifdef  PBL_COMPAT

// ============ Legacy PBL compatibility functions.  There are defined in pbl.h

pblHashTable_t * pblHtCreate( ) {
  return (pblHashTable_t*)LH_ENTRY(create)(2017); // some prime number...
}
int    pblHtDelete  ( pblHashTable_t * h ) {
  LH_ENTRY(destroy)((struct LH_ENTRY(table)*)h);
  return 0;
}
int    pblHtInsert  ( pblHashTable_t * h, const void * key, size_t keylen,
		      void * dataptr) {
  // return values:
  // -1 -> item exists, or error
  // 0  -> inserted successfully

  if(LH_ENTRY(find)((struct LH_ENTRY(table)*)h, key, keylen)) {
    return -1;
  } else { 
    LH_ENTRY(insert)((struct LH_ENTRY(table)*)h, key, keylen, dataptr);
    return 0;
  }
}
int    pblHtRemove  ( pblHashTable_t * h, const void * key, size_t keylen ) {
  // return values:
  // 0 -> OK
  //-1 => not found (or error)
  if(LH_ENTRY(remove)((struct LH_ENTRY(table)*)h, key, keylen)) {
    return 0;
  } else { 
    return -1;
  }
}
void * pblHtLookup  ( pblHashTable_t * h, const void * key, size_t keylen ) { 
  // return values:
  // 0 -> not found (or error)
  return LH_ENTRY(find)((struct LH_ENTRY(table) *) h, key, keylen);
}

static struct LH_ENTRY(table) * pblLists = 0;

void * pblHtFirst   ( pblHashTable_t * h ) {
  if(pblLists == 0) {
    pblLists = LH_ENTRY(create)(10);
  }
  struct LH_ENTRY(list) *list = malloc(sizeof(struct LH_ENTRY(list)));
  struct LH_ENTRY(list) * oldList;

  if((oldList = LH_ENTRY(insert)(pblLists, 
				 &h, sizeof(pblHashTable_t*), 
				 list))) { 
    LH_ENTRY(closelist)(oldList);
    free(oldList);
  } 
  LH_ENTRY(openlist)((struct LH_ENTRY(table)*)h, 
		     list);
  const struct LH_ENTRY(pair_t) * p = LH_ENTRY(readlist)(list);
  if(p) { 
    return p->value;
  } else {
    oldList = LH_ENTRY(remove)(pblLists, &h, sizeof(pblHashTable_t*));
    free(oldList);
    return 0;
  }
}
void * pblHtNext    ( pblHashTable_t * h ) {
  struct LH_ENTRY(list) *list = LH_ENTRY(find)(pblLists, 
					       &h, sizeof(pblHashTable_t*));
  assert(list);
  const struct LH_ENTRY(pair_t) * p = LH_ENTRY(readlist)(list);
  if(p) { 
    return p->value;
  } else {
    struct LH_ENTRY(list)* oldList = 
      LH_ENTRY(remove)(pblLists, &h, sizeof(pblHashTable_t*));
    free(oldList);
    return 0;
  }
}
void * pblHtCurrent ( pblHashTable_t * h ) {
  struct LH_ENTRY(list) *list = LH_ENTRY(find)(pblLists, 
					       &h, sizeof(pblHashTable_t*));
  if(list && list->currentPair) 
    return list->currentPair->value;
  else 
    return 0;
}
void * pblHtCurrentKey ( pblHashTable_t * h ) {
  struct LH_ENTRY(list) *list = LH_ENTRY(find)(pblLists, 
					       &h, sizeof(pblHashTable_t*));
  if(list && list->currentPair)
    return (void*)list->currentPair->key;
  else 
    return 0;
}

#endif //PBL_COMPAT
