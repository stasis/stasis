#include <stdlib.h>
#include <lladd/lhtable.h>
#include <lladd/hash.h>
#include <pbl/pbl.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
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

#define NAIVE_LOCKING


struct LH_ENTRY(table) {
  struct LH_ENTRY(pair_t) * bucketList;
  unsigned int bucketListLength;
  unsigned char bucketListBits;
  unsigned int bucketListNextExtension;
  unsigned int occupancy;
#ifdef NAIVE_LOCKING
  pthread_mutex_t lock;
#endif
};


//===================================================== Static helper functions

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

static LH_ENTRY(value_t) * removeFromLinkedList(struct LH_ENTRY(table) * table,
						int bucket, 
						const LH_ENTRY(key_t)* key, int len){
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
      ret = thePair->value;
      struct LH_ENTRY(pair_t) * oldNext = thePair->next;
      *thePair = *(thePair->next);
      free(oldNext);
    }
  } else {  // Found, in spillover bucket.
    ret = thePair->value;
    free((void*)thePair->key);
    predecessor->next = thePair->next;
    free(thePair);
  }
  return ret;
}
static struct  LH_ENTRY(pair_t)* insertIntoLinkedList(struct LH_ENTRY(table) * table,
						      int bucket, 
						      const LH_ENTRY(key_t) * key, int len,
						      LH_ENTRY(value_t) * value){
  struct LH_ENTRY(pair_t) *thePair;
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
  } else { 
    // the bucket isn't empty.  
    thePair = malloc(sizeof(struct LH_ENTRY(pair_t)));
    thePair->key = malloc(len);
    memcpy((void*)thePair->key, key, len);
    thePair->keyLength = len;
    thePair->value = value;
    thePair->next = table->bucketList[bucket].next;
    table->bucketList[bucket].next = thePair;
  }
  return thePair;
}
static void extendHashTable(struct LH_ENTRY(table) * table) { 
  int maxExtension = twoToThe(table->bucketListBits-1);
  // If table->bucketListNextExtension == maxExtension, then newBucket =
  // twoToThe(table->bucketListBits), which is one higher than the hash can
  // return.

  if(table->bucketListNextExtension < maxExtension) { 
    table->bucketListNextExtension++;
  } else {
    table->bucketListNextExtension = 1;
    table->bucketListBits ++;
    maxExtension = twoToThe(table->bucketListBits-1);
  }
  
  int splitBucket   = table->bucketListNextExtension - 1;
  int newBucket     = table->bucketListNextExtension - 1 + maxExtension;

  // Assumes realloc is reasonably fast... This seems to be a good
  // assumption under linux.
  table->bucketList = realloc(table->bucketList, 
			      (1+newBucket) * sizeof(struct LH_ENTRY(pair_t)));
  table->bucketListLength = 1+newBucket;
  table->bucketList[newBucket].key = 0;
  table->bucketList[newBucket].keyLength = 0;
  table->bucketList[newBucket].value = 0;
  table->bucketList[newBucket].next = 0;

  // Now, table->nextExtension, table->tableBits are correct, so we
  // can call hash.

  struct LH_ENTRY(pair_t) * splitBucketRoot = 
    &(table->bucketList[splitBucket]);
  while(splitBucketRoot->key &&
	(hash(splitBucketRoot->key, splitBucketRoot->keyLength, 
	     table->bucketListBits, table->bucketListNextExtension) ==
	 newBucket)) {
    insertIntoLinkedList(table, newBucket, 
			 splitBucketRoot->key, splitBucketRoot->keyLength, 
			 splitBucketRoot->value);
    removeFromLinkedList(table, splitBucket, 
			 splitBucketRoot->key, splitBucketRoot->keyLength);
  }
  if(splitBucketRoot->key) {
    assert(hash(splitBucketRoot->key, splitBucketRoot->keyLength,
		table->bucketListBits, table->bucketListNextExtension) 
	   == splitBucket);
  } else { 
    assert(!splitBucketRoot->next);
  }
  struct LH_ENTRY(pair_t) * next = splitBucketRoot->next;
  while(next) { 
    // We know that next isn't the bucketList root, so removing it from
    // the list doesn't change its successor.
    struct LH_ENTRY(pair_t) * newNext = next->next;

    if(hash(next->key, next->keyLength, 
	    table->bucketListBits, table->bucketListNextExtension) ==
       newBucket) {
      insertIntoLinkedList(table, newBucket,
			   next->key, next->keyLength, next->value);
      removeFromLinkedList(table, splitBucket,
			   next->key, next->keyLength);
    } else { 
      assert(hash(next->key, next->keyLength, 
		  table->bucketListBits, table->bucketListNextExtension) == 
	     splitBucket);

    }
    next = newNext;
  }
}

//======================================================== The public interface


struct LH_ENTRY(table) * LH_ENTRY(create)(int initialSize) {
  struct LH_ENTRY(table) * ret = malloc(sizeof(struct LH_ENTRY(table)));
  ret->bucketList = calloc(initialSize, sizeof(struct LH_ENTRY(pair_t)));
  hashGetParamsForSize(initialSize, 
		       &(ret->bucketListBits),
		       &(ret->bucketListNextExtension));
  ret->bucketListLength = initialSize;
  ret->occupancy = 0;
  //  printf("Table: {size = %d, bits = %d, ext = %d\n", ret->bucketListLength, ret->bucketListBits, ret->bucketListNextExtension);
#ifdef NAIVE_LOCKING
  pthread_mutex_init(&(ret->lock), 0);
#endif
  return ret;
}

LH_ENTRY(value_t) * LH_ENTRY(insert) (struct LH_ENTRY(table) * table,
				      const  LH_ENTRY(key_t) * key, int len,
					     LH_ENTRY(value_t) * value) { 
#ifdef NAIVE_LOCKING
  pthread_mutex_lock(&(table->lock));
#endif
  // @todo 32 vs. 64 bit..
  long bucket = hash(key, len, 
		     table->bucketListBits, table->bucketListNextExtension);
  struct LH_ENTRY(pair_t) * thePair = 0;
  struct LH_ENTRY(pair_t) * junk;
  LH_ENTRY(value_t) * ret;
  /*  if(table->bucketList[bucket].key == 0) { 
    // XXX just call findInLinkedList, and then call
    // insertIntoLinkedList if it fails.  
    
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
    } else { */
  if((thePair = findInLinkedList(key, len, &(table->bucketList[bucket]), 
				 &junk))) { 
    // In this bucket.
    ret = thePair->value;
    thePair->value = value;
    // Don't need to update occupancy.
  } else { 
    // Not in this bucket
    thePair = insertIntoLinkedList(table, bucket, key, len, value);
    ret = 0;
    table->occupancy++;
  }
  //  }

  { // more sanity checks
    // Did we set thePair correctly?
    assert(thePair->value == value);
    assert(thePair->keyLength == len);
    assert(!memcmp(thePair->key, key, len));
    struct LH_ENTRY(pair_t) * pairInBucket = 0;
    // Is thePair in the bucket?
    pairInBucket = findInLinkedList(key, len, 
				    &(table->bucketList[bucket]), 
				    &junk);
    assert(pairInBucket);
    assert(pairInBucket == thePair);
    // Exactly one time?
    assert(!findInLinkedList(key, len, pairInBucket->next, &junk));
  }

  if(FILL_FACTOR < (  ((double)table->occupancy) / 
                      ((double)table->bucketListLength)
		    )) { 
    extendHashTable(table);
  }
#ifdef NAIVE_LOCKING
  pthread_mutex_unlock(&(table->lock));
#endif

  return ret;
}

LH_ENTRY(value_t) * LH_ENTRY(remove) (struct LH_ENTRY(table) * table,
				      const  LH_ENTRY(key_t) * key, int len) { 
#ifdef NAIVE_LOCKING
  pthread_mutex_lock(&(table->lock));
#endif
  // @todo 32 vs. 64 bit..
  long bucket = hash(key, len, 
		     table->bucketListBits, table->bucketListNextExtension);

  LH_ENTRY(value_t) * ret = removeFromLinkedList(table, bucket, key, len);
#ifdef NAIVE_LOCKING
  pthread_mutex_unlock(&(table->lock));
#endif
  return ret;
}

LH_ENTRY(value_t) * LH_ENTRY(find)(struct LH_ENTRY(table) * table,
				   const  LH_ENTRY(key_t) * key, int len) { 
#ifdef NAIVE_LOCKING
  pthread_mutex_lock(&(table->lock));
#endif
  // @todo 32 vs. 64 bit..
  int bucket = hash(key, len, 
		    table->bucketListBits, table->bucketListNextExtension);
  struct LH_ENTRY(pair_t) * predecessor;
  struct LH_ENTRY(pair_t) * thePair;
  thePair = findInLinkedList(key, len, 
			     &(table->bucketList[bucket]), 
			     &predecessor);
#ifdef NAIVE_LOCKING
  pthread_mutex_unlock(&(table->lock));
#endif

  if(!thePair) { 
    return 0;
  } else {
    return thePair->value;
  }
}

void LH_ENTRY(openlist)(const struct LH_ENTRY(table) * table, 
			struct LH_ENTRY(list)  * list) { 
#ifdef NAIVE_LOCKING
  pthread_mutex_lock(&(((struct LH_ENTRY(table)*)table)->lock));
#endif
  list->table = table;
  list->currentPair = 0;
  list->nextPair = 0;
  list->currentBucket = -1;
#ifdef NAIVE_LOCKING
  pthread_mutex_unlock(&(((struct LH_ENTRY(table)*)table)->lock));
#endif

}

const struct LH_ENTRY(pair_t)* LH_ENTRY(readlist)(struct LH_ENTRY(list)  * list) { 
#ifdef NAIVE_LOCKING
  pthread_mutex_lock(&(((struct LH_ENTRY(table)*)(list->table))->lock));
#endif
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
  // XXX is it even meaningful to return a pair object on an unlocked hashtable?
  const struct LH_ENTRY(pair_t)* ret = list->currentPair;
#ifdef NAIVE_LOCKING
  pthread_mutex_unlock(&(((struct LH_ENTRY(table)*)(list->table))->lock));
#endif
  return ret;
}

void LH_ENTRY(closelist)(struct LH_ENTRY(list) * list) {
#ifdef NAIVE_LOCKING
  pthread_mutex_lock(&(((struct LH_ENTRY(table)*)(list->table))->lock));
#endif
  assert(list->currentBucket != -2);
  list->currentBucket = -2;
#ifdef NAIVE_LOCKING
  pthread_mutex_unlock(&(((struct LH_ENTRY(table)*)(list->table))->lock));
#endif
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
#ifdef NAIVE_LOCKING
  pthread_mutex_destroy(&(t->lock));
#endif
  free(t);
}


#ifdef  PBL_COMPAT

// ============ Legacy PBL compatibility functions.  There are defined in pbl.h

pblHashTable_t * pblHtCreate( ) {
  //  return (pblHashTable_t*)LH_ENTRY(create)(2048);
  return (pblHashTable_t*)LH_ENTRY(create)(16); 
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
