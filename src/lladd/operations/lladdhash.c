#include <config.h>
#include <lladd/common.h>

#include <lladd/operations/lladdhash.h>
#include <lladd/operations/pageOperations.h>
#include <lladd/bufferManager.h>
#include <lladd/transactional.h>

#include "../page/indirect.h"

#include <assert.h>
#include <stdio.h>

const recordid ZERO_RID = {0,0,0};

/** All of this information is derived from the hashTables rid at
    runtime, or is specific to the piece of memory returned by
    lHtOpen() */
struct lladdHash_t {
  recordid hashTable;
  int iter_next_bucket;
  recordid iter_next_ptr;
};

recordid lHtCreate(int xid, int size) {
  return rallocMany(xid, sizeof(recordid), size);

}

/** @todo lHtDelete is unimplemented.  First need to implement derallocMany() */
int lHtDelete(int xid, lladdHash_t *ht) {
  return 1;
}



lladdHash_t * lHtOpen(int xid, recordid rid) {
  lladdHash_t * ret = malloc(sizeof(lladdHash_t));
  ret->hashTable = rid;
  ret->iter_next_bucket = 0;
  ret->iter_next_ptr = ZERO_RID;
  return ret;
}

void lHtClose(int xid, lladdHash_t * lht) {
  free(lht);
}
/** @todo lHtValid could be more thorough. In particular, if we used
    arrays of fixed length pages, then we could cheaply verify that
    the entire hashbucket set had the correct size. */
int lHtValid(int xid, lladdHash_t *ht) {
  Page * p = loadPage(ht->hashTable.page);
  int ret = 1;
  if(*page_type_ptr(p) != INDIRECT_PAGE) {
    ret = 0;
  }
  if(ht->hashTable.slot == 0) {
    ht->hashTable.slot = 1;
    if(dereferenceRID(ht->hashTable).size != sizeof(recordid)) {
      ret = 0;
    }
    ht->hashTable.slot = 0;
  } else {
    ret = 1;
  }
  return ret;
}
/**
 * Hash function generator, taken directly from pblhash
 */
static int hash( const unsigned char * key, size_t keylen, int size ) {
    int ret = 104729;

    for( ; keylen-- > 0; key++ )
    {
        if( *key )
        {
            ret *= *key + keylen;
            ret %= size;
        }
    }

    return( ret % size );
}

typedef struct {
  recordid data;
  recordid next;
  int keyLength;
} lht_entry_record;

static lht_entry_record * follow_overflow(int xid, lht_entry_record * entry) {
  if(!isNullRecord(entry->next)) {
    recordid next = entry->next;
    entry = malloc(next.size);
    Tread(xid, next, entry);
    return entry;
  } else {
    return NULL;
  }
}

static lht_entry_record * getEntry(int xid, recordid * entryRID, lladdHash_t * ht, const void * key, int keySize) {
  
  recordid bucket = ht->hashTable;
  lht_entry_record * entry;

  recordid tmp;
  
  bucket.slot = hash(key, keySize, indirectPageRecordCount(bucket));

  if(!entryRID) {
    entryRID = &tmp;
  }

  Tread(xid, bucket, entryRID);
  
  if(!isNullRecord(*entryRID)) {
    
    entry = malloc(entryRID->size);
    
    Tread(xid, *entryRID, entry);
  } else { 
    entry = NULL;
  }
  while(entry && memcmp(entry+1, key, keySize)) {
    *entryRID = entry->next;
    if(!isNullRecord(*entryRID)) {
      lht_entry_record * newEntry = follow_overflow(xid, entry);
      free(entry);
      entry=newEntry;
    } else { 
      entry=NULL;
    }
  }
  
  return entry;
}
/** Insert a new entry into the hashtable.  The entry *must not* already exist. */
static void insert_entry(int xid, lladdHash_t * ht, const void * key, int keySize, recordid dat) {
  /* First, create the entry in memory. */

  recordid bucket = ht->hashTable;
  lht_entry_record * entry = malloc(sizeof(lht_entry_record) + keySize);
  bucket.slot = hash(key, keySize, indirectPageRecordCount(bucket));

  entry->data = dat;
  Tread(xid, bucket, &(entry->next));
  entry->keyLength = keySize;
  memcpy(entry+1, key, keySize);
  
  /* Now, write the changes to disk. */

  recordid entryRID = Talloc(xid, sizeof(lht_entry_record) + keySize);
  Tset(xid, entryRID, entry);
  Tset(xid, bucket, &entryRID);

  free(entry);

}
/** Assumes that the entry does, in fact, exist. */
static void delete_entry(int xid, lladdHash_t * ht, const void * key, int keySize) {

  lht_entry_record * entryToDelete;
  lht_entry_record * prevEntry = NULL;
  recordid prevEntryRID;
  recordid currentEntryRID;
  recordid nextEntryRID;

  recordid bucket = ht->hashTable;
  bucket.slot = hash(key, keySize, indirectPageRecordCount(bucket));

  Tread(xid, bucket, &currentEntryRID);

  entryToDelete = malloc(currentEntryRID.size);
  Tread(xid, currentEntryRID, entryToDelete);
  nextEntryRID = entryToDelete->next;

  while(memcmp(entryToDelete+1, key, keySize)) {

    if(prevEntry) {  
      free(prevEntry); 
    } 
    prevEntry = entryToDelete;
    prevEntryRID = currentEntryRID;

    entryToDelete = follow_overflow(xid, entryToDelete);
    assert(entryToDelete);
    currentEntryRID = nextEntryRID;
    
    nextEntryRID = entryToDelete->next;
  }

  if(prevEntry) {
    prevEntry->next = nextEntryRID;
    Tset(xid, prevEntryRID, prevEntry);
    free(prevEntry);
  } else {
    Tset(xid, bucket, &nextEntryRID);
  }
  Tdealloc(xid, currentEntryRID);
  free(entryToDelete);

}

recordid lHtLookup( int xid, lladdHash_t *ht, const void *key, int keylen) {
  recordid ret;
  lht_entry_record * entry = getEntry(xid, NULL, ht, key, keylen);
  if(entry) {
    ret = entry->data;
    free(entry);
  } else {
    ret = ZERO_RID;
  }
  return ret;
}



recordid lHtInsert(int xid, lladdHash_t *ht, const void *key, int keylen, recordid dat) { /*{void *dat, long datlen) { */
  recordid entryRID;
  recordid ret;
  lht_entry_record * entry = getEntry(xid, &entryRID, ht, key, keylen);
  if(entry){
    /*     assert(0); */
    ret = entry->data;
    entry->data = dat;
    Tset(xid, entryRID, entry);
  } else {
    insert_entry(xid, ht, key, keylen, dat);
    ret = ZERO_RID;
  }
  return ret;
  
}
/** @todo lHtRemove could be more efficient.  Currently, it looks up
    the hash table entry twice to remove it. */
recordid lHtRemove( int xid, lladdHash_t *ht, const void *key, int keySize) {

  /* ret = lookup key */
  lht_entry_record * entry = getEntry(xid, NULL, ht, key, keySize);
  recordid data;

  if(entry) {
    data = entry->data;
    
    delete_entry(xid, ht, key, keySize); 

  } else {
    data = ZERO_RID;
  }
  return data;
}

/** @todo hashtable iterators are currently unimplemented... */
int lHtPosition( int xid, lladdHash_t *ht, const void *key, int key_length ) {
  abort();
  return -1;
}
int lHtFirst( int xid, lladdHash_t *ht, void *buf ) {

  /*	ht->iterIndex = 0;
	ht->iterData = NULL;
	return lHtNext( xid, ht, buf); */
  abort();
  return -1;
}
int lHtNext( int xid, lladdHash_t *ht, void *buf ) {
  abort();
  return -1;
}
int lHtCurrent(int xid, lladdHash_t *ht, void *buf) {
  abort();
  return -1;
}
int lHtCurrentKey(int xid, lladdHash_t *ht, void *buf) {
  abort();
  return -1;
}
int isNullRecord(recordid x) {
  return (((x).slot == 0) && ((x).page == 0) && ((x).size==0));
}
