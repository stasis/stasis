#include <lladd/transactional.h>
#include <lladd/hash.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define __USE_GNU 
#include <pthread.h>

/*** @todo this is a big hack but it seems necessary to work with
     gcc when the moon is full. (thanks mike demmer. ;) */
#ifndef PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP
# define PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP \
  {0, 0, 0, PTHREAD_MUTEX_RECURSIVE_NP, __LOCK_INITIALIZER}
#endif


/** A quick note on the format of linked lists.  Each entry consists 
     of a struct with some variable length data appended to it.

  To access an entry's contents:
  
  lladd_linkedList_entry * entry; 
  ...
  if(entry->size) {
  	key = (byte*)(entry + 1);
  	value = ((byte*)(entry+1)) + keySize;
  } else {
    entry->size must be nonzero if the entry is defined.  It will be 
      zero if the entry is uniniailized (this can be the case if the 
      list has not yet been initialized. The end of the list is marked 
      by a next field with size -1.
  }
  
  To get the successor in the list:
  
  lladd_linkedList_entry next = entry->next;
  
  
  
	@file
*/

static pthread_mutex_t linked_list_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

static void __TlinkedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize);
static int  __TlinkedListRemove(int xid, recordid list, const byte * key, int keySize);
typedef struct {
  recordid list;
  int keySize;  
} lladd_linkedListInsert_log;
typedef struct {
  recordid list;
  int keySize;
  int valueSize;
} lladd_linkedListRemove_log;

static int operateInsert(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
  assert(!p);
  lladd_linkedListRemove_log * log = (lladd_linkedListRemove_log*)dat;
  
  byte * key;
  byte * value;
  int keySize, valueSize;
  
  keySize = log->keySize;
  valueSize = log->valueSize;
  key = (byte*)(log+1);
  value = ((byte*)(log+1))+keySize;
  pthread_mutex_lock(&linked_list_mutex);
//  printf("Operate insert called: rid.page = %d keysize = %d valuesize = %d %d {%d %d %d}\n", rid.page, log->keySize, log->valueSize, *(int*)key, value->page, value->slot, value->size);
  // Skip writing the undo!  Recovery will write a CLR after we're done, effectively
  // wrapping this in a nested top action, so we needn't worry about that either.
  __TlinkedListInsert(xid, log->list, key, keySize, value, valueSize);
  pthread_mutex_unlock(&linked_list_mutex);
  
  return 0;
}
static int operateRemove(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
  assert(!p);
  lladd_linkedListRemove_log * log = (lladd_linkedListRemove_log*)dat;
  
  byte * key;
  int keySize;
  
  keySize = log->keySize;
  key = (byte*)(log+1);
  pthread_mutex_lock(&linked_list_mutex);
//  printf("Operate remove called: %d\n", *(int*)key);
  // Don't call the version that writes an undo entry!
  __TlinkedListRemove(xid, log->list, key, keySize);
  pthread_mutex_unlock(&linked_list_mutex);
  return 0;
}

int TlinkedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  int ret = TlinkedListRemove(xid, list, key, keySize);

  lladd_linkedListInsert_log * undoLog = malloc(sizeof(lladd_linkedListInsert_log) + keySize);

  undoLog->list = list;
  undoLog->keySize = keySize;
  memcpy(undoLog+1, key, keySize);
  pthread_mutex_lock(&linked_list_mutex);
  void * handle = TbeginNestedTopAction(xid, OPERATION_LINKED_LIST_INSERT, 
                    (byte*)undoLog, sizeof(lladd_linkedListInsert_log) + keySize);
  free(undoLog);
  __TlinkedListInsert(xid, list, key, keySize, value, valueSize);

  TendNestedTopAction(xid, handle);
  pthread_mutex_unlock(&linked_list_mutex);
  
  return ret;  
}

Operation getLinkedListInsert() {
  Operation o = { 
    OPERATION_NOOP, 
    SIZEIS_PAGEID,
    OPERATION_LINKED_LIST_REMOVE,
    &operateInsert
  };
  return o;
}
Operation getLinkedListRemove() {
  Operation o = { 
    OPERATION_NOOP, 
    SIZEIS_PAGEID,
    OPERATION_LINKED_LIST_INSERT,
    &operateRemove
  };
  return o;
}
static void __TlinkedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  //int ret = TlinkedListRemove(xid, list, key, keySize);
  lladd_linkedList_entry * entry = malloc(sizeof(lladd_linkedList_entry) + keySize + valueSize);
  Tread(xid, list, entry);
  if(!entry->next.size) {
    memcpy(entry+1, key, keySize);
    memcpy(((byte*)(entry+1))+keySize, value, valueSize);
    entry->next.page = 0;
    entry->next.slot = 0;
    entry->next.size = -1;
    Tset(xid, list, entry);
  } else {
    lladd_linkedList_entry * newEntry = malloc(sizeof(lladd_linkedList_entry) + keySize + valueSize);
    memcpy(newEntry + 1, key, keySize);
    memcpy(((byte*)(newEntry+1))+keySize, value, valueSize);
    newEntry->next = entry->next;
    recordid newRid = Talloc(xid, sizeof(lladd_linkedList_entry) + keySize + valueSize);
    Tset(xid, newRid, newEntry);
    entry->next = newRid;
    Tset(xid, list, entry);
    free(newEntry);
  }
  free(entry);
  //return ret;
}

int TlinkedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value) {
  lladd_linkedList_entry * entry = malloc(list.size);
  pthread_mutex_lock(&linked_list_mutex);
  Tread(xid, list, entry);

  if(!entry->next.size) {
    free(entry);    
    pthread_mutex_unlock(&linked_list_mutex);
    return -1; // empty list 
  }
  while(1) {
    if(!memcmp(entry + 1, key, keySize)) { 
      // Bucket contains the entry of interest.
      int valueSize = list.size - (sizeof(lladd_linkedList_entry) + keySize);
     *value  = malloc(valueSize);
      memcpy(*value, ((byte*)(entry+1))+keySize, valueSize);
      free(entry);
      pthread_mutex_unlock(&linked_list_mutex);
      return valueSize;
    }
    if(entry->next.size != -1) {
       assert(entry->next.size == list.size);  // Don't handle lists with variable length records for now
       Tread(xid, entry->next, entry);
    } else {
       break;
    }
  }
  free(entry);
  pthread_mutex_unlock(&linked_list_mutex);
  return -1;
}




int TlinkedListRemove(int xid, recordid list, const byte * key, int keySize) {
  byte * value;
  int valueSize;
  pthread_mutex_lock(&linked_list_mutex);
  int ret = TlinkedListFind(xid, list, key, keySize, &value);
  if(ret != -1) {
    valueSize = ret;
  } else {
    pthread_mutex_unlock(&linked_list_mutex);
    return 0;
  }
  int entrySize = sizeof(lladd_linkedListRemove_log) + keySize + valueSize;
  lladd_linkedListRemove_log * undoLog = malloc(entrySize);

  undoLog->list = list;
  undoLog->keySize = keySize;
  undoLog->valueSize = valueSize;
  
  memcpy(undoLog+1, key, keySize);
  memcpy(((byte*)(undoLog+1))+keySize, value, valueSize);
 // printf("entry size %d sizeof(remove_log)%d keysize %d valuesize %d sizeof(rid) %d key %d value {%d %d %ld}\n",
 //       entrySize, sizeof(lladd_linkedListRemove_log), keySize, valueSize, sizeof(recordid), key, value->page, value->slot, value->size);
  void * handle = TbeginNestedTopAction(xid, OPERATION_LINKED_LIST_REMOVE, 
  			(byte*)undoLog, entrySize);
  free(value);
  free(undoLog);
  __TlinkedListRemove(xid, list, key, keySize);

  TendNestedTopAction(xid, handle);
  pthread_mutex_unlock(&linked_list_mutex);
  return 1;  
}

static int __TlinkedListRemove(int xid, recordid list, const byte * key, int keySize) {
  lladd_linkedList_entry * entry = malloc(list.size);
  pthread_mutex_lock(&linked_list_mutex);
  Tread(xid, list, entry);
  if(entry->next.size == 0) {
    //Empty List.
    free(entry);
    pthread_mutex_unlock(&linked_list_mutex);
    return 0;
  }
  int listRoot = 1;
  recordid lastRead = list;
  recordid oldLastRead;
  oldLastRead.size = -2;
  while(1) {
    if(!memcmp(entry + 1, key, keySize)) { 
      // Bucket contains the entry of interest.
      if(listRoot) {
	if(entry->next.size == -1) {
	  memset(entry, 0, list.size);
	  Tset(xid, lastRead, entry);
	} else {
	  assert(entry->next.size == list.size);  // Otherwise, sometihng strange is happening, or the list contains entries with variable sizes.
	  lladd_linkedList_entry * entry2 = malloc(list.size);
	  Tread(xid, entry->next, entry2);
	  Tdealloc(xid, entry->next); // could break iterator, since it writes one entry ahead.
	  Tset(xid, lastRead, entry2);
	  free(entry2);
	  }
      } else {
	lladd_linkedList_entry * entry2 = malloc(list.size);
	assert(oldLastRead.size != -2);
	Tread(xid, oldLastRead, entry2);
	memcpy(&(entry2->next), &(entry->next), sizeof(recordid));
	Tset(xid, oldLastRead, entry2);
	Tdealloc(xid, lastRead);
	free (entry2);
      }
      free(entry);
      pthread_mutex_unlock(&linked_list_mutex);
      return 1;
    } else { // Entry doesn't match the key we're looking for.
      if(entry->next.size != -1) {
	 assert(entry->next.size == list.size);  // Don't handle lists with variable length records for now
	 oldLastRead = lastRead;
	 lastRead = entry->next;
	 Tread(xid, entry->next, entry);
	 listRoot = 0;
      } else {
	 break;
      }
    }
  } 
  free(entry);
  pthread_mutex_unlock(&linked_list_mutex);
  return 0;
}
/*** @todo TlinkedListMove could be much faster, but this is good enough for a first pass */
int TlinkedListMove(int xid, recordid start_list, recordid end_list, const byte *key, int keySize) {
  byte * value;
  pthread_mutex_lock(&linked_list_mutex);
  int valueSize = TlinkedListFind(xid, start_list, key, keySize, &value);
  if(valueSize == -1) {
    pthread_mutex_unlock(&linked_list_mutex);
    return 0;
  } else {
    TlinkedListRemove(xid, start_list, key, keySize);
    TlinkedListInsert(xid, end_list, key, keySize, value, valueSize);
    free(value);
    pthread_mutex_unlock(&linked_list_mutex);
    return 1;
  }
}
recordid TlinkedListCreate(int xid, int keySize, int valueSize) {
  recordid ret = Talloc(xid, sizeof(lladd_linkedList_entry) + keySize + valueSize);
  byte * cleared = calloc(sizeof(lladd_linkedList_entry) + keySize + valueSize, sizeof(byte));
  Tset(xid, ret, cleared);
  free(cleared);
  return ret;
}
void TlinkedListDelete(int xid, recordid list) {
  lladd_linkedList_entry * entry = malloc(list.size);
  
  Tread(xid, list, entry);
  Tdealloc(xid, list);
  
  if(entry->next.size == 0) {
    return;
  }
  
  while(entry->next.size != -1) {
    recordid nextEntry;
    Tread(xid, nextEntry, entry);
    assert(!memcmp(&nextEntry, &(entry->next), sizeof(recordid)));
    Tdealloc(xid, nextEntry);
  }
  
  free(entry);
}

lladd_linkedList_iterator * TlinkedListIterator(int xid, recordid list, int keySize, int valueSize) {
  lladd_linkedList_iterator * it = malloc(sizeof(lladd_linkedList_iterator));
  it->keySize = keySize;
  it->valueSize = valueSize;
  it->next = list;
  it->first = -1;
  it->listRoot = list;
  return it;
}

int TlinkedListNext(int xid, lladd_linkedList_iterator * it, byte ** key, int * keySize, byte **value, int * valueSize) {

  if(it->next.size == -1)  { free(it); return 0; }
  pthread_mutex_lock(&linked_list_mutex);
  if(it->first == -1) {
    it->first = 1;
  } else if(it->first) {
    lladd_linkedList_entry * entry = malloc(it->next.size);
    Tread(xid, it->listRoot, entry);
    int listTouched;
    listTouched = memcmp(&(entry->next), &(it->next), sizeof(recordid));
    free(entry);
    if(listTouched) {
      //The root entry was removed.  Reset the iterator.
      it->first = -1;
      it->next = it->listRoot;
      int ret = TlinkedListNext(xid, it, key, keySize, value, valueSize);
      pthread_mutex_unlock(&linked_list_mutex);
      return ret;
    } else {
      //continue as normal.
      it->first = 0;
    }
  }
  
  assert(it->keySize + it->valueSize + sizeof(lladd_linkedList_entry) == it->next.size);

  lladd_linkedList_entry * entry = malloc(it->next.size);
  Tread(xid, it->next, entry);
  int ret;
  if(entry->next.size) {
    *keySize = it->keySize;
    *valueSize = it->valueSize;
    *key = malloc(*keySize);
    *value = malloc(*valueSize);
    
    it->next = entry->next;
  
    memcpy(*key, entry+1, *keySize);
    memcpy(*value, ((byte*)(entry + 1))+*keySize, *valueSize);
    
    ret = 1;    
  } else {
    // This entry was empty (this case occurs with empty lists)
    free(it);

    ret = 0;    
  }
  free(entry);  
  pthread_mutex_unlock(&linked_list_mutex);
  return ret;
}
