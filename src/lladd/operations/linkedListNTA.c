#include <lladd/transactional.h>
#include <lladd/hash.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
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
int TlinkedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  int ret = TlinkedListRemove(xid, list, key, keySize);
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
  return ret;
}

int TlinkedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value) {
  lladd_linkedList_entry * entry = malloc(list.size);
  Tread(xid, list, entry);

  if(!entry->next.size) {
    free(entry);    
    return -1; // empty list 
  }
  while(1) {
    if(!memcmp(entry + 1, key, keySize)) { 
      // Bucket contains the entry of interest.
      int valueSize = list.size - (sizeof(lladd_linkedList_entry) + keySize);
     *value  = malloc(valueSize);
      memcpy(*value, ((byte*)(entry+1))+keySize, valueSize);
      free(entry);
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
  return -1;
}
int TlinkedListRemove(int xid, recordid list, const byte * key, int keySize) {
  lladd_linkedList_entry * entry = malloc(list.size);

  Tread(xid, list, entry);
  if(entry->next.size == 0) {
    //Empty List.
    free(entry);
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
  return 0;
}
/*** @todo TlinkedListMove could be much faster, but this is good enough for a first pass */
int TlinkedListMove(int xid, recordid start_list, recordid end_list, const byte *key, int keySize) {
  byte * value;
  int valueSize = TlinkedListFind(xid, start_list, key, keySize, &value);
  if(valueSize == -1) {
    return 0;
  } else {
    TlinkedListRemove(xid, start_list, key, keySize);
    TlinkedListInsert(xid, end_list, key, keySize, value, valueSize);
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
      return TlinkedListNext(xid, it, key, keySize, value, valueSize);
    } else {
      //continue as normal.
      it->first = 0;
    }
  }
  
  assert(it->keySize + it->valueSize + sizeof(lladd_linkedList_entry) == it->next.size);

  lladd_linkedList_entry * entry = malloc(it->next.size);
  Tread(xid, it->next, entry);
  if(entry->next.size) {
    *keySize = it->keySize;
    *valueSize = it->valueSize;
    *key = malloc(*keySize);
    *value = malloc(*valueSize);
    
    it->next = entry->next;
  
    memcpy(*key, entry+1, *keySize);
    memcpy(*value, ((byte*)(entry + 1))+*keySize, *valueSize);
    
    free(entry);  
    return 1;
  } else {
    // This entry was empty (this case occurs with empty lists)
    free(it); 
    free(entry);
    return 0;
  }
}
