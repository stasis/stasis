#include <lladd/transactional.h>
#include "../blobManager.h"
#include "../page.h"
#include "../page/slotted.h"
#include <assert.h>
#include <string.h>
/** 
    Low level function that looks at the page structure, and finds the 'real' recordid 
    of a page oriented list rid */
/*recordid dereferencePagedListRID(int xid, recordid rid) {
  Page * p = loadPage(rid.page);
  while((*numslots_ptr(p)-POLL_NUM_RESERVED) <= rid.slot) {
    int oldSlot = rid.slot;
    oldSlot -= (*numslots_ptr(p) - POLL_NUM_RESERVED);
    rid.slot = POLL_NEXT;
    readRecord(xid, p , rid, &rid);
    rid.slot = oldSlot;
    releasePage(p);
    p = loadPage(rid.page);
  }
  releasePage(p);
  rid.slot+=POLL_NUM_RESERVED;
  return rid;
}*/

recordid TpagedListAlloc(int xid) {
  long page = TpageAlloc(xid);
  recordid list = TallocFromPage(xid, page, sizeof(long));
  long zero = 0;
  Tset(xid, list, &zero);
  assert(list.slot == 0);
  assert(list.size == sizeof(long));
  return list;
}

int TpagedListSpansPages(int xid, recordid list) {
  // TpagedListCompact(int xid, recordid list);

  list.slot = 0;
  list.size = sizeof(long);
  long nextPage;
  Tread(xid, list, &nextPage);
  return nextPage != 0;
}

/** Should have write lock on page for this whole function. */
int TpagedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  int ret = 0;
  // if find in list, return 1
  byte * val;
  if(-1 != TpagedListFind(xid, list, key, keySize, &val)) {
    free(val);
    ret = 1;
    int removed = TpagedListRemove(xid, list, key, keySize);
    assert(removed);
  }
  Page * p = loadPage(list.page);
  int recordSize = (sizeof(short)+keySize+valueSize);
  int isBlob = recordSize >= BLOB_THRESHOLD_SIZE;
  int realSize = recordSize;
  if(isBlob) {
    recordSize = sizeof(blob_record_t);
  }

  while(slottedFreespace(p) < recordSize) {
    // load next page, update some rid somewhere
    list.slot = 0;
    list.size = sizeof(long);
    long nextPage;
    readRecord(xid, p, list, &nextPage);
    //    printf("+ page = %d nextpage=%ld freespace: %d recordsize: %d\n", list.page, nextPage, slottedFreespace(p), recordSize); fflush(stdout);
    if(nextPage == 0) {
      releasePage(p);
      nextPage = TpageAlloc(xid);
      Tset(xid, list, &nextPage);
      p = loadPage(nextPage);
      //slottedPageInitialize(p);
      // ** @todo shouldn't a log entry be generated here?? */
      list.page = nextPage;
      assert(slottedFreespace(p) >= recordSize);
      long zero = 0;
      recordid rid = TallocFromPage(xid, list.page, sizeof(long));
      Tset(xid, rid, &zero);
    } else {
      releasePage(p);
      list.page = nextPage;
      p = loadPage(nextPage);

    }
  }

  if(isBlob) {
    recordSize = realSize;
  }

  releasePage(p);
  //  printf("recordsize = %d\n", recordSize);
  recordid rid = TallocFromPage(xid, list.page, recordSize); // Allocates a record at a location given by the caller
  short* record = malloc(recordSize);
  *record = keySize;
  memcpy((record+1), key, keySize);
  memcpy(((char*)(record+1))+keySize, value, valueSize);
  Tset(xid, rid, record);
  
  return ret;
}
int TpagedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value) {

  long nextPage = 1;
  
  while (nextPage) {
    int i;

    Page * p = loadPage(list.page);
    
    //    int pageCount = TrecordsInPage(xid, list.page);
    int pageCount = *numslots_ptr(p);
 
    //    printf("%ld\n", nextPage);
    //fflush(stdout);

    for(i = 1; i < pageCount; i++) {
      recordid entry = list;
      entry.slot = i;
      //      int length = TrecordSize(xid, entry);
      int length = getRecordSize(xid,p,entry);
      if(length != -1) {  // then entry is defined.
	short * dat = malloc(length);
	entry.size = length;
	//	Tread(xid, entry, dat);
	slottedRead(xid, p, entry, (byte*)dat);
	if(*dat == keySize && !memcmp(dat+1, key, keySize)) {
	  int valueSize = length-keySize-sizeof(short);
	  *value = malloc(valueSize);
	  
	  memcpy(*value, ((byte*)(dat+1))+keySize, valueSize);
	  
	  free(dat);
	  releasePage(p);
	  return valueSize;
	}
	free(dat);
      }
    }


    //    recordid rid = list;
    
    list.slot = 0;
    list.size = sizeof(long);

    //    Tread(xid, list, &nextPage);
    slottedRead(xid, p, list, (byte*)&nextPage);

    list.page = nextPage;

    releasePage(p);


  } 
  return -1;
}
int TpagedListRemove(int xid, recordid list, const byte * key, int keySize) {
  long nextPage = 1;
  
  while (nextPage) {
    int i;

    Page * p = loadPage(list.page);

    //    int pageCount = TrecordsInPage(xid, list.page);
    int pageCount = *numslots_ptr(p);
 
    //    printf("%ld\n", nextPage);
    fflush(stdout);

    for(i = 1; i < pageCount; i++) {
      recordid entry = list;
      entry.slot = i;
      //      int length = TrecordSize(xid, entry);
      int length = getRecordSize(xid, p, entry);
      if(length != -1) {  // then entry is defined.
	short * dat = malloc(length);
	entry.size = length;
	
	slottedRead(xid,p,entry,(byte*)dat);

	//	Tread(xid, entry, dat);
	
	if(*dat == keySize && !memcmp(dat+1, key, keySize)) {
	  releasePage(p);
	  Tdealloc(xid, entry);
	  //	  assert(-1 == TrecordSize(xid, entry));
	  free(dat);
	  return 1;
	}
	free(dat);
      }
    }
    
    list.slot = 0;
    list.size = sizeof(long);

    //    Tread(xid, list, &nextPage);
    slottedRead(xid,p,list, (byte*)&nextPage);

    list.page = nextPage;
    releasePage(p);
  } 
  return 0;

}
int TpagedListMove(int xid, recordid start_list, recordid end_list, const byte *key, int keySize) {
  byte * value;
  int valueSize = TpagedListFind(xid, start_list, key, keySize, &value);
  if(valueSize != -1) {
    int ret = TpagedListRemove(xid, start_list, key, keySize);
    assert(ret);
    ret = TpagedListInsert(xid, end_list, key, keySize, value, valueSize);
    assert(!ret);
    free(value);
    return 1;
  } else { 
    return 0;
  }
}

lladd_pagedList_iterator * TpagedListIterator(int xid, recordid list) {
  lladd_pagedList_iterator * ret = malloc(sizeof(lladd_pagedList_iterator));

  ret->page = list.page;
  ret->slot = 1;

  return ret;
}

int TpagedListNext(int xid, lladd_pagedList_iterator * it,
		   byte ** key, int * keySize, 
		   byte ** value, int * valueSize) {
  //  printf("next: page %d slot %d\n", it->page, it->slot);
  recordid rid;
  while(it->page) {
    while(it->slot < TrecordsInPage(xid, it->page)) {
      rid.page = it->page;
      rid.slot = it->slot;
      rid.size=TrecordSize(xid, rid);
      if(rid.size != -1) {
	// found entry!
	
	byte * dat = malloc(rid.size);
	Tread(xid, rid, dat);
	
	// populate / alloc pointers passed in by caller.
	
	*keySize = *(short*)dat;
	*valueSize = rid.size - *keySize - sizeof(short);

	*key = malloc(*keySize);
	*value = malloc(*valueSize);

	memcpy(*key, ((short*)dat)+1, *keySize);
	memcpy(*value, ((byte*)(((short*)dat)+1)) + *keySize, *valueSize);

	free(dat);
	it->slot++;
	return 1;
      } 
      it->slot++;
    }
    rid.page = it->page;
    rid.slot = 0;
    rid.size = sizeof(long);
    Tread(xid, rid, &(it->page));
    it->slot = 1;

  }
  free(it);
    
  return 0;
  
}
