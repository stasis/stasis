#include <lladd/transactional.h>


#include <malloc.h>
#include <assert.h>
#include <string.h>

typedef struct {
  short nextEntry;
  short keySize;
} pagedListEntry;

recordid TpagedListAlloc(int xid) {

  recordid ret = Talloc(xid, sizeof(pagedListHeader));
  pagedListHeader header;
  header.thisPage = 0;
  header.nextPage = NULLRID;
  Tset(xid, ret, &header);
  return ret;
}

compensated_function int TpagedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  pagedListHeader header;
  Tread(xid, list, &header);
  recordid headerRid = list;

  byte * garbage;
  int ret = (TpagedListFind(xid, list, key, keySize, &garbage) != -1);
  if(ret) {
    TpagedListRemove(xid, list, key, keySize);
    free(garbage);
  }
  int entrySize = sizeof(pagedListEntry) + keySize + valueSize;

  recordid rid = TallocFromPage(xid, headerRid.page, entrySize);
  DEBUG("Alloced rid: {%d %d %d}", rid.page, rid.slot, rid.size);

  // When the loop completes, header will contain the contents of the page header the entry will be inserted into, 
  // headerrid will contain the rid of that header, and rid will contain the newly allocated recordid
  while(rid.size == -1) {
    if(header.nextPage.size == -1)  {
      header.nextPage = Talloc(xid, sizeof(pagedListHeader));
      DEBUG("allocing on new page %d\n", header.nextPage.page);
      Tset(xid, headerRid, &header);
      pagedListHeader newHead;
      newHead.thisPage = 0;
      newHead.nextPage.page =0;
      newHead.nextPage.slot =0;
      newHead.nextPage.size =-1;
      Tset(xid, header.nextPage, &newHead);
    }

    headerRid = header.nextPage;
    Tread(xid, header.nextPage, &header);
    rid = TallocFromPage(xid, headerRid.page, entrySize);
    DEBUG("Alloced rid: {%d %d %d}", rid.page, rid.slot, rid.size);
  }

  pagedListEntry * dat = malloc(entrySize);
  
  dat->keySize   = keySize;
  dat->nextEntry = header.thisPage;
  memcpy(dat+1, key, keySize);
  memcpy(((byte*)(dat+1))+keySize, value, valueSize);
  Tset(xid, rid, dat);

  header.thisPage = rid.slot;
  DEBUG("Header.thisPage = %d\n", rid.slot);
  Tset(xid, headerRid, &header);
  free(dat);

  return ret;
}

compensated_function int TpagedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value) {
  pagedListHeader header;
  Tread(xid, list, &header);

  recordid rid;
  rid.page = list.page;
  rid.slot = header.thisPage;

  while(rid.slot || header.nextPage.size != -1) {

    if(rid.slot) {
      rid.size = TrecordSize(xid, rid);
      pagedListEntry * dat;      
      dat = malloc(rid.size);
      Tread(xid, rid, dat);
      
      if(!memcmp(dat+1, key, keySize)) {
	int valueSize = rid.size - keySize - sizeof(pagedListEntry);
	*value = malloc(valueSize);
	memcpy(*value, ((byte*)(dat+1))+keySize, valueSize);
	free(dat);
	return valueSize;
      }
      //      if(dat->nextEntry) {          // another entry on this page
      rid.slot = dat->nextEntry;
      free(dat);	//      }
    } else if (header.nextPage.size != -1) {  // another page
      rid.page = header.nextPage.page;
      Tread(xid, header.nextPage, &header);
      rid.slot = header.thisPage;
    } else {                     // we've reached the end of the last page
      rid.slot = 0;
    }
    

  }

  return -1;
}

compensated_function int TpagedListRemove(int xid, recordid list, const byte * key, int keySize) {
  pagedListHeader header;
  Tread(xid, list, &header);
  recordid headerRid;
  recordid rid;
  rid.page = list.page;
  rid.slot = header.thisPage;
  short lastSlot = -1;
  headerRid = list;
  while(rid.slot || header.nextPage.size != -1) {
    if(rid.slot) {
      rid.size = TrecordSize(xid, rid);
      pagedListEntry * dat = malloc(rid.size);
      Tread(xid, rid, dat);
      
      if(!memcmp(dat+1, key, keySize)) {
	
	if(lastSlot != -1) {
	  recordid lastRid = rid;
	  lastRid.slot = lastSlot;
	  lastRid.size = TrecordSize(xid, lastRid);
	  pagedListEntry * lastRidBuf = malloc(lastRid.size);
	  Tread(xid, lastRid, lastRidBuf);
	  lastRidBuf->nextEntry = dat->nextEntry;
	  Tset(xid, lastRid, lastRidBuf);
	  free(lastRidBuf);
	} else {
	  header.thisPage = dat->nextEntry;
	  Tset(xid, headerRid, &header);
	}
	Tdealloc(xid, rid);
	free(dat);
	return 1;
      }
      lastSlot = rid.slot;
      rid.slot = dat->nextEntry;
      //    }
      //    if(dat->nextEntry) {          // another entry on this page
      //      lastSlot = rid.slot;
      //      rid.slot = dat->nextEntry;
      free(dat);
    } else if (header.nextPage.size != -1) {  // another page
      lastSlot = -1;
      rid.page = header.nextPage.page;
      headerRid = header.nextPage;
      Tread(xid, header.nextPage, &header);
      rid.slot = header.thisPage;
      //    } else {                     // we've reached the end of the last page
      //      rid.slot = 0;
    }
    
    //    free(dat);
  }

  return 0;
}

compensated_function int TpagedListMove(int xid, recordid start_list, recordid end_list, const byte * key, int keySize) {
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
  pagedListHeader header;
  Tread(xid, list, &header);
  lladd_pagedList_iterator * it = malloc(sizeof(lladd_pagedList_iterator));

  it->headerRid = header.nextPage;
  it->entryRid  = list;
  //  printf("slot <- %d\n", header.thisPage);
  it->entryRid.slot = header.thisPage;

  return it;
}

int TpagedListNext(int xid, lladd_pagedList_iterator * it,
		   byte ** key, int * keySize,
		   byte ** value, int * valueSize) {
  while(it->entryRid.slot || it->headerRid.size != -1) {
    if(it->entryRid.slot) {
      it->entryRid.size = TrecordSize(xid, it->entryRid);
      assert(it->entryRid.size != -1);

      pagedListEntry * entry = malloc(it->entryRid.size);
      
      Tread(xid, it->entryRid, entry);
      
      *keySize = entry->keySize;
      *valueSize = it->entryRid.size - *keySize - sizeof(pagedListEntry);

      *key = malloc(*keySize);
      *value = malloc(*valueSize);

      memcpy(*key, entry+1, *keySize);
      memcpy(*value, ((byte*)(entry+1))+*keySize, *valueSize);
      
      it->entryRid.slot = entry->nextEntry;
      //      printf("slotA <- %d\n", it->entryRid.slot);

      free(entry);
      return 1;

    } else {  // move to next page.
      pagedListHeader header;
      Tread(xid, it->headerRid, &header);
      it->entryRid.page = it->headerRid.page;
      it->headerRid = header.nextPage;
      it->entryRid.slot = header.thisPage;
      //      printf("slotB <- %d\n", it->entryRid.slot);
    }
  }
  return 0;
}
