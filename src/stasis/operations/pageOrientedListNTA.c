#include <stasis/transactional.h>

#include <assert.h>

typedef struct {
  short nextEntry;
  short keySize;
} pagedListEntry;

recordid TpagedListAlloc(int xid) {
  recordid ret;
  ret = Talloc(xid, sizeof(pagedListHeader));
  pagedListHeader header;
  memset(&header,0,sizeof(header));
  header.thisPage = 0;
  header.nextPage = NULLRID;
  Tset(xid, ret, &header);
  return ret;
}

int TpagedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  int ret;

    pagedListHeader header;
    Tread(xid, list, &header);
    recordid headerRid = list;

    pagedListHeader firstHeader = header;

    ret = 0;
    int entrySize = sizeof(pagedListEntry) + keySize + valueSize;

    recordid rid = TallocFromPage(xid, headerRid.page, entrySize);
    DEBUG("Alloced rid: {%d %d %d}", rid.page, rid.slot, rid.size);

    // When the loop completes, header will contain the contents of the page header the entry will be inserted into,
    // headerrid will contain the rid of that header, and rid will contain the newly allocated recordid
    while(rid.size == -1) {
      if(header.nextPage.size == -1)  {
      // We're at the end of the list

        recordid newHeadRid = Talloc(xid, sizeof(pagedListHeader));
        pagedListHeader newHead;
        memset(&newHead,0,sizeof(newHead));
        newHead.thisPage = 0;
        newHead.nextPage = firstHeader.nextPage;

        firstHeader.nextPage = newHeadRid;

        Tset(xid, newHeadRid, &newHead);
        Tset(xid, list, &firstHeader);

        header = newHead;
        headerRid = newHeadRid;
      } else {
        headerRid = header.nextPage;
        Tread(xid, header.nextPage, &header);
      }
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

int TpagedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value) {
    pagedListHeader header;
    Tread(xid, list, &header);

    recordid rid;
    rid.page = list.page;
    rid.slot = header.thisPage;
    rid.size = 0;

    while(rid.slot || header.nextPage.size != -1) {

      if(rid.slot) {
        rid.size = TrecordSize(xid, rid);
        pagedListEntry * dat;
        dat = malloc(rid.size);
        Tread(xid, rid, dat);

        if(dat->keySize == keySize && !memcmp(dat+1, key, keySize)) {
          int valueSize = rid.size - keySize - sizeof(pagedListEntry);
          *value = malloc(valueSize);
          memcpy(*value, ((byte*)(dat+1))+keySize, valueSize);
          free(dat);
          return valueSize;
        }
        rid.slot = dat->nextEntry;  // rid.slot will be zero if this is the last entry
        free(dat);
      } else if (header.nextPage.size != -1) {  // another page
        rid.page = header.nextPage.page;
        Tread(xid, header.nextPage, &header);
        rid.slot = header.thisPage;
      }
    }
  return -1;
}

int TpagedListRemove(int xid, recordid list, const byte * key, int keySize) {
  pagedListHeader header;
  int ret = 0;

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

        if(dat->keySize == keySize && !memcmp(dat+1, key, keySize)) {

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
          ret = 1;
          break;
        }
        lastSlot = rid.slot;
        rid.slot = dat->nextEntry;
        free(dat);
      } else if (header.nextPage.size != -1) {  // another page
        lastSlot = -1;
        rid.page = header.nextPage.page;
        headerRid = header.nextPage;
        Tread(xid, header.nextPage, &header);
        rid.slot = header.thisPage;
      }
    }
  return ret;
}

int TpagedListMove(int xid, recordid start_list, recordid end_list, const byte * key, int keySize) {
  byte * value = NULL;
  int ret;
    int valueSize = TpagedListFind(xid, start_list, key, keySize, &value);
    if(valueSize != -1) {
      ret = TpagedListRemove(xid, start_list, key, keySize);
      assert(ret);
      ret = TpagedListInsert(xid, end_list, key, keySize, value, valueSize);
      assert(!ret);
      if(value) { free(value); }
      //      ret = 1;
    } else {
      ret = 0;
    }
  return ret;
}

lladd_pagedList_iterator * TpagedListIterator(int xid, recordid list) {
  pagedListHeader header;
  assert(list.size == sizeof(pagedListHeader));
    Tread(xid, list, &header);

  lladd_pagedList_iterator * it = malloc(sizeof(lladd_pagedList_iterator));

  it->headerRid = header.nextPage;
  it->entryRid  = list;
  //  printf("slot <- %d\n", header.thisPage);
  it->entryRid.slot = header.thisPage;

  return it;
}
void TpagedListClose(int xid, lladd_pagedList_iterator * it) {
  free(it);
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
