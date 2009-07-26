#include <stasis/transactional.h>

#include <assert.h>

typedef struct {
  short nextEntry;
  short keySize;
} pagedListEntry;

compensated_function recordid TpagedListAlloc(int xid) {
  recordid ret;
  try_ret(NULLRID) {
    ret = Talloc(xid, sizeof(pagedListHeader));
    pagedListHeader header;
    memset(&header,0,sizeof(header));
    header.thisPage = 0;
    header.nextPage = NULLRID;
    Tset(xid, ret, &header);
  } end_ret(NULLRID);
  return ret;
}

compensated_function int TpagedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize) {
  int ret;
  try_ret(compensation_error()) {
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
      if(compensation_error()) { break; }
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

  } end_ret(compensation_error());
  return ret;
}

compensated_function int TpagedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value) {
  try_ret(compensation_error()) {
    pagedListHeader header;
    Tread(xid, list, &header);

    recordid rid;
    rid.page = list.page;
    rid.slot = header.thisPage;
    rid.size = 0;

    while(rid.slot || header.nextPage.size != -1) {
      if(compensation_error()) { break; }

      if(rid.slot) {
        rid.size = TrecordSize(xid, rid);
        pagedListEntry * dat;
        if(compensation_error()) { break; }
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
  } end_ret(compensation_error());
  return -1;
}

compensated_function int TpagedListRemove(int xid, recordid list, const byte * key, int keySize) {
  pagedListHeader header;
  int ret = 0;
  try_ret(compensation_error()) {

    Tread(xid, list, &header);
    recordid headerRid;
    recordid rid;
    rid.page = list.page;
    rid.slot = header.thisPage;
    short lastSlot = -1;
    headerRid = list;
    while(rid.slot || header.nextPage.size != -1) {
      if(compensation_error()) { break; }
      if(rid.slot) {
        rid.size = TrecordSize(xid, rid);
        if(compensation_error()) { break; };
        pagedListEntry * dat = malloc(rid.size);
        Tread(xid, rid, dat);

        if(dat->keySize == keySize && !memcmp(dat+1, key, keySize)) {

          if(lastSlot != -1) {
            recordid lastRid = rid;
            lastRid.slot = lastSlot;
            lastRid.size = TrecordSize(xid, lastRid);
            if(compensation_error()) { free(dat); break; }
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
  } end_ret(compensation_error());
  return ret;
}

compensated_function int TpagedListMove(int xid, recordid start_list, recordid end_list, const byte * key, int keySize) {
  byte * value = NULL;
  int ret;
  try_ret(compensation_error()) {
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
  } end_ret(compensation_error());
  return ret;
}

compensated_function lladd_pagedList_iterator * TpagedListIterator(int xid, recordid list) {
  pagedListHeader header;
  assert(list.size == sizeof(pagedListHeader));
  try_ret(NULL) {
    Tread(xid, list, &header);
  } end_ret(NULL);

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
compensated_function int TpagedListNext(int xid, lladd_pagedList_iterator * it,
                                       byte ** key, int * keySize,
                                       byte ** value, int * valueSize) {
  while(it->entryRid.slot || it->headerRid.size != -1) {
    if(it->entryRid.slot) {
      try_ret(compensation_error()) {
        it->entryRid.size = TrecordSize(xid, it->entryRid);
      } end_ret(compensation_error());
      assert(it->entryRid.size != -1);

      pagedListEntry * entry = malloc(it->entryRid.size);
      begin_action_ret(free, entry, compensation_error()) {
        Tread(xid, it->entryRid, entry);
      } end_action_ret(compensation_error());

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
      try_ret(compensation_error()) {
        Tread(xid, it->headerRid, &header);
      } end_ret(compensation_error());
      it->entryRid.page = it->headerRid.page;
      it->headerRid = header.nextPage;
      it->entryRid.slot = header.thisPage;
      //      printf("slotB <- %d\n", it->entryRid.slot);
    }
  }
  return 0;
}
