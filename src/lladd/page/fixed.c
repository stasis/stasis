#include "../page.h"

#include "fixed.h"

#include <assert.h>


int recordsPerPage(size_t size) {
  return (USABLE_SIZE_OF_PAGE - 2*sizeof(short)) / size;
}

void fixedPageInitialize(Page * page, size_t size, int count) {
  assert(*page_type_ptr(page) == UNINITIALIZED_PAGE);
  *page_type_ptr(page) = FIXED_PAGE;
  *recordsize_ptr(page) = size;
  assert(count <= recordsPerPage(size));
  *recordcount_ptr(page)= count;
}

short fixedPageCount(Page * page) {
  assert(*page_type_ptr(page) == FIXED_PAGE || *page_type_ptr(page) == ARRAY_LIST_PAGE);
  return *recordcount_ptr(page);
}

short fixedPageRecordSize(Page * page) {
  assert(*page_type_ptr(page) == FIXED_PAGE || *page_type_ptr(page) == ARRAY_LIST_PAGE);
  return *recordsize_ptr(page);
}

recordid fixedRawRallocMany(Page * page, int count) {

  assert(*page_type_ptr(page) == FIXED_PAGE);
  recordid rid;

  writelock(page->rwlatch, 33);
  if(*recordcount_ptr(page) + count <= recordsPerPage(*recordsize_ptr(page))) {
    rid.page = page->id;
    rid.slot = *recordcount_ptr(page);
    rid.size = *recordsize_ptr(page);
    *recordcount_ptr(page)+=count;
  } else {
    rid.page = -1;
    rid.slot = -1;
    rid.size = -1;
  }
  unlock(page->rwlatch);

  return rid;
}

recordid fixedRawRalloc(Page *page) {
  assert(*page_type_ptr(page) == FIXED_PAGE);
  return fixedRawRallocMany(page, 1);
}

static void checkRid(Page * page, recordid rid) {
  if(*page_type_ptr(page)) {
    assert(*page_type_ptr(page) == FIXED_PAGE || *page_type_ptr(page) == ARRAY_LIST_PAGE);
    assert(page->id == rid.page);
    assert(*recordsize_ptr(page) == rid.size);
    //  assert(recordsPerPage(rid.size) > rid.slot); 
    int recCount = *recordcount_ptr(page);
    assert(recCount  > rid.slot);          
  } else {
    fixedPageInitialize(page, rid.size, recordsPerPage(rid.size));
  }
}

void fixedReadUnlocked(Page * page, recordid rid, byte * buf) {
  if(!memcpy(buf, fixed_record_ptr(page, rid.slot), rid.size)) {
    perror("memcpy");
    abort();
  }
}
void fixedRead(Page * page, recordid rid, byte * buf) {
  readlock(page->rwlatch, 57);

  //  printf("R { %d %d %d }\n", rid.page, rid.slot, rid.size);
  checkRid(page, rid);



  fixedReadUnlocked(page, rid, buf);

  unlock(page->rwlatch);

}

void fixedWriteUnlocked(Page * page, recordid rid, const byte *dat) {
  checkRid(page,rid);
  if(!memcpy(fixed_record_ptr(page, rid.slot), dat, rid.size)) {
    perror("memcpy");
    abort();
  }
}

void fixedWrite(Page * page, recordid rid, const byte* dat) {
  readlock(page->rwlatch, 73);
  
  //  printf("W { %d %d %d }\n", rid.page, rid.slot, rid.size);
  //  checkRid(page, rid);

  fixedWriteUnlocked(page, rid, dat);

  unlock(page->rwlatch);
}
