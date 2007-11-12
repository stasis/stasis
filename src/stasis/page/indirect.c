#include "indirect.h"
#include "slotted.h"
#include <math.h>
#include <string.h>
#include <assert.h>
#include "../blobManager.h"
#include "../page.h"

#include <stasis/operations.h>

void indirectInitialize(Page * p, int height) {
  *level_ptr(p) = height;
  stasis_page_cleanup(p);
  *stasis_page_type_ptr(p) = INDIRECT_PAGE;
  memset(p->memAddr, INVALID_SLOT, ((size_t)level_ptr(p)) - ((size_t)p->memAddr));
}
/** @todo Is locking for dereferenceRID really necessary? */
compensated_function recordid dereferenceIndirectRID(int xid, recordid rid) {
  Page * page;
  try_ret(NULLRID) {
    page = loadPage(xid, rid.page);
    readlock(page->rwlatch, 0);
  } end_ret(NULLRID);
  //  printf("a"); fflush(stdout);
  int offset = 0;
  int max_slot;
  while(*stasis_page_type_ptr(page) == INDIRECT_PAGE) {
    int i = 0;
    for(max_slot = *maxslot_ptr(page, i); ( max_slot + offset ) <= rid.slot; max_slot = *maxslot_ptr(page, i)) {
      i++;
      assert(max_slot != INVALID_SLOT);
    }

    if(i) {
      offset += *maxslot_ptr(page, i - 1);
    } /** else, the adjustment to the offset is zero */
    
    int nextPage = *page_ptr(page, i);

    unlock(page->rwlatch);
    releasePage(page);
    try_ret(NULLRID) {
      page = loadPage(xid, nextPage);
      readlock(page->rwlatch, 0);
    } end_ret(NULLRID);
  }
  //  printf("b"); fflush(stdout);
  
  rid.page = page->id;
  rid.slot -= offset;

  unlock(page->rwlatch);
  releasePage(page);
  //  printf("c"); fflush(stdout);

  return rid;
}

#define min(x, y) ((x) < (y) ? (x) : (y))
/** Would be static, but there is a unit test for this function */
unsigned int calculate_level (unsigned int number_of_pages) {
  long long tmp = INDIRECT_POINTERS_PER_PAGE;
  unsigned int level = 1;
  while(tmp < number_of_pages) {
    tmp *= INDIRECT_POINTERS_PER_PAGE;
    level++;
  }
  
  return level;
}

compensated_function recordid __rallocMany(int xid, int parentPage, int recordSize, int recordCount);
/**
   @todo is there a way to implement rallocMany so that it doesn't
   have to physically log pre- and post-images of the allocated space?
*/
compensated_function recordid rallocMany(int xid, int recordSize, int recordCount) {
  recordid ret;
  int page;
  try_ret(NULLRID) {
    page = TpageAlloc(xid/*, SLOTTED_PAGE*/);
  }end_ret(NULLRID);
  try_ret(NULLRID) {
    ret = __rallocMany(xid, page, recordSize, recordCount);
  }end_ret(NULLRID);

  return  ret;
}

compensated_function recordid __rallocMany(int xid, int parentPage, int recordSize, int recordCount) {

  /* How many levels of pages do we need? */

  int physical_size;
  recordid rid;

  if(recordSize > BLOB_THRESHOLD_SIZE) {
    physical_size = sizeof(blob_record_t);
  } else {
    physical_size = recordSize;
  }

  int records_per_page = (USABLE_SIZE_OF_PAGE - SLOTTED_PAGE_HEADER_OVERHEAD)
                             / (physical_size + SLOTTED_PAGE_OVERHEAD_PER_RECORD);   /* we need to take the floor */

  int number_of_pages = (int)ceil( (double)recordCount / (double)records_per_page);  /* need to take ceiling here */

  Page p;
  byte buffer[PAGE_SIZE];
  p.memAddr = buffer;
  p.rwlatch = initlock();
  p.loadlatch = initlock();
    
    
  if(number_of_pages > 1) {

    int level = calculate_level(number_of_pages);
    DEBUG("recordsize = %d, physicalsize = %d, recordCount = %d, level = %d\n", 
	   recordSize, physical_size, recordCount, level);

    /* OK, now allocate the pages. */
    
    int next_level_records_per_page = records_per_page;
    
    for(int i = 0; i < (level - 1); i++) {
      next_level_records_per_page *= INDIRECT_POINTERS_PER_PAGE;
    }
    
    int newPageCount = (int)ceil((double)recordCount / (double)next_level_records_per_page);
    int firstChildPage;

    try_ret(NULLRID) {
      firstChildPage = TpageAllocMany(xid, newPageCount/*, SLOTTED_PAGE*/);/*pageAllocMultiple(newPageCount); */
    } end_ret(NULLRID);

    int tmpRecordCount = recordCount;
    int thisChildPage = firstChildPage;    

    while(tmpRecordCount > 0) {
      try_ret(NULLRID) {
	__rallocMany(xid, thisChildPage, recordSize, min(tmpRecordCount, next_level_records_per_page));
      } end_ret(NULLRID);
      tmpRecordCount -= next_level_records_per_page;
      thisChildPage ++;

    }

    assert((thisChildPage-firstChildPage)== newPageCount);

    tmpRecordCount = recordCount;

    indirectInitialize(&p, level);
    
    int i = 0;

    for(tmpRecordCount = recordCount; tmpRecordCount > 0; tmpRecordCount -= next_level_records_per_page) {
      
      *page_ptr(&p, i) = firstChildPage + i;
      if(i) {
	*maxslot_ptr(&p, i) = *maxslot_ptr(&p, i-1) + min(tmpRecordCount+1, next_level_records_per_page);
      } else {
	*maxslot_ptr(&p, i) = min(tmpRecordCount+1, next_level_records_per_page);
      }
      i++;
    }

    assert(i == newPageCount);

  } else {
    DEBUG("recordsize = %d, recordCount = %d, level = 0 (don't need indirect pages)\n", recordSize, recordCount);
    
    /* Initialize leaves.  (As SLOTTED_PAGE's) */

    writelock(p.rwlatch,0);
    stasis_slotted_initialize_page(&p);
    p.id = parentPage;
    for(int i = 0; i < recordCount; i++) {
      /* Normally, we would worry that the page id isn't set, but
	 we're discarding the recordid returned by page ralloc
	 anyway. */
      recordid rid = stasis_record_alloc_begin(xid, &p, recordSize);
      stasis_record_alloc_done(xid, &p, rid);
    }
      unlock(p.rwlatch);
  }
  try_ret(NULLRID) {
    TpageSet(xid, parentPage, p.memAddr);
  } end_ret(NULLRID);

  rid.page = parentPage;
  rid.slot = RECORD_ARRAY;
  rid.size = recordSize;

  deletelock(p.rwlatch);
  deletelock(p.loadlatch);

  return rid;
}
 
compensated_function int indirectPageRecordCount(int xid, recordid rid) {
  Page * p;
  try_ret(-1){
    p = loadPage(xid, rid.page);
  }end_ret(-1);
  readlock(p->rwlatch, 0);
  int i = 0;
  unsigned int ret;
  if(*stasis_page_type_ptr(p) == INDIRECT_PAGE) {
    
    while(*maxslot_ptr(p, i) > 0) {
      i++;
    }
    if(!i) {
      ret = 0;
    } else {
      ret = (*maxslot_ptr(p, i-1)) - 1;
    }
  } else if (*stasis_page_type_ptr(p) == SLOTTED_PAGE) {

    int numslots = *numslots_ptr(p);
    ret = 0;
    for(int i = 0; i < numslots; i++) {
      if(*slot_ptr(p,i) != INVALID_SLOT) {
	ret++;
      }
    }
    
  } else {
    printf("Unknown page type in indirectPageRecordCount\n");
    abort();
  }

  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}

static int notSupported(int xid, Page * p) { return 0; }

void indirectLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
}
void indirectFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
}
void indirectCleanup(Page *p) { }
static page_impl pi = {
    INDIRECT_PAGE,
    0, //read,
    0, //write,
    0, //readDone
    0, //writeDone
    0, //getType,
    0, //setType,
    0, //getLength,
    0, //recordFirst,
    0, //recordNext,
    notSupported, // is block supported
    0, //pageGenericBlockFirst,
    0, //pageGenericBlockNext,
    0, //pageGenericBlockDone,
    0, //freespace,
    0, //compact,
    0, //preRalloc,
    0, //postRalloc,
    0, //Free,
    0, //XXX page_impl_dereference_identity,
    indirectLoaded,
    indirectFlushed,
    indirectCleanup
};

/**
    @todo Flesh out INDIRECT_PAGE's implementation of new PAGE_API, or
    remove INDIRECT_PAGE from Stasis.
*/
page_impl indirectImpl() {
  return pi;
}
