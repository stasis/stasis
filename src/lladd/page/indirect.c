#include "indirect.h"
#include "slotted.h"
#include <math.h>
#include <string.h>
#include <assert.h>
#include "../blobManager.h"
#include "../page.h"

#include <lladd/operations.h>

void indirectInitialize(Page * p, int height) {
  *level_ptr(p) = height;
  *page_type_ptr(p) = INDIRECT_PAGE;
  memset(p->memAddr, INVALID_SLOT, ((int)level_ptr(p)) - ((int)p->memAddr));
}

recordid dereferenceRID(recordid rid) {
  Page * this = loadPage(rid.page);
  int offset = 0;
  int max_slot;
  while(*page_type_ptr(this) == INDIRECT_PAGE) {
    int i = 0;
    for(max_slot = *maxslot_ptr(this, i); ( max_slot + offset ) <= rid.slot; max_slot = *maxslot_ptr(this, i)) {
      i++;
      assert(max_slot != INVALID_SLOT);
    }

    if(i) {
      offset += *maxslot_ptr(this, i - 1);
    } /** else, the adjustment to the offset is zero */
    
    int nextPage = *page_ptr(this, i);

    releasePage(this);
    this = loadPage(nextPage);
  }
  
  rid.page = this->id;
  rid.slot -= offset;

  releasePage(this);
  
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

recordid __rallocMany(int xid, int parentPage, int recordSize, int recordCount);
/**
   @todo is there a way to implement rallocMany so that it doesn't
   have to physically log pre- and post-images of the allocated space?
*/
recordid rallocMany(int xid, int recordSize, int recordCount) {
  int page = TpageAlloc(xid, SLOTTED_PAGE);
  return __rallocMany(xid, page, recordSize, recordCount);
}

recordid __rallocMany(int xid, int parentPage, int recordSize, int recordCount) {

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

    int firstChildPage = TpageAllocMany(xid, newPageCount, SLOTTED_PAGE);/*pageAllocMultiple(newPageCount); */
    int tmpRecordCount = recordCount;
    int thisChildPage = firstChildPage;    

    while(tmpRecordCount > 0) {
      
      __rallocMany(xid, thisChildPage, recordSize, min(tmpRecordCount, next_level_records_per_page));
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

    pageInitialize(&p); 
    p.id = parentPage;
    for(int i = 0; i < recordCount; i++) {
      /* Normally, we would worry that the page id isn't set, but
	 we're discarding the recordid returned by page ralloc
	 anyway. */
      pageRalloc(&p, recordSize);
    }
    
  }

  TpageSet(xid, parentPage,  &p);
  
  rid.page = parentPage;
  rid.slot = RECORD_ARRAY;
  rid.size = recordSize;

  deletelock(p.rwlatch);
  deletelock(p.loadlatch);

  return rid;
}

unsigned int indirectPageRecordCount(recordid rid) {
  Page * p = loadPage(rid.page);
  int i = 0;
  unsigned int ret;
  if(*page_type_ptr(p) == INDIRECT_PAGE) {
    
    while(*maxslot_ptr(p, i) > 0) {
      i++;
    }
    if(!i) {
      ret = 0;
    } else {
      ret = (*maxslot_ptr(p, i-1)) - 1;
    }
  } else if (*page_type_ptr(p) == SLOTTED_PAGE) {

    int numslots = *numslots_ptr(p);
    ret = 0;
    for(int i = 0; i < numslots; i++) {
      if(isValidSlot(p, i)) {
	ret++;
      }
    }
    
  } else {
    printf("Unknown page type in indirectPageRecordCount()\n");
    fflush(NULL);
    abort();
  }
  releasePage(p);
  return ret;
}
