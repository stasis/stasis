#include "indirect.h"
#include "slotted.h"
#include <math.h>
#include <string.h>
#include <assert.h>
#include "../blobManager.h"
#include "../page.h"
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

recordid rallocMany(int parentPage, lsn_t lsn, int recordSize, int recordCount) {

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
    int firstChildPage = pageAllocMultiple(newPageCount);
    int tmpRecordCount = recordCount;
    int thisChildPage = firstChildPage;    

    while(tmpRecordCount > 0) {
      
      rallocMany(thisChildPage, lsn, recordSize, min(tmpRecordCount, next_level_records_per_page));
      tmpRecordCount -= next_level_records_per_page;
      thisChildPage ++;

    }

    assert((thisChildPage-firstChildPage)== newPageCount);

    tmpRecordCount = recordCount;

    Page * p = loadPage(parentPage);

    writelock(p->rwlatch, 99);
    
    indirectInitialize(p, level);
    
    int i = 0;

    for(tmpRecordCount = recordCount; tmpRecordCount > 0; tmpRecordCount -= next_level_records_per_page) {
      
      *page_ptr(p, i) = firstChildPage + i;
      if(i) {
	*maxslot_ptr(p, i) = *maxslot_ptr(p, i-1) + min(tmpRecordCount, next_level_records_per_page);
      } else {
	*maxslot_ptr(p, i) = min(tmpRecordCount, next_level_records_per_page);
      }
      i++;
    }

    assert(i == newPageCount);

    pageWriteLSN(p, lsn);

    unlock(p->rwlatch);
    releasePage(p);
    
    rid.page = parentPage;
    rid.slot = RECORD_ARRAY;
    rid.size = recordSize;

  } else {
    DEBUG("recordsize = %d, recordCount = %d, level = 0 (don't need indirect pages)\n", recordSize, recordCount);

    Page * p = loadPage(parentPage);

    writelock(p->rwlatch, 127);

    pageInitialize(p);

    unlock(p->rwlatch);

    for(int i = 0; i < recordCount; i++) {
      pageRalloc(p, recordSize);
    }

    writelock(p->rwlatch, 127);
    pageWriteLSN(p, lsn);
    unlock(p->rwlatch);

    
    releasePage(p);
    rid.page = parentPage;
    rid.slot = RECORD_ARRAY;
    rid.size = recordSize;
  }
  return rid;
}
