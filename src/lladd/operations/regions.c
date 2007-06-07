#include "config.h"
#include "../page.h"
#include <lladd/operations.h>
#include <assert.h>

typedef struct regionAllocLogArg{
  int startPage;
  unsigned int pageCount;
  int allocationManager;
} regionAllocArg;

static pthread_mutex_t region_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t holding_mutex;
static void TregionAllocHelper(int xid, unsigned int pageid, unsigned int pageCount, int allocationManager);
static void TallocBoundaryTag(int xid, unsigned int page, boundary_tag* tag);
static void TreadBoundaryTag(int xid, unsigned int page, boundary_tag* tag); 
static void TsetBoundaryTag(int xid, unsigned int page, boundary_tag* tag); 
static void TdeallocBoundaryTag(int xid, unsigned int page);

/** This doesn't need a latch since it is only initiated within nested
    top actions (and is local to this file.  During abort(), the nested 
    top action's logical undo grabs the necessary latches.
    
    @todo opearate_alloc_boundary_tag is executed without holding the
    proper mutex during REDO.  For now this doesn't matter, but it
    could matter in the future.
*/
static int operate_alloc_boundary_tag(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) { 
  writelock(p->rwlatch, 0);
  slottedPageInitialize(p);
  *page_type_ptr(p) = BOUNDARY_TAG_PAGE;
  recordPostAlloc(xid, p, rid);
  pageWriteLSN(xid, p, lsn);
  byte * buf = recordWriteNew(xid, p, rid);
  memcpy(buf, dat, recordGetLength(xid, p, rid));
  unlock(p->rwlatch);
  return 0;
}

static int operate_alloc_region(int xid, Page * p, lsn_t lsn, recordid rid, const void * datP) { 
  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();
  regionAllocArg *dat = (regionAllocArg*)datP;
  TregionAllocHelper(xid, dat->startPage, dat->pageCount, dat->allocationManager);
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
  return 0;
}

static int operate_dealloc_region_unlocked(int xid, Page * p, lsn_t lsn, recordid rid, const void * datP) { 
  regionAllocArg *dat = (regionAllocArg*)datP;
  
  unsigned int firstPage = dat->startPage + 1;

  boundary_tag t; 

  TreadBoundaryTag(xid, firstPage - 1, &t);

  t.status = REGION_VACANT;
  t.region_xid = xid;
  
  TsetBoundaryTag(xid, firstPage -1, &t);

  //  TregionDealloc(xid, dat->startPage+1);
  return 0;
}

static int operate_dealloc_region(int xid, Page * p, lsn_t lsn, recordid rid, const void * datP) { 
  int ret;

  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();

  ret = operate_dealloc_region_unlocked(xid, p, lsn, rid, datP);

  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);

  return ret;
}

static void TallocBoundaryTag(int xid, unsigned int page, boundary_tag* tag) {
  //printf("Alloc boundary tag at %d = { %d, %d, %d }\n", page, tag->size, tag->prev_size, tag->status);
  assert(holding_mutex == pthread_self());
  recordid rid = {page, 0, sizeof(boundary_tag)};
  Tupdate(xid, rid, tag, OPERATION_ALLOC_BOUNDARY_TAG);
}

static void TreadBoundaryTag(int xid, unsigned int page, boundary_tag* tag) { 
  assert(holding_mutex == pthread_self());
  recordid rid = { page, 0, sizeof(boundary_tag) };
  assert(TpageGetType(xid, rid.page) == BOUNDARY_TAG_PAGE);
  Tread(xid, rid, tag);
  assert((page == 0 && tag->prev_size == UINT32_MAX) || (page != 0 && tag->prev_size != UINT32_MAX));
  //printf("Read boundary tag at %d = { %d, %d, %d }\n", page, tag->size, tag->prev_size, tag->status);
}
static void TsetBoundaryTag(int xid, unsigned int page, boundary_tag* tag) { 
  //printf("Write boundary tag at %d = { %d, %d, %d }\n", page, tag->size, tag->prev_size, tag->status);
  
  // Sanity checking:
  assert((page == 0 && tag->prev_size == UINT32_MAX) || (page != 0 && tag->prev_size < UINT32_MAX/2));
  assert(holding_mutex == pthread_self());

  boundary_tag t2;
  TreadBoundaryTag(xid, page, &t2);
  //assert(tag->size != t2.size || tag->prev_size != t2.prev_size || tag->status != t2.status); <-- Useful for finding performance problems.

  // Now, set the record:
  recordid rid = { page, 0, sizeof(boundary_tag) };
  Tset(xid, rid, tag);
}

static void TdeallocBoundaryTag(int xid, unsigned int page) {
  boundary_tag t;
  assert(holding_mutex == pthread_self());

  TreadBoundaryTag(xid, page, &t);
  t.status = REGION_CONDEMNED;
  t.region_xid = xid;
  TsetBoundaryTag(xid, page, &t);

}
 
void regionsInit() { 
  Page * p = loadPage(-1, 0);
  int pageType = *page_type_ptr(p);

  holding_mutex = pthread_self();
  if(pageType != BOUNDARY_TAG_PAGE) {
    boundary_tag t;
    t.size = UINT32_MAX;
    t.prev_size = UINT32_MAX;
    t.status = REGION_VACANT;
    t.region_xid = INVALID_XID;
    t.allocation_manager = 0;

    // This does what TallocBoundaryTag(-1, 0, &t); would do, but it
    // doesn't produce a log entry.  The log entry would be invalid
    // since we haven't initialized everything yet.  We don't need to
    // flush the page, since this code is deterministic, and will be
    // re-run before recovery if this update doesn't make it to disk
    // after a crash.
    recordid rid = {0,0,sizeof(boundary_tag)};

    operate_alloc_boundary_tag(0,p,0,rid,&t);
  }
  holding_mutex = 0;
  releasePage(p);
}

void fsckRegions(int xid) { 

  // Ignore region_xid, allocation_manager for now.
  pthread_mutex_lock(&region_mutex);
  holding_mutex = pthread_self();
  int pageType;
  boundary_tag tag;
  boundary_tag prev_tag;
  prev_tag.size = UINT32_MAX;
  int tagPage = 0;
  pageType = TpageGetType(xid, tagPage);
  assert(pageType == BOUNDARY_TAG_PAGE);

  TreadBoundaryTag(xid, tagPage, &tag);

  assert(tag.prev_size == UINT32_MAX);

  while(tag.size != UINT32_MAX) { 
    // Ignore region_xid, allocation_manager for now.
    assert(tag.status == REGION_VACANT || tag.status == REGION_ZONED);
    assert(prev_tag.size == tag.prev_size);

    for(int i = 0; i < tag.size; i++) { 
      int thisPage = tagPage + 1 + i;
      pageType = TpageGetType(xid, thisPage);

      if(pageType == BOUNDARY_TAG_PAGE) { 
	boundary_tag orphan;
	TreadBoundaryTag(xid, thisPage, &orphan);
	assert(orphan.status == REGION_CONDEMNED);
	Page * p  = loadPage(xid, thisPage);
	releasePage(p);
      } else if (pageType == SLOTTED_PAGE) { 
	Page * p = loadPage(xid, thisPage);
	releasePage(p);
      }
    }
    prev_tag = tag;
    tagPage = tagPage + 1 + prev_tag.size;
    TreadBoundaryTag(xid, tagPage, &tag);
  }

  assert(tag.status == REGION_VACANT);  // space at EOF better be vacant!
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);

}

static void TregionAllocHelper(int xid, unsigned int pageid, unsigned int pageCount, int allocationManager) {

  boundary_tag t;
  TreadBoundaryTag(xid, pageid, &t);


  if(t.size != pageCount) { 
    // need to split region
    // allocate new boundary tag.

    assert(t.size > pageCount);

    unsigned int newPageid = pageid + pageCount + 1;
    boundary_tag new_tag;
    
    if(t.size != UINT32_MAX) {

      new_tag.size = t.size - pageCount - 1; // pageCount must be strictly less than t->size, so this is non-negative.

      boundary_tag succ_tag;
      TreadBoundaryTag(xid, pageid + t.size + 1, &succ_tag);
      succ_tag.prev_size = new_tag.size;
      TsetBoundaryTag(xid, pageid + t.size + 1, &succ_tag);

    } else { 

      new_tag.size = UINT32_MAX;

    }
    new_tag.prev_size = pageCount;
    // Create the new region, and disassociate it from this transaction immediately. 
    // This has two implications:  
    //  - It could cause some fragmentation if interleaved transactions are allocating, and some abort.
    //  - Multiple transactions can allocate space at the end of the page file without blocking each other.
    new_tag.status = REGION_VACANT; 
    new_tag.region_xid = INVALID_XID;
    new_tag.allocation_manager = 0;

    TallocBoundaryTag(xid, newPageid, &new_tag);

  }

  t.status = REGION_ZONED;
  t.region_xid = xid;
  t.allocation_manager = allocationManager;
  t.size = pageCount;

  TsetBoundaryTag(xid, pageid, &t);

}

static void consolidateRegions(int xid, unsigned int * firstPage, boundary_tag  *t) { 

  if(t->status != REGION_VACANT || TisActiveTransaction(t->region_xid)) { return; }

  //  (*firstPage)++;

  int mustWriteOriginalTag = 0;

  // If successor is vacant, merge.
  if(t->size != UINT32_MAX) { // is there a successor?
    unsigned int succ_page = (*firstPage) + 1 + t->size;
    boundary_tag succ_tag;
    TreadBoundaryTag(xid, succ_page, &succ_tag);

    // TODO: Check page_type_ptr()...

    if(succ_tag.size == UINT32_MAX) { 
      t->size = UINT32_MAX;
      assert(succ_tag.status == REGION_VACANT);
      // TODO: Truncate page file.
      TdeallocBoundaryTag(xid, succ_page);
      mustWriteOriginalTag = 1;
    } else if(succ_tag.status == REGION_VACANT && (!TisActiveTransaction(succ_tag.region_xid))) {

      t->size = t->size + succ_tag.size + 1;
      unsigned int succ_succ_page = succ_page + succ_tag.size + 1;

      boundary_tag succ_succ_tag;

      TreadBoundaryTag(xid, succ_succ_page, &succ_succ_tag);
      succ_succ_tag.prev_size = t->size;
      TsetBoundaryTag(xid, succ_succ_page, &succ_succ_tag);
    
      TdeallocBoundaryTag(xid, succ_page);
      mustWriteOriginalTag = 1;
    } else { 
      mustWriteOriginalTag = 0;
    }

  }

  // If predecessor is vacant, merge.  (Doing this after the successor
  // is merged makes life easier, since merging with the predecessor
  // creates a situation where the current page is not a boundary
  // tag...)

  if(t->prev_size != UINT32_MAX) { 
    
    unsigned int pred_page = ((*firstPage) - 1) - t->prev_size;  // If the predecessor is length zero, then it's boundary tag is two pages before this region's tag.
    
    boundary_tag pred_tag;
    TreadBoundaryTag(xid, pred_page, &pred_tag);
    
    if(pred_tag.status == REGION_VACANT && (!TisActiveTransaction(pred_tag.region_xid))) { 
      
      TdeallocBoundaryTag(xid, *firstPage);
      
      if(t->size == UINT32_MAX) { 
	pred_tag.size = UINT32_MAX;
	
	// TODO: truncate region
	
      } else { 
	
	pred_tag.size += (t->size + 1);
	
	unsigned int succ_page = (*firstPage) + 1+ t->size;
	assert(pred_page + pred_tag.size + 1 == succ_page);
	
	boundary_tag succ_tag;
	TreadBoundaryTag(xid, succ_page, &succ_tag);
	succ_tag.prev_size = pred_tag.size;
	TsetBoundaryTag(xid, succ_page, &succ_tag);
	
	//	assert(succ_tag.status != REGION_VACANT);
	assert(succ_page - pred_page - 1 == pred_tag.size);
      }
      
      TsetBoundaryTag(xid, pred_page, &pred_tag);
      
      assert(pred_page < *firstPage);
      (*firstPage) = pred_page;
      (*t) = pred_tag;
    } else { 
      if(mustWriteOriginalTag) { 
	TsetBoundaryTag(xid, (*firstPage), t);
      } 
    }
  } else { 
    if(mustWriteOriginalTag) { 
      TsetBoundaryTag(xid, (*firstPage), t);
    }
  }

}

void TregionDealloc(int xid, unsigned int firstPage) {

  // Note that firstPage is the first *caller visible* page in the
  // region.  The boundary tag is stored on firstPage - 1.  Also, note
  // that a region of size N takes up N+1 pages on disk.

  // Deferred coalescing would probably make sense...

  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();

  boundary_tag t;
  TreadBoundaryTag(xid, firstPage-1, &t);

  regionAllocArg arg = { firstPage-1, t.size, t.allocation_manager };

  assert(t.status != REGION_VACANT); 

  void * handle = TbeginNestedTopAction(xid, OPERATION_DEALLOC_REGION, (const byte*)&arg, sizeof(regionAllocArg));

  operate_dealloc_region_unlocked(xid, 0, 0, NULLRID, (const byte*)&arg);

  /*t.status = REGION_VACANT;
  t.region_xid = xid;

  TsetBoundaryTag(xid, firstPage -1, &t); */

  firstPage --;

  TendNestedTopAction(xid, handle);
  
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
}

unsigned int TregionAlloc(int xid, unsigned int pageCount, int allocationManager) { 
  // Initial implementation.  Naive first fit.

  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();

  void * ntaHandle = TbeginNestedTopAction(xid, OPERATION_NOOP, 0, 0);

  unsigned int pageid = 0;
  boundary_tag t;

  TreadBoundaryTag(xid, pageid, &t); // XXX need to check if there is a boundary tag there or not!

  //  printf("consolidateRegions pageid, t: %d, {%d, %d, %d} -> ", pageid, t.size, t.prev_size, t.status);

  consolidateRegions(xid, &pageid, &t);
  
  //  printf(" %d, {%d, %d, %d}\tpageCount=%d\n", pageid, t.size, t.prev_size, t.status, pageCount);

  while(t.status != REGION_VACANT || t.size < pageCount || TisActiveTransaction(t.region_xid)) { 
    // TODO: This while loop and the boundary tag manipulation below should be factored into two submodules.

    //    printf("t.status = %d, REGION_VACANT = %d, t.size = %d, pageCount = %d\n", t.status, REGION_VACANT, t.size, pageCount);
    pageid += ( t.size + 1 );
    TreadBoundaryTag(xid, pageid, &t);

    //    printf("\tconsolidateRegions pageid, t: %d, {%d, %d, %d} -> ", pageid, t.size, t.prev_size, t.status);
    
    consolidateRegions(xid, &pageid, &t);
    
    //    printf(" %d, {%d, %d, %d}\tpageCount=%d\n", pageid, t.size, t.prev_size, t.status, pageCount);

  }
  //  printf("page = %d, t.status = %d, REGION_VACANT = %d, t.size = %d, pageCount = %d (alloced)\n", pageid, t.status, REGION_VACANT, t.size, pageCount);
  
  TendNestedTopAction(xid, ntaHandle);

  regionAllocArg arg = { pageid, pageCount, allocationManager };
  ntaHandle = TbeginNestedTopAction(xid, OPERATION_ALLOC_REGION, (const byte*)&arg, sizeof(regionAllocArg));

  TregionAllocHelper(xid, pageid, pageCount, allocationManager);

  TendNestedTopAction(xid, ntaHandle);

  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
  
  return pageid+1;
}


Operation getAllocBoundaryTag() {
  Operation o = { 
    OPERATION_ALLOC_BOUNDARY_TAG, 
    sizeof(boundary_tag),
    OPERATION_NOOP,
    &operate_alloc_boundary_tag
  };
  return o;
}

Operation getAllocRegion() { 
  Operation o = { 
    OPERATION_ALLOC_REGION,
    sizeof(regionAllocArg),
    OPERATION_DEALLOC_REGION,
    &operate_alloc_region
  };
  return o;
}

Operation getDeallocRegion() { 
  Operation o = { 
    OPERATION_DEALLOC_REGION,
    sizeof(regionAllocArg),
    OPERATION_ALLOC_REGION,
    &operate_dealloc_region
  };
  return o;
}

void TregionFindNthActive(int xid, unsigned int regionNumber, unsigned int * firstPage, unsigned int * size) { 
  boundary_tag t;
  recordid rid = {0, 0, sizeof(boundary_tag)};
  pthread_mutex_lock(&region_mutex);
  holding_mutex = pthread_self();
  Tread(xid, rid, &t);
  unsigned int prevSize = 0;
  while(t.status == REGION_VACANT) { 
    rid.page += (t.size + 1);
    Tread(xid, rid, &t);
    assert(t.size != UINT_MAX);
    assert(t.prev_size != UINT_MAX); 
    assert(prevSize == t.prev_size || !prevSize);
    prevSize = t.size;
  }
  for(int i = 0; i < regionNumber; i++) { 
    rid.page += (t.size + 1);
    Tread(xid, rid, &t);
    if(t.status == REGION_VACANT) { i--; }
    assert(t.size != UINT_MAX);
    assert(t.prev_size != UINT_MAX || i == 0);
    assert(prevSize == t.prev_size || !prevSize);
    prevSize = t.size;
  }
  *firstPage = rid.page+1;
  *size = t.size;
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
}
