#include "config.h"
#include <stasis/page.h>
#include <stasis/operations.h>
#include <stasis/logger/logger2.h>
#include <assert.h>

typedef struct regionAllocLogArg{
  pageid_t startPage;
  pageid_t pageCount;
  int allocationManager;
} regionAllocArg;

static pthread_mutex_t region_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t holding_mutex;
static void TregionAllocHelper(int xid, pageid_t page, pageid_t pageCount, int allocationManager);
static void TallocBoundaryTag(int xid, pageid_t page, boundary_tag* tag);
static int  readBoundaryTag(int xid, pageid_t page, boundary_tag* tag); 
static void TsetBoundaryTag(int xid, pageid_t page, boundary_tag* tag); 
static void TdeallocBoundaryTag(int xid, pageid_t page);

/** This doesn't need a latch since it is only initiated within nested
    top actions (and is local to this file.  During abort(), the nested 
    top action's logical undo grabs the necessary latches.
*/
static int op_alloc_boundary_tag(const LogEntry* e, Page* p) {
  stasis_slotted_initialize_page(p);
  recordid rid = {p->id, 0, sizeof(boundary_tag)};
  assert(e->update.arg_size == sizeof(boundary_tag));
  *stasis_page_type_ptr(p) = BOUNDARY_TAG_PAGE;
  stasis_record_alloc_done(e->xid, p, rid);
  byte * buf = stasis_record_write_begin(e->xid, p, rid);
  memcpy(buf, getUpdateArgs(e), stasis_record_length_read(e->xid, p, rid));
  stasis_record_write_done(e->xid, p, rid, buf);
  return 0;
}

static int op_alloc_region(const LogEntry *e, Page* p) {
  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();
  regionAllocArg *dat = (regionAllocArg*)getUpdateArgs(e);
  TregionAllocHelper(e->xid, dat->startPage, dat->pageCount, dat->allocationManager);
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
  return 0;
}

static int operate_dealloc_region_unlocked(int xid, regionAllocArg *dat) {

  pageid_t firstPage = dat->startPage + 1;

  boundary_tag t; 

  int ret = readBoundaryTag(xid, firstPage - 1, &t);
  assert(ret);

  t.status = REGION_VACANT;
  t.region_xid = xid;

  TsetBoundaryTag(xid, firstPage -1, &t);

  return 0;
}

static int op_dealloc_region(const LogEntry* e, Page* p) {
  int ret;

  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();

  ret = operate_dealloc_region_unlocked(e->xid, (regionAllocArg*)getUpdateArgs(e));

  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);

  return ret;
}

static void TallocBoundaryTag(int xid, pageid_t page, boundary_tag* tag) {
  //printf("Alloc boundary tag at %d = { %d, %d, %d }\n", page, tag->size, tag->prev_size, tag->status);
  assert(holding_mutex == pthread_self());
  Tupdate(xid, page, tag, sizeof(boundary_tag), OPERATION_ALLOC_BOUNDARY_TAG);
}

int readBoundaryTag(int xid, pageid_t page, boundary_tag* tag) { 
  assert(holding_mutex == pthread_self());
  recordid rid = { page, 0, sizeof(boundary_tag) };
  if(TpageGetType(xid, rid.page) != BOUNDARY_TAG_PAGE) {
    return 0;
  }
  Tread(xid, rid, tag);
  assert((page == 0 && tag->prev_size == PAGEID_T_MAX) || (page != 0 && tag->prev_size != PAGEID_T_MAX));
  //printf("Read boundary tag at %d = { %d, %d, %d }\n", page, tag->size, tag->prev_size, tag->status);
  return 1;
}
int TregionReadBoundaryTag(int xid, pageid_t page, boundary_tag* tag) {
  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();
  int ret = readBoundaryTag(xid,page-1,tag);
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
  return ret;
}

static void TsetBoundaryTag(int xid, pageid_t page, boundary_tag* tag) { 
  //printf("Write boundary tag at %d = { %d, %d, %d }\n", page, tag->size, tag->prev_size, tag->status);

  // Sanity checking:
  assert((page == 0 && tag->prev_size == PAGEID_T_MAX) || (page != 0 && tag->prev_size < PAGEID_T_MAX/2));
  assert(holding_mutex == pthread_self());

  boundary_tag t2;
  int ret = readBoundaryTag(xid, page, &t2);
  assert(ret);
  //assert(tag->size != t2.size || tag->prev_size != t2.prev_size || tag->status != t2.status); <-- Useful for finding performance problems.

  // Now, set the record:
  recordid rid = { page, 0, sizeof(boundary_tag) };
  Tset(xid, rid, tag);
}

static void TdeallocBoundaryTag(int xid, pageid_t page) {
  boundary_tag t;
  assert(holding_mutex == pthread_self());

  int ret = readBoundaryTag(xid, page, &t);
  assert(ret);
  t.status = REGION_CONDEMNED;
  t.region_xid = xid;
  TsetBoundaryTag(xid, page, &t);

}
 
void regionsInit() { 
  Page * p = loadPage(-1, 0);
  int pageType = *stasis_page_type_ptr(p);

  holding_mutex = pthread_self();
  if(pageType != BOUNDARY_TAG_PAGE) {
    boundary_tag t;
    t.size = PAGEID_T_MAX;
    t.prev_size = PAGEID_T_MAX;
    t.status = REGION_VACANT;
    t.region_xid = INVALID_XID;
    t.allocation_manager = 0;

    // This does what TallocBoundaryTag(-1, 0, &t); would do, but it
    // doesn't produce a log entry.  The log entry would be invalid
    // since we haven't initialized everything yet.  We don't need to
    // flush the page, since this code is deterministic, and will be
    // re-run before recovery if this update doesn't make it to disk
    // after a crash.
    //    recordid rid = {0,0,sizeof(boundary_tag)};

    // hack; allocate a fake log entry; pass it into ourselves.
    LogEntry * e = allocUpdateLogEntry(0,0,OPERATION_ALLOC_BOUNDARY_TAG,
                                       p->id, (const byte*)&t, sizeof(boundary_tag));
    writelock(p->rwlatch,0);
    op_alloc_boundary_tag(e,p);
    unlock(p->rwlatch);
    freeLogEntry(e);
  }
  holding_mutex = 0;
  releasePage(p);
}
const static int FIRST_TAG = 0;
int TregionNextBoundaryTag(int xid, pageid_t* pid, boundary_tag * tag, int type) {
  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();


  int ret = readBoundaryTag(xid, *pid-1, tag);
  if(ret) { 
    while(1) {
      if(tag->size == PAGEID_T_MAX) {
	ret = 0;
	break;
      }
      *pid += tag->size+1;
      int succ = readBoundaryTag(xid,*pid-1, tag);
      assert(succ); // detect missing boundary tags
      DEBUG("tag status = %d\n",tag->status);
      if(tag->status == REGION_ZONED && ((!type) || (tag->allocation_manager == type))) {
	break;
      }
    }
  } else {
    ret = 0;
  }
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
  return ret;
}

void fsckRegions(int xid) { 

  // Ignore region_xid, allocation_manager for now.
  pthread_mutex_lock(&region_mutex);
  holding_mutex = pthread_self();
  int pageType;
  boundary_tag tag;
  boundary_tag prev_tag;
  prev_tag.size = PAGEID_T_MAX;
  pageid_t tagPage = 0;
  pageType = TpageGetType(xid, tagPage);
  assert(pageType == BOUNDARY_TAG_PAGE);

  int ret =readBoundaryTag(xid, tagPage, &tag);
  assert(ret);
  assert(tag.prev_size == PAGEID_T_MAX);

  while(tag.size != PAGEID_T_MAX) { 
    // Ignore region_xid, allocation_manager for now.
    assert(tag.status == REGION_VACANT || tag.status == REGION_ZONED);
    assert(prev_tag.size == tag.prev_size);

    for(pageid_t i = 0; i < tag.size; i++) { 
      pageid_t thisPage = tagPage + 1 + i;
      pageType = TpageGetType(xid, thisPage);

      if(pageType == BOUNDARY_TAG_PAGE) { 
	boundary_tag orphan;
	int ret = readBoundaryTag(xid, thisPage, &orphan);
	assert(ret);
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
    int ret = readBoundaryTag(xid, tagPage, &tag);
    assert(ret);
  }

  assert(tag.status == REGION_VACANT);  // space at EOF better be vacant!
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);

}

static void TregionAllocHelper(int xid, pageid_t page, pageid_t pageCount, int allocationManager) {

  boundary_tag t;
  int ret = readBoundaryTag(xid, page, &t);
  assert(ret);

  if(t.size != pageCount) { 
    // need to split region
    // allocate new boundary tag.

    assert(t.size > pageCount);

    pageid_t newPageid = page + pageCount + 1;
    boundary_tag new_tag;
    
    if(t.size != PAGEID_T_MAX) {

      new_tag.size = t.size - pageCount - 1; // pageCount must be strictly less than t->size, so this is non-negative.

      boundary_tag succ_tag;
      int ret = readBoundaryTag(xid, page + t.size + 1, &succ_tag);
      assert(ret);
      succ_tag.prev_size = new_tag.size;
      TsetBoundaryTag(xid, page + t.size + 1, &succ_tag);

    } else { 

      new_tag.size = PAGEID_T_MAX;

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

  TsetBoundaryTag(xid, page, &t);

}

static void consolidateRegions(int xid, pageid_t * firstPage, boundary_tag  *t) { 

  if(t->status != REGION_VACANT || TisActiveTransaction(t->region_xid)) { return; }

  //  (*firstPage)++;

  int mustWriteOriginalTag = 0;

  // If successor is vacant, merge.
  if(t->size != PAGEID_T_MAX) { // is there a successor?
    pageid_t succ_page = (*firstPage) + 1 + t->size;
    boundary_tag succ_tag;
    int ret = readBoundaryTag(xid, succ_page, &succ_tag);
    assert(ret);

    // TODO: Check stasis_page_type_ptr()...

    if(succ_tag.size == PAGEID_T_MAX) { 
      t->size = PAGEID_T_MAX;
      assert(succ_tag.status == REGION_VACANT);
      // TODO: Truncate page file.
      TdeallocBoundaryTag(xid, succ_page);
      mustWriteOriginalTag = 1;
    } else if(succ_tag.status == REGION_VACANT && (!TisActiveTransaction(succ_tag.region_xid))) {

      t->size = t->size + succ_tag.size + 1;
      pageid_t succ_succ_page = succ_page + succ_tag.size + 1;

      boundary_tag succ_succ_tag;

      ret = readBoundaryTag(xid, succ_succ_page, &succ_succ_tag);
      assert(ret);
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

  if(t->prev_size != PAGEID_T_MAX) { 
    
    pageid_t pred_page = ((*firstPage) - 1) - t->prev_size;  // If the predecessor is length zero, then it's boundary tag is two pages before this region's tag.
    
    boundary_tag pred_tag;
    int ret = readBoundaryTag(xid, pred_page, &pred_tag);
    assert(ret);
    
    if(pred_tag.status == REGION_VACANT && (!TisActiveTransaction(pred_tag.region_xid))) { 
      
      TdeallocBoundaryTag(xid, *firstPage);
      
      if(t->size == PAGEID_T_MAX) { 
	pred_tag.size = PAGEID_T_MAX;
	
	// TODO: truncate region
	
      } else { 
	
	pred_tag.size += (t->size + 1);
	
	pageid_t succ_page = (*firstPage) + 1+ t->size;
	assert(pred_page + pred_tag.size + 1 == succ_page);
	
	boundary_tag succ_tag;
	ret = readBoundaryTag(xid, succ_page, &succ_tag);
	assert(ret);
	succ_tag.prev_size = pred_tag.size;
	TsetBoundaryTag(xid, succ_page, &succ_tag);

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

void TregionDealloc(int xid, pageid_t firstPage) {

  // Note that firstPage is the first *caller visible* page in the
  // region.  The boundary tag is stored on firstPage - 1.  Also, note
  // that a region of size N takes up N+1 pages on disk.

  // Deferred coalescing would probably make sense...

  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();

  boundary_tag t;
  int ret = readBoundaryTag(xid, firstPage-1, &t);
  assert(ret);

  regionAllocArg arg = { firstPage-1, t.size, t.allocation_manager };

  assert(t.status != REGION_VACANT); 

  void * handle = TbeginNestedTopAction(xid, OPERATION_DEALLOC_REGION, (const byte*)&arg, sizeof(regionAllocArg));

  operate_dealloc_region_unlocked(xid, &arg);

  firstPage --;

  TendNestedTopAction(xid, handle);
  
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
}

pageid_t TregionAlloc(int xid, pageid_t pageCount, int allocationManager) { 
  // Initial implementation.  Naive first fit.

  pthread_mutex_lock(&region_mutex);
  assert(0 == holding_mutex);
  holding_mutex = pthread_self();

  void * ntaHandle = TbeginNestedTopAction(xid, OPERATION_NOOP, 0, 0);

  pageid_t pageid = 0;
  boundary_tag t;

  int ret = readBoundaryTag(xid, pageid, &t); // XXX need to check if there is a boundary tag there or not!
  assert(ret);
  //  printf("consolidateRegions pageid, t: %d, {%d, %d, %d} -> ", pageid, t.size, t.prev_size, t.status);

  consolidateRegions(xid, &pageid, &t);
  
  //  printf(" %d, {%d, %d, %d}\tpageCount=%d\n", pageid, t.size, t.prev_size, t.status, pageCount);

  while(t.status != REGION_VACANT || t.size < pageCount || TisActiveTransaction(t.region_xid)) { 
    // TODO: This while loop and the boundary tag manipulation below should be factored into two submodules.

    //    printf("t.status = %d, REGION_VACANT = %d, t.size = %d, pageCount = %d\n", t.status, REGION_VACANT, t.size, pageCount);
    pageid += ( t.size + 1 );
    ret = readBoundaryTag(xid, pageid, &t);
    assert(ret);

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


stasis_operation_impl stasis_op_impl_boundary_tag_alloc() {
  stasis_operation_impl o = { 
    OPERATION_ALLOC_BOUNDARY_TAG, 
    OPERATION_ALLOC_BOUNDARY_TAG, 
    OPERATION_NOOP,
    op_alloc_boundary_tag
  };
  return o;
}

stasis_operation_impl stasis_op_impl_region_alloc() { 
  stasis_operation_impl o = { 
    OPERATION_ALLOC_REGION,
    OPERATION_NOOP,
    OPERATION_ALLOC_REGION_INVERSE,
    noop
  };
  return o;
}
stasis_operation_impl stasis_op_impl_region_alloc_inverse() { 
  stasis_operation_impl o = { 
    OPERATION_ALLOC_REGION_INVERSE,
    OPERATION_ALLOC_REGION_INVERSE,
    OPERATION_INVALID,
    op_dealloc_region
  };
  return o;
}
stasis_operation_impl stasis_op_impl_region_dealloc() { 
  stasis_operation_impl o = { 
    OPERATION_DEALLOC_REGION,
    OPERATION_NOOP,
    OPERATION_DEALLOC_REGION_INVERSE,
    noop
  };
  return o;
}

stasis_operation_impl stasis_op_impl_region_dealloc_inverse() { 
  stasis_operation_impl o = { 
    OPERATION_DEALLOC_REGION_INVERSE,
    OPERATION_DEALLOC_REGION_INVERSE,
    OPERATION_INVALID,
    op_alloc_region
  };
  return o;
}

void TregionFindNthActive(int xid, pageid_t regionNumber, pageid_t * firstPage, pageid_t * size) { 
  boundary_tag t;
  recordid rid = {0, 0, sizeof(boundary_tag)};
  pthread_mutex_lock(&region_mutex);
  holding_mutex = pthread_self();
  Tread(xid, rid, &t);
  pageid_t prevSize = 0;
  while(t.status == REGION_VACANT) { 
    rid.page += (t.size + 1);
    Tread(xid, rid, &t);
    assert(t.size != PAGEID_T_MAX);
    assert(t.prev_size != PAGEID_T_MAX); 
    assert(prevSize == t.prev_size || !prevSize);
    prevSize = t.size;
  }
  for(pageid_t i = 0; i < regionNumber; i++) { 
    rid.page += (t.size + 1);
    Tread(xid, rid, &t);
    if(t.status == REGION_VACANT) { i--; }
    assert(t.size != PAGEID_T_MAX);
    assert(t.prev_size != PAGEID_T_MAX || i == 0);
    assert(prevSize == t.prev_size || !prevSize);
    prevSize = t.size;
  }
  *firstPage = rid.page+1;
  *size = t.size;
  holding_mutex = 0;
  pthread_mutex_unlock(&region_mutex);
}
