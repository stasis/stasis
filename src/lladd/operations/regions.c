#include "../page.h"
#include <lladd/operations.h>
#include "../page/slotted.h"
#include <assert.h>

#define INVALID_XID (-1)

typedef struct regionAllocLogArg{
  int startPage;
  unsigned int pageCount;
  int allocationManager;
} regionAllocArg;

#define boundary_tag_ptr(p) (((byte*)end_of_usable_space_ptr((p)))-sizeof(boundary_tag_t))

pthread_mutex_t region_mutex = PTHREAD_MUTEX_INITIALIZER;


static void TregionAllocHelper(int xid, unsigned int pageid, unsigned int pageCount, int allocationManager);

static int operate_alloc_boundary_tag(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) { 
  slottedPageInitialize(p);
  *page_type_ptr(p) = BOUNDARY_TAG_PAGE;
  slottedPostRalloc(xid, p, lsn, rid);
  slottedWrite(xid, p, lsn, rid, dat);
  return 0;
}

static int operate_alloc_region(int xid, Page * p, lsn_t lsn, recordid rid, const void * datP) { 
  pthread_mutex_lock(&region_mutex);
  assert(!p);
  regionAllocArg *dat = (regionAllocArg*)datP;
  TregionAllocHelper(xid, dat->startPage, dat->pageCount, dat->allocationManager);
  pthread_mutex_unlock(&region_mutex);
  return 0;
}

static int operate_dealloc_region(int xid, Page * p, lsn_t lsn, recordid rid, const void * datP) { 
  regionAllocArg *dat = (regionAllocArg*)datP;
  assert(!p);
  TregionDealloc(xid, dat->startPage+1);
  return 0;
}

// TODO: Implement these four functions.
static void TallocBoundaryTag(int xid, unsigned int page, boundary_tag* tag) {
  //  printf("Alloc boundary tag at %d\n", page);
  recordid rid = {page, 0, sizeof(boundary_tag)};
  Tupdate(xid, rid, tag, OPERATION_ALLOC_BOUNDARY_TAG);
}
static void TdeallocBoundaryTag(int xid, unsigned int page) {
  //no-op
}
 
static void TreadBoundaryTag(int xid, unsigned int page, boundary_tag* tag) { 
  recordid rid = { page, 0, sizeof(boundary_tag) };
  Tread(xid, rid, tag);
}
static void TsetBoundaryTag(int xid, unsigned int page, boundary_tag* tag) { 
  //  printf("Writing boundary tag at %d\n", page);
  recordid rid = { page, 0, sizeof(boundary_tag) };
  Tset(xid, rid, tag);
}

void regionsInit() { 
  Page * p = loadPage(-1, 0);
  int pageType = *page_type_ptr(p);
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
  releasePage(p);
}

static void TregionAllocHelper(int xid, unsigned int pageid, unsigned int pageCount, int allocationManager) {
  boundary_tag t;
  TreadBoundaryTag(xid, pageid, &t);

  if(t.size != pageCount) { 
    // need to split region
    // allocate new boundary tag.

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

unsigned int TregionAlloc(int xid, unsigned int pageCount, int allocationManager) { 
  // Initial implementation.  Naive first fit.

  
  pthread_mutex_lock(&region_mutex);

  unsigned int pageid = 0;
  boundary_tag t;
  unsigned int prev_size = UINT32_MAX;

  TreadBoundaryTag(xid, pageid, &t); // XXX need to check if there is a boundary tag there or not!

  while(t.status != REGION_VACANT || t.size < pageCount) { // TODO: This while loop and the boundary tag manipulation below should be factored into two submodules.
    //    printf("t.status = %d, REGION_VACANT = %d, t.size = %d, pageCount = %d\n", t.status, REGION_VACANT, t.size, pageCount);
    assert(t.prev_size == prev_size);
    prev_size = t.size;
    pageid += ( t.size + 1 );
    TreadBoundaryTag(xid, pageid, &t);
  }
  //  printf("page = %d, t.status = %d, REGION_VACANT = %d, t.size = %d, pageCount = %d (alloced)\n", pageid, t.status, REGION_VACANT, t.size, pageCount);

  assert(t.prev_size == prev_size);

  regionAllocArg arg = { pageid, pageCount, allocationManager };

  void * ntaHandle = TbeginNestedTopAction(xid, OPERATION_ALLOC_REGION, (const byte*)&arg, sizeof(regionAllocArg));

  TregionAllocHelper(xid, pageid, pageCount, allocationManager);

  TendNestedTopAction(xid, ntaHandle);

  pthread_mutex_unlock(&region_mutex);
  
  return pageid+1;
}

void TregionDealloc(int xid, unsigned int firstPage) {

  // Note that firstPage is the first *caller visible* page in the
  // region.  The boundary tag is stored on firstPage - 1.  Also, note
  // that a region of size N takes up N+1 pages on disk.

  // Deferred coalescing would probably make sense...

  pthread_mutex_lock(&region_mutex);

  boundary_tag t;
  TreadBoundaryTag(xid, firstPage - 1, &t);

  assert(t.status != REGION_VACANT);
  t.status = REGION_VACANT;

  // If successor is vacant, merge.
  if(t.size != UINT32_MAX) {  // is there a successor?
    unsigned int succ_page = firstPage + t.size;
    boundary_tag succ_tag;
    TreadBoundaryTag(xid, succ_page, &succ_tag);

    // TODO: Check page_type_ptr()...
    if(succ_tag.size == UINT32_MAX) { 
      t.size = UINT32_MAX;

      // TODO: Truncate page file.
      TdeallocBoundaryTag(xid, succ_page);

    } else if(succ_tag.status == REGION_VACANT) { 

      t.size = t.size + succ_tag.size + 1;
      unsigned int succ_succ_page = succ_page + succ_tag.size + 1;

      boundary_tag succ_succ_tag;

      TreadBoundaryTag(xid, succ_succ_page, &succ_succ_tag);
      succ_succ_tag.prev_size = t.size;
      TsetBoundaryTag(xid, succ_succ_page, &succ_succ_tag);
      
      TsetBoundaryTag(xid, succ_page, &succ_tag);
      
    }
  }

  // If predecessor is vacant, merge.  (Doing this after the successor
  // is merged makes life easier, since merging with the predecessor
  // creates a situation where the current page is not a boundary
  // tag...)

  if(t.prev_size != UINT32_MAX) { 

    unsigned int pred_page = (firstPage - 2) - t.prev_size;  // If the predecessor is length zero, then it's boundary tag is two pages before this region's tag.
    
    boundary_tag pred_tag;
    TreadBoundaryTag(xid, pred_page, &pred_tag);
    
    if(pred_tag.status == REGION_VACANT) {
      if(t.size == UINT32_MAX) { 
	pred_tag.size = UINT32_MAX;
	
	// TODO: truncate region
	
      } else { 
	
	pred_tag.size += (t.size + 1);
	
	unsigned int succ_page = firstPage + t.size;
	assert(pred_page + pred_tag.size + 1 == succ_page);

	boundary_tag succ_tag;
	TreadBoundaryTag(xid, succ_page, &succ_tag);
	succ_tag.prev_size = pred_tag.size;
	TsetBoundaryTag(xid, succ_page, &succ_tag);
	
	assert(succ_tag.status != REGION_VACANT);
	assert(succ_page - pred_page - 1 == pred_tag.size);
      }
      
      TsetBoundaryTag(xid, pred_page, &pred_tag);
      TdeallocBoundaryTag(xid, firstPage -1);
      
    } else { 
      TsetBoundaryTag(xid, firstPage - 1, &t);
    }
  } else { 
    TsetBoundaryTag(xid, firstPage - 1, &t);
  }
  pthread_mutex_unlock(&region_mutex);
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
}

/*Operation getAllocRegion() { 
  

}

Operation getFreeRegion() { 

}*/
