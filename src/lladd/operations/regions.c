#include "../page.h"
#include <lladd/operations.h>
#include "../page/slotted.h"
#include <assert.h>
#define REGION_BASE      (123)
#define REGION_VACANT    (REGION_BASE + 0)
#define REGION_ZONED     (REGION_BASE + 1)
#define REGION_OCCUPIED  (REGION_BASE + 2)
#define REGION_CONDEMNED (REGION_BASE + 3)

#define INVALID_XID (-1)

#define boundary_tag_ptr(p) (((byte*)end_of_usable_space_ptr((p)))-sizeof(boundary_tag_t))

typedef struct boundary_tag { 
  int size;
  int prev_size;
  int status;
  int region_xid;
  int allocation_manager;
} boundary_tag;

static int operate_alloc_boundary_tag(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) { 
  slottedPageInitialize(p);
  *page_type_ptr(p) = BOUNDARY_TAG_PAGE;
  slottedPostRalloc(xid, p, lsn, rid);
  slottedWrite(xid, p, lsn, rid, dat);
  return 0;
}

// TODO: Implement these four functions.
static void TallocBoundaryTag(int xid, int page, boundary_tag* tag) {
  recordid rid = {page, 0, sizeof(boundary_tag)};
  Tupdate(xid, rid, tag, OPERATION_ALLOC_BOUNDARY_TAG);
}
static void TdeallocBoundaryTag(int xid, int page) {
  // no-op
}
 
static void TreadBoundaryTag(int xid, int page, boundary_tag* tag) { 
  recordid rid = { page, 0, sizeof(boundary_tag) };
  Tread(xid, rid, tag);
}
static void TsetBoundaryTag(int xid, int page, boundary_tag* tag) { 
  recordid rid = { page, 0, sizeof(boundary_tag) };
  Tset(xid, rid, tag);
}

void regionsInit() { 
  Page * p = loadPage(-1, 0);
  int pageType = *page_type_ptr(p);
  releasePage(p);
  if(pageType != BOUNDARY_TAG_PAGE) {
    boundary_tag t;
    t.size = INT32_MAX;
    t.prev_size = INT32_MAX;
    t.status = REGION_VACANT;
    t.region_xid = INVALID_XID;
    t.allocation_manager = 0;
    TallocBoundaryTag(-1, 0, &t);
  }
}

pthread_mutex_t region_mutex = PTHREAD_MUTEX_INITIALIZER;

int TregionAlloc(int xid, int pageCount, int allocationManager) { 
  // Initial implementation.  Naive first fit.

  pthread_mutex_lock(&region_mutex);

  int pageid = 0;
  boundary_tag t;
  int prev_size = INT32_MAX;


  TreadBoundaryTag(xid, pageid, &t); // XXX need to check if there is a boundary tag there or not!

  while(t.status != REGION_VACANT || t.size < pageCount) { // TODO: This while loop and the boundary tag manipulation below should be factored into two submodules.
    prev_size = t.size;
    pageid += ( t.size + 1 );
    TreadBoundaryTag(xid, pageid, &t);
  }

  t.status = REGION_ZONED;
  t.region_xid = xid;
  t.allocation_manager = allocationManager;
  assert(t.prev_size = prev_size);
  if(t.size != pageCount) { 
    // need to split region

    // allocate new boundary tag.

    int newPageid = pageid + pageCount + 1;
    boundary_tag new_tag;
    
    if(t.size != INT32_MAX) {

      new_tag.size = t.size - pageCount - 1; // pageCount must be strictly less than t->size, so this is non-negative.

      boundary_tag succ_tag;

      TreadBoundaryTag(xid, pageid + t.size + 1, &succ_tag);
      succ_tag.prev_size = pageCount;
      TsetBoundaryTag(xid, pageid + t.size + 1, &succ_tag);

    } else { 

      new_tag.size = INT32_MAX;

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

  TsetBoundaryTag(xid, pageid, &t);

  pthread_mutex_unlock(&region_mutex);
  
  return pageid;
}

void TregionFree(int xid, int firstPage) {

  // Note that firstPage is the first *caller visible* page in the
  // region.  The boundary tag is stored on firstPage - 1.  Also, note
  // that a region of size N takes up N+1 pages on disk.

  // Deferred coalescing would probably make sense...

  pthread_mutex_lock(&region_mutex);

  boundary_tag t;
  TreadBoundaryTag(xid, firstPage - 1, &t);

  // If successor is vacant, merge.
  if(t.size != INT32_MAX) {  // is there a successor?
    int succ_page = firstPage + t.size;
    boundary_tag succ_tag;
    TreadBoundaryTag(xid, succ_page, &succ_tag);

    // TODO: Check page_type_ptr()...
    if(succ_tag.size == INT32_MAX) { 
      t.size = INT32_MAX;

      // TODO: Truncate page file.
      TdeallocBoundaryTag(xid, succ_page);

    } else if(succ_tag.status == REGION_VACANT) { 

      t.size = t.size + succ_tag.size + 1;
      int succ_succ_page = succ_page + succ_tag.size + 1;

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

  if(t.prev_size != INT32_MAX) { 

    int pred_page = (firstPage - 2) - t.prev_size;  // If the predecessor is length zero, then it's boundary tag is two pages before this region's tag.
    
    boundary_tag pred_tag;
    TreadBoundaryTag(xid, pred_page, &pred_tag);
    
    if(pred_tag.status == REGION_VACANT) {
      if(t.size == INT32_MAX) { 
	pred_tag.size = INT32_MAX;
	
	// TODO: truncate region
	
      } else { 
	
	pred_tag.size += (t.size + 1);
	
	int succ_page = firstPage + t.size;
	
	boundary_tag succ_tag;
	TreadBoundaryTag(xid, succ_page, &succ_tag);
	succ_tag.prev_size = pred_tag.size;
	TsetBoundaryTag(xid, succ_page, &succ_tag);
	
	assert(succ_tag.status != REGION_VACANT);
	assert(succ_page - pred_page == pred_tag.size);
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
    sizeof(int),
    OPERATION_NOOP,
    &operate_alloc_boundary_tag
  };
  return o;
}

/*Operation getAllocRegion() { 
  

}

Operation getFreeRegion() { 

}*/
