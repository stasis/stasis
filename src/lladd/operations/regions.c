#include "../page.h"
#include <lladd/operations.h>
/*
#define REGION_BASE      (123)
#define REGION_VACANT    (REGION_BASE + 0)
#define REGION_ZONED     (REGION_BASE + 1)
#define REGION_OCCUPIED  (REGION_BASE + 2)
#define REGION_CONDEMNED (REGION_BASE + 3)

#define boundary_tag_ptr(p) (((byte*)end_of_usable_space_ptr((p)))-sizeof(boundary_tag_t))

typedef struct boundary_tag_t { 
  int size;
  int prev_size;
  int status;
  int region_xid;
  int allocation_manager;
} boundary_tag_t;


void boundaryTagInit(Page * p) { 
  *page_type_ptr(p) = LLADD_BOUNDARY_TAG;
  boundary_tag_t * tag = boundary_tag_ptr(p);
  tag.size = INT32_MAX;
  tag.prev_size = -1;
  tag.status = REGION_VACANT;
  tag.region_xid = INVALID_XID;
  tag.allocation_manager = 0;
}

regionsInit() { 
  Page * p = loadPage(0);
  if(*page_type_ptr(p) != LLADD_BOUNDARY_TAG) {
    assert(*page_type_ptr(p) == 0);
    boundaryTagInit(p);
  }
  releasePage(p);
}

pthread_mutex_t region_mutex = PTHREAD_MUTEX_INITIALIZER;

int TregionAlloc(int xid, int pageCount, int allocationManager) { 
  // Initial implementation.  Naive first fit.
  pthread_mutex_lock(&region_mutex);
  int ret = -1;
  Page * p = loadPage(0);
  boundary_tag_t * t = boundary_tag_ptr(p);
  while(t.status != REGION_VACANT || t.size < pageCount) { // XXX This while loop and the boundary tag manipulation below should be factored into two submodules.
    int nextPage = p->id + t.size;
    releasePage(p);
    p = loadPage(nextPage);
    t = boundary_tag_ptr(p);
  }
  t->status = REGION_ZONED;
  t->region_xid = xid;
  t->allocation_manager = allocationManager;
  if(t->size != pageCount) { 
    // need to split region

    if(t.size != INT_MAX) {

      // allocate new boundary tag.
      int newRegionSize = t->size - pageCount - 1; // pageCount must be strictly less than t->size, so this is safe.
      Page * new_tag = loadPage(p->id + pageCount + 1);
      boundaryTagInit(p);
      boundary_tag_ptr(p)->size = newRegionSize;
      boundary_tag_ptr(p)->prev_size = pageCount;
      boundary_tag_ptr(p)->status = REGION_EPHEMERAL; // region disappears if transaction aborts; is VACANT if it succeeds.  GET RID OF EPHEMERAL; just make it vacant, and merge on abort.
      boundary_tag_ptr(p)->region_xid = xid;
      boundary_tag_ptr(p)->allocation_manager = 0;
      releasePage(new_tag);

      Page * next = loadPage(p->id + t.size + 1);
      boundary_tag_ptr(next)->prev_size = newRegionSize;
      releasePage(next);
    } else { 
      Page * new_tag = loadPage(p->id + pageCount + 1);
      boundaryTagInit(p);
      boundary_tag_ptr(p)->size = INT_MAX;
      boundary_tag_ptr(p)->prev_size = pageCount;
      boundary_tag_ptr(p)->status = REGION_EPHEMERAL;
      boundary_tag_ptr(p)->region_xid = xid;
      boundary_tag_ptr(p)->allocation_manager = 0;
    }

  }
  releasePage(p);

  pthread_mutex_unlock(&region_mutex);
}

void TregionFree(int xid, int firstPage) {

}

int TregionSize(int xid, int firstPage) { 

}

Operation getRegionAlloc() { 

}

Operation getRegionFree() { 

}
*/
