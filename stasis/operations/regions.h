/**
   Allocates and deallocates regions of pages.  The page before each
   region is of type BOUNDARY_TAG.  All regions except the last one in
   the page file have a BOUNDARY_TAG page immediately after the end of
   the region.

   Each region is managed by an allocation manager, which manages the
   allocation of pages within a region.  The contents of pages within
   a newly allocated region are undefined.
*/

typedef struct boundary_tag { 
  unsigned int size;
  unsigned int prev_size;
  int status;
  int region_xid;
  int allocation_manager;
} boundary_tag;

#define REGION_BASE      (123)
#define REGION_VACANT    (REGION_BASE + 0)
#define REGION_ZONED     (REGION_BASE + 1)
#define REGION_OCCUPIED  (REGION_BASE + 2)
#define REGION_CONDEMNED (REGION_BASE + 3)

void regionsInit();

unsigned int TregionAlloc(int xid, unsigned int pageCount, int allocaionManager);
void TregionDealloc(int xid, unsigned int firstPage);
unsigned int TregionSize(int xid, unsigned int firstPage);

/** Currently, this function is O(n) in the number of regions, so be careful! */
void TregionFindNthActive(int xid, unsigned int n, unsigned int * firstPage, unsigned int * size);

Operation getAllocBoundaryTag();

Operation getAllocRegion();
Operation getDeallocRegion();

/** This function checks the regions in the page file for consistency.
    It makes sure that the doublly linked list is consistent (eg
    this->next->prev == this), and it makes sure that all boundary
    tags pages (that are marked REGION_ZONED) occur somewhere in the
    linked list. */

void fsckRegions(int xid);

// XXX need callbacks to handle transaction commit/abort.
