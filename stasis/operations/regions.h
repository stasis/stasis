#ifndef STASIS_OPERATIONS_REGIONS_H
#define STASIS_OPERATIONS_REGIONS_H
#include <stasis/operations.h>
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
  pageid_t size;
  pageid_t prev_size;
  int status;
  int region_xid;
  int allocation_manager;
} boundary_tag;

#define REGION_BASE      (123)
#define REGION_VACANT    (REGION_BASE + 0)
#define REGION_ZONED     (REGION_BASE + 1)
#define REGION_OCCUPIED  (REGION_BASE + 2)
#define REGION_CONDEMNED (REGION_BASE + 3)

void regionsInit(stasis_log_t *log);

pageid_t TregionAlloc(int xid, pageid_t pageCount, int allocaionManager);
void TregionDealloc(int xid, pageid_t firstPage);
unsigned int TregionSize(int xid, pageid_t firstPage);

/** Currently, this function is O(n) in the number of regions, so be careful! */
void TregionFindNthActive(int xid, pageid_t n, pageid_t * firstPage, pageid_t * size);

int TregionNextBoundaryTag(int xid, pageid_t*pid, boundary_tag *tag, int allocationManager);
int TregionReadBoundaryTag(int xid, pageid_t pid, boundary_tag *tag);

stasis_operation_impl stasis_op_impl_boundary_tag_alloc();

stasis_operation_impl stasis_op_impl_region_alloc();
stasis_operation_impl stasis_op_impl_region_alloc_inverse();
stasis_operation_impl stasis_op_impl_region_dealloc();
stasis_operation_impl stasis_op_impl_region_dealloc_inverse();

/** This function checks the regions in the page file for consistency.
    It makes sure that the doublly linked list is consistent (eg
    this->next->prev == this), and it makes sure that all boundary
    tags pages (that are marked REGION_ZONED) occur somewhere in the
    linked list. */

void fsckRegions(int xid);

// XXX need callbacks to handle transaction commit/abort.
#endif
