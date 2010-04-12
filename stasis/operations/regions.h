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
  stasis_transaction_fingerprint_t region_xid_fp;
  int allocation_manager;
} boundary_tag;

#define REGION_BASE      (123)
#define REGION_VACANT    (REGION_BASE + 0)
#define REGION_ZONED     (REGION_BASE + 1)
#define REGION_OCCUPIED  (REGION_BASE + 2)
#define REGION_CONDEMNED (REGION_BASE + 3)

void regionsInit(stasis_log_t *log);

pageid_t TregionAlloc(int xid, pageid_t pageCount, int allocationManager);
void TregionDealloc(int xid, pageid_t firstPage);
pageid_t TregionSize(int xid, pageid_t firstPage);
/**
   @param xid The transaction forcing the region to disk.  Perhaps
              some day, the force won't be guaranteed to finish until
              Tcommit(). Not currently used (this call is currently
              synchronous).
   @param bm  NULL, or the buffer manager associated with h.
   @param h   NULL, or a buffer manager handle that's recently been
              used to perform I/O against this region.  (If the handle
              is non-null, and has been set to optimize for sequential
	      writes, this call will tell linux to drop the pages from
	      cache once it's forced the data to disk.)
   @param pid The first page of the region to be forced.
 */
void TregionForce(int xid, stasis_buffer_manager_t* bm, stasis_buffer_manager_handle_t* h, pageid_t pid);
void TregionPrefetch(int xid, pageid_t firstPage);

/** Currently, this function is O(n) in the number of regions, so be careful! */
void TregionFindNthActive(int xid, pageid_t n, pageid_t * firstPage, pageid_t * size);
/**
 * Read the active boundary tag that follows the given region.
 * Inactive boundary tags (ones that are locked by ongoing transactions, or that point
 * to free space) will be skipped.
 *
 * @param xid The transaction reading the boundary tag
 * @param pid A pointer to the current pageid.
 * @param tag A pointer to a buffer that will hold the next boundary tag.
 * @param allocationManager The allocation manager whose tags should be returned, or 0 if all active tags should be returned.
 * @return 0 on failure, true on success
 */
int TregionNextBoundaryTag(int xid, pageid_t*pid, boundary_tag *tag, int allocationManager);
/** Read the boundary tag marking the first page of some region.  If pid = ROOT_RECORD.page, this will return the first boundary tag.
 *
 * @param xid The transaction examining the boundary tag
 * @param pid The first page in the region (ie: the one returned by TregionAlloc().  In the current implementation, the boundary tag lives on the page before this one.
 * @param tag A buffer that the tag will be read into.
 */
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
