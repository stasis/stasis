#include <stasis/common.h>
#include <stasis/operations/arrayList.h>
#include <stasis/bufferManager.h>
#include <stasis/transactional.h>
#include <stasis/page/fixed.h>
#include <assert.h>
#include <math.h>

BEGIN_C_DECLS

#define MAX_OFFSET_POSITION    3
#define FIRST_DATA_PAGE_OFFSET 4

typedef struct {
  pageid_t firstPage;
  pageid_t initialSize;
  pageid_t multiplier;//XXX rest are not page numbers or offsets, but must all be same length
  pageid_t size;      // *has* to be smaller than a page; passed into TinitializeFixedPage()
  pageid_t maxOffset;
} array_list_parameter_t;

static array_list_parameter_t array_list_read_parameter(int xid, Page * p) {

  array_list_parameter_t alp;
  alp.firstPage = p->id;
  /*  tlp.maxOffset   = *(int*)fixed_record_ptr(p, 3); */
  recordid rid = { p->id, 0, sizeof(pageid_t) };
  alp.initialSize = *(pageid_t*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 1;
  alp.multiplier = *(pageid_t*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 2;
  alp.size = *(pageid_t*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 3;
  alp.maxOffset = *(pageid_t*)stasis_record_read_begin(xid, p, rid);

  return alp;
}

static int array_list_get_block_containing_offset(array_list_parameter_t alp, int offset, pageid_t * firstSlotInBlock) {
  int rec_per_page = stasis_page_fixed_records_per_page((size_t)alp.size);
  long thisHigh = rec_per_page * alp.initialSize;
  int lastHigh = 0;
  int pageRidSlot = 0;
  int currentPageLength = alp.initialSize;

  while(((pageid_t)offset) >= thisHigh) {
    pageRidSlot ++;
    lastHigh = thisHigh;
    currentPageLength *= alp.multiplier;
    thisHigh += rec_per_page * currentPageLength;
  }
  if(firstSlotInBlock) {
    *firstSlotInBlock = lastHigh;
  }
  return pageRidSlot;
}

static int array_list_op_init_header(const LogEntry* e, Page* p) {

  assert(e->update.arg_size == sizeof(array_list_parameter_t));

  const array_list_parameter_t * alp
    = (const array_list_parameter_t *)stasis_log_entry_update_args_cptr(e);

  stasis_page_fixed_initialize_page(p, sizeof(pageid_t),
                               stasis_page_fixed_records_per_page(sizeof(pageid_t)));

  recordid initialSizeRid, multiplierRid, slotSizeRid, maxOffsetRid, firstDataPageRid;

  initialSizeRid.page
    = multiplierRid.page = slotSizeRid.page
    = maxOffsetRid.page = firstDataPageRid.page = p->id;

  initialSizeRid.size
    = multiplierRid.size = slotSizeRid.size
    = maxOffsetRid.size = firstDataPageRid.size = sizeof(pageid_t);

  initialSizeRid.slot = 0;
  multiplierRid.slot = 1;
  slotSizeRid.slot = 2;
  maxOffsetRid.slot = 3;
  // Note that firstDataPageRid is not part of the header page's header..
  // Instead, it is the value stored on the first slot of the header page.
  firstDataPageRid.slot = 4;

  // Write header.
  stasis_record_write(e->xid, p, initialSizeRid, (const byte*)&(alp->initialSize));
  stasis_record_write(e->xid, p, multiplierRid, (const byte*)&(alp->multiplier));
  stasis_record_write(e->xid, p, slotSizeRid, (const byte*)&(alp->size));
  stasis_record_write(e->xid, p, maxOffsetRid, (const byte*)&(alp->maxOffset));

  // Write first slot.  The page after this one stores data
  pageid_t firstDataPage = alp->firstPage + 1;
  stasis_record_write(e->xid, p, firstDataPageRid, (const byte*)&firstDataPage);


  p->pageType = ARRAY_LIST_PAGE;

  return 0;
}

/*----------------------------------------------------------------------------*/

stasis_operation_impl stasis_op_impl_array_list_header_init(void) {
  stasis_operation_impl o = {
    OPERATION_ARRAY_LIST_HEADER_INIT,
    UNINITIALIZED_PAGE,
    OPERATION_ARRAY_LIST_HEADER_INIT,
    /* Do not need to roll back this page, since it will be deallocated. */
    OPERATION_NOOP,
    &array_list_op_init_header
  };
  return o;
}

recordid stasis_array_list_dereference_recordid(int xid, Page * p, int offset) {
  readlock(p->rwlatch,0);
  array_list_parameter_t tlp = array_list_read_parameter(xid, p);

  int rec_per_page = stasis_page_fixed_records_per_page((size_t)tlp.size);
  pageid_t lastHigh = 0;
  int pageRidSlot = 0; /* The slot on the root arrayList page that contains the first page of the block of interest */

  assert(tlp.maxOffset >= offset);

  pageRidSlot = array_list_get_block_containing_offset(tlp, offset, &lastHigh);

  int dataSlot = offset - lastHigh;  /* The offset in the block of interest of the slot we want. */
  pageid_t blockPage = dataSlot / rec_per_page;  /* The page in the block of interest that contains the slot we want */
  int blockSlot = dataSlot - blockPage * rec_per_page;

  pageid_t thePage;

  recordid rid = { p->id, pageRidSlot + FIRST_DATA_PAGE_OFFSET, sizeof(pageid_t) };
  thePage = *(int*)stasis_record_read_begin(xid,p,rid);
  unlock(p->rwlatch);

  rid.page = thePage + blockPage;
  rid.slot = blockSlot;
  rid.size = tlp.size;

  return rid;

}

/*----------------------------------------------------------------------------*/
recordid TarrayListAlloc(int xid, pageid_t count, int multiplier, int recordSize) {

  pageid_t firstPage;
  firstPage = TpageAllocMany(xid, count+1);

  if(firstPage == INVALID_PAGE) { return NULLRID; }

  array_list_parameter_t alp;

  alp.firstPage = firstPage;
  alp.initialSize = count;
  alp.multiplier = multiplier;
  alp.size = recordSize;
  alp.maxOffset = -1;

  recordid rid;

  rid.page = firstPage;
  rid.slot = 0; /* number of slots in array (maxOffset + 1) */
  rid.size = recordSize;
  Tupdate(xid, firstPage, &alp, sizeof(alp), OPERATION_ARRAY_LIST_HEADER_INIT);
#ifdef ARRAY_LIST_OLD_ALLOC
  for(pageid_t i = 0; i < count; i++) {
    TinitializeFixedPage(xid, firstPage+1+i, alp.size);
  }
#else
  TinitializeFixedPageRange(xid, firstPage+1, count, alp.size);
#endif

  return rid;
}

void TarrayListDealloc(int xid, recordid rid) {
  Page * p = loadPage(xid, rid.page);
  array_list_parameter_t alp = array_list_read_parameter(xid, p);
  slotid_t n = array_list_get_block_containing_offset(alp, alp.maxOffset, NULL) + 1;
  rid.size = sizeof(pageid_t); // The rid we pass to callers has its size set to the size of the user data
                               // We want the size of internal records.
  for(slotid_t i = 1; i < n; i++) {  // block 0 points to p->id + 1.
    pageid_t pid;
    rid.slot = i + FIRST_DATA_PAGE_OFFSET;
    stasis_record_read(xid, p, rid, (byte*)&pid);
    TpageDeallocMany(xid, pid);
  }
  releasePage(p);
  TpageDeallocMany(xid, rid.page);
}

/** @todo locking for arrayList... this isn't pressing since currently
    the only thing that calls arraylist (the hashtable
    implementations) serializes bucket list operations anyway...

    @todo this function calls pow(), which is horribly inefficient.
*/
int TarrayListExtend(int xid, recordid rid, int slots) {
  Page * p = loadPage(xid, rid.page);
  readlock(p->rwlatch, 0);
  array_list_parameter_t alp
    = array_list_read_parameter(xid, p);
  unlock(p->rwlatch);;
  releasePage(p);
  p = NULL;

  int lastCurrentBlock; // just a slot on a page
  if(alp.maxOffset == -1) {
    lastCurrentBlock = 0;
  } else{
    lastCurrentBlock = array_list_get_block_containing_offset(alp, alp.maxOffset, NULL);
  }
  int lastNewBlock = array_list_get_block_containing_offset(alp, alp.maxOffset+slots, NULL);

  DEBUG("lastCurrentBlock = %d, lastNewBlock = %d\n", lastCurrentBlock, lastNewBlock);

  recordid tmp;   /* recordid of slot in base page that holds new block. */
  tmp.page = rid.page;
  tmp.size = sizeof(pageid_t);

  recordid tmp2;  /* recordid of newly created pages. */
  tmp2.slot = 0;
  tmp2.size = alp.size;
  /* Iterate over the (small number) of indirection blocks that need to be updated */

  for(pageid_t i = lastCurrentBlock+1; i <= lastNewBlock; i++) {
    /* Alloc block i */
#ifdef HAVE_POWL
    pageid_t blockSize = alp.initialSize * powl(alp.multiplier, i);
#else
    pageid_t blockSize = alp.initialSize * powf(alp.multiplier, i);
#endif
    pageid_t newFirstPage = TpageAllocMany(xid, blockSize);
    DEBUG("block %lld %lld %lld\n", (long long)i, (long long)newFirstPage, (long long)blockSize);
    tmp.slot = i + FIRST_DATA_PAGE_OFFSET;
    /* Iterate over the (large number) of new blocks, clearing their contents */
#ifdef ARRAY_LIST_OLD_ALLOC
    // old way
    {
      for(pageid_t i = newFirstPage; i < newFirstPage + blockSize; i++) {
        TinitializeFixedPage(xid, i, alp.size);
      }
    }
#else
    TinitializeFixedPageRange(xid, newFirstPage, blockSize, alp.size);
#endif
    TsetRaw(xid,tmp,&newFirstPage);
    DEBUG("Tset: {%d, %d, %d} = %d\n", tmp.page, tmp.slot, tmp.size, newFirstPage);
  }

  tmp.slot = MAX_OFFSET_POSITION;

  pageid_t newMaxOffset = alp.maxOffset+slots;
  TsetRaw(xid, tmp, &newMaxOffset);

  return 0;

}

int TarrayListLength(int xid, recordid rid) {
 Page * p = loadPage(xid, rid.page);
 readlock(p->rwlatch, 0);
 array_list_parameter_t alp
   = array_list_read_parameter(xid, p);
 unlock(p->rwlatch);
 releasePage(p);
 return alp.maxOffset+1;
}
END_C_DECLS
