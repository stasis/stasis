
#include <config.h>
#include <stasis/common.h>

#include <stasis/page.h>
#include <stasis/operations/pageOperations.h>
#include <stasis/operations/arrayList.h>
#include <stasis/transactional.h>
#include <stasis/bufferManager.h>

#include <assert.h>
#define _XOPEN_SOURCE 600
#include <math.h>

typedef struct {
  pageid_t firstPage;
  pageid_t initialSize;
  pageid_t multiplier;//XXX rest are not page numbers or offsets, but must all be same length
  int      size;      // *has* to be an int; passed into OPERATION_FIXED_PAGE_ALLOC
  pageid_t maxOffset;
} TarrayListParameters;


static TarrayListParameters pageToTLP(int xid, Page * p);
static int getBlockContainingOffset(TarrayListParameters tlp, int offset, pageid_t * firstSlotInBlock);

#define MAX_OFFSET_POSITION    3
#define FIRST_DATA_PAGE_OFFSET 4

/*----------------------------------------------------------------------------*/

compensated_function recordid TarrayListAlloc(int xid, pageid_t count, int multiplier, int size) {
  
  pageid_t firstPage;
  try_ret(NULLRID) {
    firstPage = TpageAllocMany(xid, count+1);
  } end_ret(NULLRID);
  TarrayListParameters tlp;

  tlp.firstPage = firstPage;
  tlp.initialSize = count;
  tlp.multiplier = multiplier;
  tlp.size = size;
  tlp.maxOffset = 0;

  recordid rid;

  rid.page = firstPage;
  rid.size = size;
  rid.slot = 0;
  try_ret(NULLRID) {
    Tupdate(xid, firstPage, &tlp, sizeof(tlp), OPERATION_ARRAY_LIST_HEADER_INIT);
  } end_ret(NULLRID);

  return rid;
}


static int op_array_list_header_init(const LogEntry* e, Page* p) {

  assert(e->update.arg_size == sizeof(TarrayListParameters));

  const TarrayListParameters * tlp
    = (const TarrayListParameters*)getUpdateArgs(e);

  pageid_t firstPage = tlp->firstPage;
  pageid_t count     = tlp->initialSize;
  pageid_t multiplier = tlp->multiplier;
  int size = tlp->size;

  stasis_fixed_initialize_page(p, sizeof(pageid_t),
			       stasis_fixed_records_per_page(sizeof(pageid_t)));

  recordid countRid, multiplierRid, slotSizeRid, maxOffset, firstDataPageRid;
  countRid.page
    = multiplierRid.page = slotSizeRid.page 
    = maxOffset.page = firstDataPageRid.page = p->id;

  countRid.size
    = multiplierRid.size = slotSizeRid.size
    = maxOffset.size = firstDataPageRid.size = sizeof(pageid_t);

  countRid.slot = 0;
  multiplierRid.slot = 1;
  slotSizeRid.slot = 2;
  maxOffset.slot = 3;
  firstDataPageRid.slot = 4;

  pageid_t firstDataPage = firstPage + 1;
  (*(pageid_t*)stasis_record_write_begin(e->xid, p, countRid))
    = count;
  (*(pageid_t*)stasis_record_write_begin(e->xid, p, multiplierRid))
    = multiplier;
  (*(pageid_t*)stasis_record_write_begin(e->xid, p, firstDataPageRid))
    = firstDataPage;
  (*(pageid_t*)stasis_record_write_begin(e->xid, p, slotSizeRid))
    = size;
  (*(pageid_t*)stasis_record_write_begin(e->xid, p, maxOffset))
    = -1;

  *stasis_page_type_ptr(p) = ARRAY_LIST_PAGE;

  recordid ret;
  ret.page = firstPage;
  ret.slot = 0;        /* slot = # of slots in array... */
  ret.size = size;

  return 0;
}

stasis_operation_impl stasis_op_impl_array_list_header_init() {
  stasis_operation_impl o = {
    OPERATION_ARRAY_LIST_HEADER_INIT,
    OPERATION_ARRAY_LIST_HEADER_INIT,
    /* Do not need to roll back this page, since it will be deallocated. */
    OPERATION_NOOP,
    &op_array_list_header_init
  };
  return o;
}
/*----------------------------------------------------------------------------*/

/** @todo locking for arrayList... this isn't pressing since currently
    the only thing that calls arraylist (the hashtable
    implementations) serialize bucket list operations anyway... 

    @todo this function calls pow(), which is horribly inefficient. 
*/

compensated_function int TarrayListExtend(int xid, recordid rid, int slots) {
  Page * p;
  try_ret(compensation_error()) {
    p = loadPage(xid, rid.page);
  } end_ret(compensation_error());
  readlock(p->rwlatch, 0);
  TarrayListParameters tlp = pageToTLP(xid, p);
  unlock(p->rwlatch);;
  releasePage(p);
  p = NULL;

  int lastCurrentBlock; // just a slot on a page
  if(tlp.maxOffset == -1) {
    lastCurrentBlock = -1;
  } else{
    lastCurrentBlock = getBlockContainingOffset(tlp, tlp.maxOffset, NULL);
  } 
  int lastNewBlock = getBlockContainingOffset(tlp, tlp.maxOffset+slots, NULL);

  DEBUG("lastCurrentBlock = %d, lastNewBlock = %d\n", lastCurrentBlock, lastNewBlock);

  recordid tmp;   /* recordid of slot in base page that holds new block. */
  tmp.page = rid.page;
  tmp.size = sizeof(pageid_t);
  
  recordid tmp2;  /* recordid of newly created pages. */
  tmp2.slot = 0;
  tmp2.size = tlp.size;
  /* Iterate over the (small number) of indirection blocks that need to be updated */
  try_ret(compensation_error()) {
    for(pageid_t i = lastCurrentBlock+1; i <= lastNewBlock; i++) {
      /* Alloc block i */
      pageid_t blockSize = tlp.initialSize * pow(tlp.multiplier, i);
      pageid_t newFirstPage = TpageAllocMany(xid, blockSize);
      DEBUG("block %d\n", i);
      tmp.slot = i + FIRST_DATA_PAGE_OFFSET;
      /* Iterate over the (large number) of new blocks, clearing their contents */
      /* @todo XXX arraylist generates N log entries initing pages.
	 It should generate 1 entry.  (Need better LSN handling first.)*/
      {
	for(pageid_t i = newFirstPage; i < newFirstPage + blockSize; i++) {
	  TinitializeFixedPage(xid, i, tlp.size);
	}
      }
      TsetRaw(xid,tmp,&newFirstPage);

      DEBUG("Tset: {%d, %d, %d} = %d\n", tmp.page, tmp.slot, tmp.size, newFirstPage);
    }
    
    tmp.slot = MAX_OFFSET_POSITION;
    
    pageid_t newMaxOffset = tlp.maxOffset+slots;
    TsetRaw(xid, tmp, &newMaxOffset);
  } end_ret(compensation_error());
  return 0;

}

compensated_function int TarrayListLength(int xid, recordid rid) { 
 Page * p = loadPage(xid, rid.page);
 readlock(p->rwlatch, 0);
 TarrayListParameters tlp = pageToTLP(xid, p);
 unlock(p->rwlatch);
 releasePage(p);
 return tlp.maxOffset+1;
}

/*----------------------------------------------------------------------------*/

recordid dereferenceArrayListRid(int xid, Page * p, int offset) {
  readlock(p->rwlatch,0);
  TarrayListParameters tlp = pageToTLP(xid, p);

  int rec_per_page = stasis_fixed_records_per_page((size_t)tlp.size);
  pageid_t lastHigh = 0;
  int pageRidSlot = 0; /* The slot on the root arrayList page that contains the first page of the block of interest */

  assert(tlp.maxOffset >= offset);
  
  pageRidSlot = getBlockContainingOffset(tlp, offset, &lastHigh);
  
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
static int getBlockContainingOffset(TarrayListParameters tlp, int offset, pageid_t * firstSlotInBlock) {
  int rec_per_page = stasis_fixed_records_per_page((size_t)tlp.size);
  long thisHigh = rec_per_page * tlp.initialSize;
  int lastHigh = 0;
  int pageRidSlot = 0;
  int currentPageLength = tlp.initialSize; 

  while(((pageid_t)offset) >= thisHigh) {
    pageRidSlot ++;
    lastHigh = thisHigh;
    currentPageLength *= tlp.multiplier;
    thisHigh += rec_per_page * currentPageLength;
  }
  if(firstSlotInBlock) {
    *firstSlotInBlock = lastHigh;
  }
  return pageRidSlot;
}

static TarrayListParameters pageToTLP(int xid, Page * p) {

  TarrayListParameters tlp;
  tlp.firstPage = p->id;
  /*  tlp.maxOffset   = *(int*)fixed_record_ptr(p, 3); */
  recordid rid = { p->id, 0, sizeof(pageid_t) };
  tlp.initialSize = *(pageid_t*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 1;
  tlp.multiplier = *(pageid_t*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 2;
  tlp.size = *(pageid_t*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 3;
  tlp.maxOffset = *(pageid_t*)stasis_record_read_begin(xid, p, rid);

  return tlp;
}
