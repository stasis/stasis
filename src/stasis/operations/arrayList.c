
#include <config.h>
#include <stasis/common.h>

#include "../page.h"
#include <stasis/operations/pageOperations.h>
#include <stasis/operations/arrayList.h>
#include <stasis/transactional.h>
#include <stasis/bufferManager.h>

#include <assert.h>
#define _XOPEN_SOURCE 600
#include <math.h>



/** 
    Implement resizable arrays, just like java's ArrayList class.

    Essentially, the base page contains a fixed size array of rids
    pointing at contiguous blocks of pages.  Each block is twice as
    big as the previous block.

    The base block is of type FIXED_PAGE, of int's. The first few slots are reserved:

*/

typedef struct {
  int firstPage;
  int initialSize;
  int multiplier;
  int size;
  int maxOffset;
} TarrayListParameters;


static TarrayListParameters pageToTLP(int xid, Page * p);
static int getBlockContainingOffset(TarrayListParameters tlp, int offset, int * firstSlotInBlock);

#define MAX_OFFSET_POSITION    3
#define FIRST_DATA_PAGE_OFFSET 4

/*----------------------------------------------------------------------------*/

compensated_function recordid TarrayListAlloc(int xid, int count, int multiplier, int size) {
  
  int firstPage;
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
    Tupdate(xid, rid, &tlp, OPERATION_ARRAY_LIST_ALLOC);
  } end_ret(NULLRID);

  return rid;
}


static int operateAlloc(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {

  const TarrayListParameters * tlp = dat;

  int firstPage = tlp->firstPage;
  int count     = tlp->initialSize;
  int multiplier = tlp->multiplier;
  int size = tlp->size;

  /* Allocing this page -> implicit lock, but latch to conformt to
   fixedPage's interface. */
  writelock(p->rwlatch, 0);
  stasis_fixed_initialize_page(p, sizeof(int), stasis_fixed_records_per_page(sizeof(int)));

  recordid countRid, multiplierRid, slotSizeRid, maxOffset, firstDataPageRid;
  countRid.page = multiplierRid.page = slotSizeRid.page =  maxOffset.page = firstDataPageRid.page = p->id;
  countRid.size = multiplierRid.size = slotSizeRid.size =  maxOffset.size = firstDataPageRid.size = sizeof(int);

  countRid.slot = 0;
  multiplierRid.slot = 1;
  slotSizeRid.slot = 2;
  maxOffset.slot = 3;
  firstDataPageRid.slot = 4;

  int firstDataPage = firstPage + 1;
  (*(int*)stasis_record_write_begin(xid, p, countRid))= count;
  (*(int*)stasis_record_write_begin(xid, p, multiplierRid))= multiplier;
  (*(int*)stasis_record_write_begin(xid, p, firstDataPageRid))= firstDataPage;
  (*(int*)stasis_record_write_begin(xid, p, slotSizeRid))= size;
  (*(int*)stasis_record_write_begin(xid, p, maxOffset))= -1;

  *stasis_page_type_ptr(p) = ARRAY_LIST_PAGE;

  stasis_page_lsn_write(xid, p, lsn);

  recordid ret;
  ret.page = firstPage;
  ret.slot = 0;        /* slot = # of slots in array... */
  ret.size = size;
  unlock(p->rwlatch);

  return 0;
}

Operation getArrayListAlloc() {
  Operation o = {
    OPERATION_ARRAY_LIST_ALLOC, /* ID */
    sizeof(TarrayListParameters),
    OPERATION_NOOP,  /* Since TpageAllocMany will be undone, the page we touch will be nuked anyway, so set this to NO-OP. */
    &operateAlloc
  };
  return o;
}
/*----------------------------------------------------------------------------*/

/** @todo locking for arrayList... this isn't pressing since currently
    the only thing that calls arraylist (the hashtable
    implementations) serialize bucket list operations anyway... 

    @todo this function calls pow(), which is horribly inefficient. 
*/

static compensated_function int TarrayListExtendInternal(int xid, recordid rid, int slots, int op) {
  Page * p;
  try_ret(compensation_error()) {
    p = loadPage(xid, rid.page);
  } end_ret(compensation_error());
  readlock(p->rwlatch, 0);
  TarrayListParameters tlp = pageToTLP(xid, p);
  unlock(p->rwlatch);;
  releasePage(p);
  p = NULL;

  int lastCurrentBlock;
  if(tlp.maxOffset == -1) {
    lastCurrentBlock = -1;
  } else{
    lastCurrentBlock = getBlockContainingOffset(tlp, tlp.maxOffset, NULL);
  } 
  int lastNewBlock = getBlockContainingOffset(tlp, tlp.maxOffset+slots, NULL);

  DEBUG("lastCurrentBlock = %d, lastNewBlock = %d\n", lastCurrentBlock, lastNewBlock);

  recordid tmp;   /* recordid of slot in base page that holds new block. */
  tmp.page = rid.page;
  tmp.size = sizeof(int);
  
  recordid tmp2;  /* recordid of newly created pages. */
  tmp2.slot = 0;
  tmp2.size = tlp.size;
  /* Iterate over the (small number) of indirection blocks that need to be updated */
  try_ret(compensation_error()) {
    for(int i = lastCurrentBlock+1; i <= lastNewBlock; i++) {
      /* Alloc block i */
      int blockSize = tlp.initialSize * pow(tlp.multiplier, i);
      int newFirstPage = TpageAllocMany(xid, blockSize);
      DEBUG("block %d\n", i);
      /* We used to call OPERATION_INITIALIZE_FIXED_PAGE on each page in current indirection block. */
      tmp.slot = i + FIRST_DATA_PAGE_OFFSET;

      TupdateRaw(xid, tmp, &newFirstPage, op);
      DEBUG("Tset: {%d, %d, %d} = %d\n", tmp.page, tmp.slot, tmp.size, newFirstPage);
    }
    
    tmp.slot = MAX_OFFSET_POSITION;
    
    int newMaxOffset = tlp.maxOffset+slots;
    TupdateRaw(xid, tmp, &newMaxOffset, op);
  } end_ret(compensation_error());
  return 0;

}

compensated_function int TarrayListInstantExtend(int xid, recordid rid, int slots) {
  return TarrayListExtendInternal(xid, rid, slots, OPERATION_INSTANT_SET);
}
compensated_function int TarrayListExtend(int xid, recordid rid, int slots) {
  return TarrayListExtendInternal(xid, rid, slots, OPERATION_SET);
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

/** 
    @todo XXX latching for dereference arraylist rid (and other dereference functions...) 
*/
recordid dereferenceArrayListRid(int xid, Page * p, int offset) {
  readlock(p->rwlatch,0);
  TarrayListParameters tlp = pageToTLP(xid, p);

  int rec_per_page = stasis_fixed_records_per_page((size_t)tlp.size);
  int lastHigh = 0;
  int pageRidSlot = 0; /* The slot on the root arrayList page that contains the first page of the block of interest */

  assert(tlp.maxOffset >= offset);
  
  pageRidSlot = getBlockContainingOffset(tlp, offset, &lastHigh);
  
  int dataSlot = offset - lastHigh;  /* The offset in the block of interest of the slot we want. */
  int blockPage = dataSlot / rec_per_page;  /* The page in the block of interest that contains the slot we want */
  int blockSlot = dataSlot - blockPage * rec_per_page;

  int thePage;

  recordid rid = { p->id, pageRidSlot + FIRST_DATA_PAGE_OFFSET, sizeof(int) };
  thePage = *(int*)stasis_record_read_begin(xid,p,rid);
  unlock(p->rwlatch);

  rid.page = thePage + blockPage;
  rid.slot = blockSlot;
  rid.size = tlp.size;

  return rid;

}
static int getBlockContainingOffset(TarrayListParameters tlp, int offset, int * firstSlotInBlock) {
  int rec_per_page = stasis_fixed_records_per_page((size_t)tlp.size);
  long thisHigh = rec_per_page * tlp.initialSize;
  int lastHigh = 0;
  int pageRidSlot = 0;
  int currentPageLength = tlp.initialSize; 

  while(((long)offset) >= thisHigh) {
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
  recordid rid = { p->id, 0, sizeof(int) };
  tlp.initialSize = *(int*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 1;
  tlp.multiplier = *(int*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 2;
  tlp.size = *(int*)stasis_record_read_begin(xid, p, rid);
  rid.slot = 3;
  tlp.maxOffset = *(int*)stasis_record_read_begin(xid, p, rid);

  return tlp;
}
