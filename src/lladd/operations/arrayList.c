#include <config.h>
#include <lladd/common.h>

#include "../page/fixed.h"
#include <lladd/operations/pageOperations.h>
#include <lladd/operations/arrayList.h>
#include <lladd/transactional.h>
#include <lladd/bufferManager.h>

#include <assert.h>
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


static TarrayListParameters pageToTLP(Page * p);
static int getBlockContainingOffset(TarrayListParameters tlp, int offset, int * firstSlotInBlock);

/*----------------------------------------------------------------------------*/

recordid TarrayListAlloc(int xid, int count, int multiplier, int size) {

  int firstPage = TpageAllocMany(xid, count+1);

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

  Tupdate(xid, rid, &tlp, OPERATION_ARRAY_LIST_ALLOC);

  return rid;
}


static int operateAlloc(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {

  const TarrayListParameters * tlp = dat;

  int firstPage = tlp->firstPage;
  int count     = tlp->initialSize;
  int multiplier = tlp->multiplier;
  int size = tlp->size;

  fixedPageInitialize(p, sizeof(int), recordsPerPage(sizeof(int)));

#define MAX_OFFSET_POSITION    3
#define FIRST_DATA_PAGE_OFFSET 4
  
  recordid countRid, multiplierRid, slotSizeRid, maxOffset, firstDataPageRid;
  countRid.page = multiplierRid.page = slotSizeRid.page =  maxOffset.page = firstDataPageRid.page = p->id;
  countRid.size = multiplierRid.size = slotSizeRid.size =  maxOffset.size = firstDataPageRid.size = sizeof(int);

  countRid.slot = 0;   
  multiplierRid.slot = 1;
  slotSizeRid.slot = 2;
  maxOffset.slot = 3;
  firstDataPageRid.slot = 4;
  
  int firstDataPage = firstPage + 1;
  /* Allocing this page -> implicit lock. */
  fixedWriteUnlocked(p, countRid,      (byte*)&count);
  fixedWriteUnlocked(p, multiplierRid, (byte*)&multiplier);
  fixedWriteUnlocked(p, firstDataPageRid, (byte*)&firstDataPage);
  fixedWriteUnlocked(p, slotSizeRid, (byte*)&size);
  int minusOne = -1;
  fixedWriteUnlocked(p, maxOffset, (byte*)&minusOne);

  /* Write lsn... */

  *page_type_ptr(p) = ARRAY_LIST_PAGE;

  pageWriteLSN(p, lsn);

  recordid ret;
  ret.page = firstPage;
  ret.slot = 0;        /* slot = # of slots in array... */
  ret.size = size;
  
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

int TarrayListExtend(int xid, recordid rid, int slots) {
  Page * p = loadPage(rid.page);
  TarrayListParameters tlp = pageToTLP(p);

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
  for(int i = lastCurrentBlock+1; i <= lastNewBlock; i++) {
    /* Alloc block i */
    int blockSize = tlp.initialSize * powl(tlp.multiplier, i);
    int newFirstPage = TpageAllocMany(xid, blockSize);
    DEBUG("block %d\n", i);
    /* Iterate over the storage blocks that are pointed to by our current indirection block. */
    for(int j = 0; j < blockSize; j++) {
      DEBUG("page %d (%d)\n", j, j + newFirstPage);
      tmp2.page = j + newFirstPage;
      /** @todo If we were a little smarter about this, and fixed.c
	  could handle uninitialized blocks correctly, then we
	  wouldn't have to iterate over the datapages in
	  TarrayListExtend() */
      //      Tupdate(xid, tmp2, NULL, OPERATION_INITIALIZE_FIXED_PAGE);
    }
    
    tmp.slot = i + FIRST_DATA_PAGE_OFFSET;
    /** @todo what does this do to recovery?? */
    /** @todo locking for arrayList... */
    /*    *page_type_ptr(p) = FIXED_PAGE;
    Tset(xid, tmp, &newFirstPage);
    *page_type_ptr(p) = ARRAY_LIST_PAGE; */
    /* @todo This is a copy of Tupdate!! Replace it.*/

    alTupdate(xid, tmp, &newFirstPage, OPERATION_SET);

    DEBUG("Tset: {%d, %d, %d} = %d\n", tmp.page, tmp.slot, tmp.size, newFirstPage);
  }

  tmp.slot = MAX_OFFSET_POSITION;

  int newMaxOffset = tlp.maxOffset+slots;
  *page_type_ptr(p) = FIXED_PAGE;
  Tset(xid, tmp, &newMaxOffset);
  *page_type_ptr(p) = ARRAY_LIST_PAGE;
  releasePage(p);

  return 0;

}
/** @todo:  TarrayListInstantExtend, is a hacked-up cut and paste version of TarrayListExtend */
int TarrayListInstantExtend(int xid, recordid rid, int slots) {
  Page * p = loadPage(rid.page);
  TarrayListParameters tlp = pageToTLP(p);

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
  for(int i = lastCurrentBlock+1; i <= lastNewBlock; i++) {
    /* Alloc block i */
    int blockSize = tlp.initialSize * powl(tlp.multiplier, i);
    int newFirstPage = TpageAllocMany(xid, blockSize);
    DEBUG("block %d\n", i);
    /* Iterate over the storage blocks that are pointed to by our current indirection block. */
    /* for(int j = 0; j < blockSize; j++) {
      DEBUG("page %d (%d)\n", j, j + newFirstPage);
      tmp2.page = j + newFirstPage;
      / ** @todo If we were a little smarter about this, and fixed.c
	  coulds handle uninitialized blocks correctly, then we
	  wouldn't have to iterate over the datapages in
	  TarrayListExtend()  * /
      // Tupdate(xid, tmp2, NULL, OPERATION_INITIALIZE_FIXED_PAGE);
    } */
    
    tmp.slot = i + FIRST_DATA_PAGE_OFFSET;
    /** @todo what does this do to recovery?? */
    /** @todo locking for arrayList... */
    *page_type_ptr(p) = FIXED_PAGE;
    TinstantSet(xid, tmp, &newFirstPage);
    *page_type_ptr(p) = ARRAY_LIST_PAGE;

    DEBUG("Tset: {%d, %d, %d} = %d\n", tmp.page, tmp.slot, tmp.size, newFirstPage);
  }

  tmp.slot = MAX_OFFSET_POSITION;

  int newMaxOffset = tlp.maxOffset+slots;
  /** @todo CORRECTNESS BUG: From recovery's point of view, arrayList is totally wrong! The
      only reason we mess with p is beacuse TinstantSet doesn't handle
      ARRAY_LIST_PAGES the way we need it to, so this won't be hard to
      fix... */
  *page_type_ptr(p) = FIXED_PAGE;
  TinstantSet(xid, tmp, &newMaxOffset);
  *page_type_ptr(p) = ARRAY_LIST_PAGE;
  releasePage(p);

  return 0;

}


static int operateInitFixed(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  
  fixedPageInitialize(p, rid.size, recordsPerPage(rid.size));

  pageWriteLSN(p, lsn);
  return 0;
}

static int operateUnInitPage(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  *page_type_ptr(p) = UNINITIALIZED_PAGE;
  pageWriteLSN(p, lsn);
  return 0;
}

Operation getInitFixed() {
  Operation o = {
    OPERATION_INITIALIZE_FIXED_PAGE,
    0,  /* The necessary parameters are hidden in the rid */
    /*OPERATION_UNINITIALIZE_PAGE,*/  OPERATION_NOOP, 
    &operateInitFixed
  };
  return o;
}
Operation getUnInitPage() {
  Operation o = {
    OPERATION_UNINITIALIZE_PAGE,
    PAGE_SIZE,
    NO_INVERSE_WHOLE_PAGE, /* OPERATION_NOOP,*/
    &operateUnInitPage
  };
  return o;
}

/*----------------------------------------------------------------------------*/

/** @todo locking for arrayLists */
recordid dereferenceArrayListRid(Page * p, int offset) {

  TarrayListParameters tlp = pageToTLP(p);

  int rec_per_page = recordsPerPage((size_t)tlp.size);
  int lastHigh = 0;
  int pageRidSlot = 0; /* The slot on the root arrayList page that contains the first page of the block of interest */

  assert(tlp.maxOffset >= offset);
  
  pageRidSlot = getBlockContainingOffset(tlp, offset, &lastHigh);
  
  int dataSlot = offset - lastHigh;  /* The offset in the block of interest of the slot we want. */
  int blockPage = dataSlot / rec_per_page;  /* The page in the block of interest that contains the slot we want */
  int blockSlot = dataSlot - blockPage * rec_per_page;

  int thePage;

  assert(pageRidSlot + FIRST_DATA_PAGE_OFFSET < fixedPageCount(p));
  thePage = *(int*)fixed_record_ptr(p, pageRidSlot + FIRST_DATA_PAGE_OFFSET); /*reading immutable record; don't need latch.*/

  recordid rid;
  rid.page = thePage + blockPage;
  rid.slot = blockSlot;
  rid.size = tlp.size;

  return rid;

}
static int getBlockContainingOffset(TarrayListParameters tlp, int offset, int * firstSlotInBlock) {
  int rec_per_page = recordsPerPage((size_t)tlp.size);
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

static TarrayListParameters pageToTLP(Page * p) {

  TarrayListParameters tlp;
  tlp.firstPage = p->id;
  tlp.initialSize = *(int*)fixed_record_ptr(p, 0);
  tlp.multiplier  = *(int*)fixed_record_ptr(p, 1);
  tlp.size        = *(int*)fixed_record_ptr(p, 2);
  tlp.maxOffset   = *(int*)fixed_record_ptr(p, 3);

  return tlp;
}


