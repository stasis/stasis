/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.
                                                                                                                                  
The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.
                                                                                                                                  
IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
                                                                                                                                  
THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
                                                                                                                                  
GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/

/************************************************************************
 * implementation of pages

 STRUCTURE OF A PAGE

 +-------------------------------------------+-----------------------+--+
 | DATA SECTION                   +--------->| RID: (PAGE, 0)        |  |
 |          +-----------------+   |          +-----------------------+  |
 |      +-->| RID: (PAGE, 1)  |   |                                     |
 |      |   +-----------------+   |                                     |
 |      |                         |                                     |
 |      +-----------------+       |        +----------------------------+
 |                        |       |   +--->| RID: (PAGE, n)             |
 |                        |       |   |    +----------------------------+
 |======================================================================|
 |^ FREE SPACE            |       |   |                                 |
 |+-----------------------|-------|---|--------------------+            |
 |                        |       |   |                    |            |
 |          +-------------|-------|---+                    |            |
 |          |             |       |                        |            |
 |      +---|---+-----+---|---+---|---+--------------+-----|------+-----+
 |      | slotn | ... | slot1 | slot0 | num of slots | free space | LSN |
 +------+-------+-----+-------+-------+--------------+------------+-----+

 NOTE:
   - slots are zero indexed.
   - slots are of implemented as (offset, length)

 Latching summary:

   Each page has an associated read/write lock.  This lock only
   protects the internal layout of the page, and the members of the
   page struct.  Here is how it is held in various circumstances:

   Record allocation:  Write lock
   Record read:        Read lock
   Read LSN            Read lock
   Record write       *READ LOCK*
   Write LSN           Write lock
 
 Any circumstance where these locks are held during an I/O operation
 is a bug.
 
 $Id$

************************************************************************/

#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include "page.h"

#include <assert.h>
#include <stdio.h>

#include <lladd/constants.h>

/* TODO:  Combine with buffer size... */
static int nextPage = 0;

static const byte *slotMemAddr(const byte *memAddr, int slotNum) ;

/** @todo:  Why does only one of the get/set First/Second HalfOfWord take an unsigned int? */
static int getFirstHalfOfWord(unsigned int *memAddr);
static int getSecondHalfOfWord(int *memAddr);
static void setFirstHalfOfWord(int *memAddr, int value);
static void setSecondHalfOfWord(int *memAddr, int value);

static int readFreeSpace(byte *memAddr);
static void writeFreeSpace(byte *memAddr, int newOffset);
static int readNumSlots(byte *memAddr);
static void writeNumSlots(byte *memAddr, int numSlots);

static int getSlotOffset(byte *memAddr, int slot) ;
static int getSlotLength(byte *memAddr, int slot) ;
static void setSlotOffset(byte *memAddr, int slot, int offset) ;
static void setSlotLength(byte *memAddr, int slot, int length) ;

/** @todo replace static ints in page.c with #defines. */

/* ------ */

static int SLOT_OFFSET_SIZE;
static int SLOT_LENGTH_SIZE;
static int SLOT_SIZE;

static int LSN_SIZE;
static int FREE_SPACE_SIZE;
static int NUMSLOTS_SIZE;

static int START_OF_LSN;
static int START_OF_FREE_SPACE;
static int START_OF_NUMSLOTS;

static int MASK_0000FFFF;
static int MASK_FFFF0000;

/* ------ */

static pthread_mutex_t pageAllocMutex;
/** We need one dummy page for locking purposes, so this array has one extra page in it. */
Page pool[MAX_BUFFER_SIZE+1];


int isValidSlot(byte *memAddr, int slot);
void invalidateSlot(byte *memAddr, int slot);
void pageDeRalloc(Page * page, recordid rid);

/**
   The caller of this function must already have a writelock on the
   page.
*/
static void pageCompact(Page * page);

/**
 * pageInit() initializes all the important variables needed in
 * all the functions dealing with pages.
 */
void pageInit() {

  nextPage = 0;
	/**
	 * For now, we will assume that slots are 4 bytes long, and that the
	 * first two bytes are the offset, and the second two bytes are the
	 * the length.  There are some functions at the bottom of this file
	 * that may be useful later if we decide to dynamically choose
	 * sizes for offset and length.
	 */

	/**
	 * the largest a slot length can be is the size of the page,
	 * and the greatest offset at which a record could possibly 
	 * start is at the end of the page
	 */
	SLOT_LENGTH_SIZE = SLOT_OFFSET_SIZE = 2; /* in bytes */
	SLOT_SIZE = SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE;

	LSN_SIZE = sizeof(long);
	FREE_SPACE_SIZE = NUMSLOTS_SIZE = 2;

	/* START_OF_LSN is the offset in the page to the lsn */
	START_OF_LSN = PAGE_SIZE - LSN_SIZE;
	START_OF_FREE_SPACE = START_OF_LSN - FREE_SPACE_SIZE;
	START_OF_NUMSLOTS = START_OF_FREE_SPACE - NUMSLOTS_SIZE;

	MASK_0000FFFF = (1 << (2*BITS_PER_BYTE)) - 1;
	MASK_FFFF0000 = ~MASK_0000FFFF;

	
	pthread_mutex_init(&pageAllocMutex, NULL);
}

void pageCommit(int xid) {
  /*	 rmTouch(xid); */
}

void pageAbort(int xid) {
  /* rmTouch(xid); */
}


static int getFirstHalfOfWord(unsigned int *memAddr) {
  unsigned int word = *memAddr;
  word = (word >> (2*BITS_PER_BYTE)); /* & MASK_0000FFFF; */
  return word;
}


static int getSecondHalfOfWord(int *memAddr) {
	int word = *memAddr;
	word = word & MASK_0000FFFF;
	return word;
}


void setFirstHalfOfWord(int *memAddr, int value){
	int word = *memAddr;
	word = word & MASK_0000FFFF;
	word = word | (value << (2*BITS_PER_BYTE));
	*memAddr = word;
}


void setSecondHalfOfWord(int *memAddr, int value) {
	int word = *memAddr;;
	word = word & MASK_FFFF0000;
	word = word | (value & MASK_0000FFFF);
	*memAddr = word;
}

/**
 * slotMemAddr() calculates the memory address of the given slot.  It does this 
 * by going to the end of the page, then walking backwards, past the LSN field
 * (LSN_SIZE), past the 'free space' and 'num of slots' fields (NUMSLOTS_SIZE),
 * and then past a slotNum slots (slotNum * SLOT_SIZE).
 */
static const byte *slotMemAddr(const byte *memAddr, int slotNum) {
	return (memAddr + PAGE_SIZE) - (LSN_SIZE + FREE_SPACE_SIZE + NUMSLOTS_SIZE + ((slotNum+1) * SLOT_SIZE));
}

/**
 * pageReadLSN() assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t pageReadLSN(const Page * page) {
  lsn_t ret;

  /*  readlock(page->rwlatch, 259); */
  ret = *(long *)(page->memAddr + START_OF_LSN);
  /*  readunlock(page->rwlatch); */

  return ret;
}

/**
 * pageWriteLSN() assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page.  The Page struct contains the new LSN and the page
 * number to which the new LSN must be written to.
 *
 * @param page You must have a writelock on page before calling this function.
 */
static void pageWriteLSN(Page * page) {

  *(long *)(page->memAddr + START_OF_LSN) = page->LSN;
}

static int unlocked_freespace(Page * page);
/**
 * freeSpace() assumes that the page is already loaded in memory.  It takes 
 * as a parameter a Page, and returns an estimate of the amount of free space
 * available to a new slot on this page.  (This is the amount of unused space 
 * in the page, minus the size of a new slot entry.)  This is either exact, 
 * or an underestimate.
 */
int freespace(Page * page) {
  int ret;
  readlock(page->rwlatch, 292);
  ret = unlocked_freespace(page);
  readunlock(page->rwlatch);
  return ret;
}

/** 
    Just like freespace(), but doesn't obtain a lock.  (So that other methods in this file can use it.)
*/
static int unlocked_freespace(Page * page) {
  int space;
  space= (slotMemAddr(page->memAddr, readNumSlots(page->memAddr)) - (page->memAddr + readFreeSpace(page->memAddr)));
  return (space < 0) ? 0 : space;
}

/**
 * readFreeSpace() assumes that the page is already loaded in memory.  It takes
 * as a parameter the memory address of the loaded page in memory and returns
 * the offset at which the free space section of this page begins.
 */
static int readFreeSpace(byte *memAddr) {
	return getSecondHalfOfWord((int*)(memAddr + START_OF_NUMSLOTS));
}

/**
 * writeFreeSpace() assumes that the page is already loaded in memory.  It takes
 * as parameters the memory address of the loaded page in memory and a new offset
 * in the page that will denote the point at which free space begins.
 */
static void writeFreeSpace(byte *memAddr, int newOffset) {
	setSecondHalfOfWord((int*)(memAddr + START_OF_NUMSLOTS), newOffset);
}

/**
 * readNumSlots() assumes that the page is already loaded in memory.  It takes
 * as a parameter the memory address of the loaded page in memory, and returns
 * the memory address at which the free space section of this page begins.
 */
static int readNumSlots(byte *memAddr) {
	return getFirstHalfOfWord((unsigned int*)(memAddr + START_OF_NUMSLOTS));
}

/**
 * writeNumSlots() assumes that the page is already loaded in memory.  It takes
 * as parameters the memory address of the loaded page in memory and an int
 * to which the value of the numSlots field in the page will be set to.
 */
static void writeNumSlots(byte *memAddr, int numSlots) {
	setFirstHalfOfWord((int*)(unsigned int*)(memAddr + START_OF_NUMSLOTS), numSlots);
}

recordid pageRalloc(Page * page, int size) {
        int freeSpace;
        int numSlots;
	int i;

	writelock(page->rwlatch, 342);
	if(unlocked_freespace(page) < size) {
	  
	  pageCompact(page);

	/* Make sure there's enough free space... */

#ifdef DEBUGGING
	  assert (unlocked_freespace(page) >= (int)size); /*Expensive, so skip it when debugging is off. */
#endif

	}
	freeSpace = readFreeSpace(page->memAddr);
	numSlots = readNumSlots(page->memAddr);
	recordid rid;


	rid.page = page->id;
	rid.slot = numSlots;
	rid.size = size;


	/* 
	   Reuse an old (invalid) slot entry.  Why was this here? 
	   
	   @todo is slot reuse in page.c a performance bottleneck? 
	   
	*/
	for (i = 0; i < numSlots; i++) { 
	  if (!isValidSlot(page->memAddr, i)) {
	    rid.slot = i;
	    break;
	  }
	}

	if (rid.slot == numSlots) {
	  writeNumSlots(page->memAddr, numSlots+1);
	}

	setSlotOffset(page->memAddr, rid.slot, freeSpace);
	setSlotLength(page->memAddr, rid.slot, rid.size);  
	writeFreeSpace(page->memAddr, freeSpace + rid.size);

	writeunlock(page->rwlatch);

	/*	DEBUG("slot: %d freespace: %d\n", rid.slot, freeSpace); */

	return rid;
}


/** Only used for recovery, to make sure that consistent RID's are created 
 * on log playback. */
recordid pageSlotRalloc(Page * page, lsn_t lsn, recordid rid) {
        int freeSpace; 
	int numSlots;

	writelock(page->rwlatch, 376);

	freeSpace = readFreeSpace(page->memAddr);
	numSlots= readNumSlots(page->memAddr);

/*	if(rid.size > BLOB_THRESHOLD_SIZE) {
	  return blobSlotAlloc(page, lsn_t lsn, recordid rid);
	  }*/

	/*	assert(rid.slot >= numSlots); */
	if(rid.slot >= numSlots) {

	  if (freeSpace < rid.size) {
	    pageCompact(page);
	    freeSpace = readFreeSpace(page->memAddr);
	    assert (freeSpace < rid.size);
	  }
	  
	  setSlotOffset(page->memAddr, rid.slot, freeSpace);
	  setSlotLength(page->memAddr, rid.slot, rid.size);  
	  writeFreeSpace(page->memAddr, freeSpace + rid.size);
	} else {
	  /*  assert(rid.size == getSlotLength(page.memAddr, rid.slot)); */ /* Fails.  Why? */
	}

	writeunlock(page->rwlatch);

	return rid;
}

int isValidSlot(byte *memAddr, int slot) {
	return (getSlotOffset(memAddr, slot) != INVALID_SLOT) ? 1 : 0;
}

void invalidateSlot(byte *memAddr, int slot) {
	setSlotOffset(memAddr, slot, INVALID_SLOT);
}


void pageDeRalloc(Page * page, recordid rid) {
  writelock(page->rwlatch, 416);
  invalidateSlot(page->memAddr, rid.slot);
  writeunlock(page->rwlatch);
}

/**

 	Move all of the records to the beginning of the page in order to 
	increase the available free space.

	@todo If we were supporting multithreaded operation, this routine 
	      would need to pin the pages that it works on.
*/
static void pageCompact(Page * page) {

	int i;
	byte buffer[PAGE_SIZE];
	int freeSpace = 0;
	int numSlots;
	int meta_size; 
	int slot_length;
	int last_used_slot = -1;

	numSlots = readNumSlots(page->memAddr);

	/*	DEBUG("Compact: numSlots=%d\n", numSlots); */
	meta_size = LSN_SIZE + FREE_SPACE_SIZE + NUMSLOTS_SIZE + (SLOT_SIZE*numSlots);

	/* Can't compact in place, slot numbers can come in different orders than 
	   the physical space allocated to them. */
	memcpy(buffer + PAGE_SIZE - meta_size, page->memAddr + PAGE_SIZE - meta_size, meta_size);

	for (i = 0; i < numSlots; i++) {
	  /*	  DEBUG("i = %d\n", i); */
	          if (isValidSlot(page->memAddr, i)) {
		    /*  		  DEBUG("Buffer offset: %d\n", freeSpace); */
			slot_length = getSlotLength(page->memAddr, i);
			memcpy(buffer + freeSpace, page->memAddr + getSlotOffset(page->memAddr, i), slot_length);
			setSlotOffset(buffer, i, freeSpace);
			freeSpace += slot_length;
			last_used_slot = i;
		} 
	}

	
	/*	if (last_used_slot < numSlots) { */
	writeNumSlots(buffer, last_used_slot + 1);
		/*} */

	/*	DEBUG("freeSpace = %d, num slots = %d\n", freeSpace, last_used_slot + 1); */
	
	writeFreeSpace(buffer, freeSpace);
	
	memcpy(page->memAddr, buffer, PAGE_SIZE);

}

/**
 * getSlotOffset() assumes that the page is already loaded in memory.  It takes
 * as parameters the memory address of the page loaded in memory, and a slot
 * number.  It returns the offset corresponding to that slot.
 */
static int getSlotOffset(byte *memAddr, int slot) {
	return getFirstHalfOfWord((unsigned int*)slotMemAddr(memAddr, slot));
}

/**
 * getSlotLength() assumes that the page is already loaded in memory.  It takes
 * as parameters the memory address of the page loaded in memory, and a slot
 * number.  It returns the length corresponding to that slot.
 */
static int getSlotLength(byte *memAddr, int slot) {
	return getSecondHalfOfWord((int*)(unsigned int*)slotMemAddr(memAddr, slot));
}

/**
 * setSlotOffset() assumes that the page is already loaded in memory.  It takes
 * as parameters the memory address of the page loaded in memory, a slot number,
 * and an offset.  It sets the offset of the given slot to the offset passed in
 * as a parameter.
 */
static void setSlotOffset(byte *memAddr, int slot, int offset) {
	setFirstHalfOfWord((int*)slotMemAddr(memAddr, slot), offset);
}

/**
 * setSlotLength() assumes that the page is already loaded in memory.  It takes
 * as parameters the memory address of the page loaded in memory, a slot number,
 * and a length.  It sets the length of the given slot to the length passed in
 * as a parameter.
 */
static void setSlotLength(byte *memAddr, int slot, int length) {
	setSecondHalfOfWord((int*)(unsigned int*)slotMemAddr(memAddr, slot), length);
}

int isBlobSlot(byte *pageMemAddr, int slot) {
	return BLOB_SLOT == getSlotLength(pageMemAddr, slot);
}

/*
  This needs should trust the rid (since the caller needs to
  override the size in special circumstances)

  @todo If the rid size has been overridden, we should check to make
  sure that this really is a special record.
*/
void pageReadRecord(int xid, Page * page, recordid rid, byte *buff) {
  byte *recAddress;
  
  readlock(page->rwlatch, 519);

  assert(page->id == rid.page);
  recAddress = page->memAddr + getSlotOffset(page->memAddr, rid.slot);
  memcpy(buff, recAddress,  rid.size);
  readunlock(page->rwlatch);
  
}

void pageWriteRecord(int xid, Page * page, recordid rid, lsn_t lsn, const byte *data) {

  byte *rec; 
  readlock(page->rwlatch, 529);
  assert(rid.size < PAGE_SIZE);
  
  rec = page->memAddr + getSlotOffset(page->memAddr, rid.slot);
  
  if(memcpy(rec, data,  rid.size) == NULL ) {
    printf("ERROR: MEM_WRITE_ERROR on %s line %d", __FILE__, __LINE__);
    exit(MEM_WRITE_ERROR);
  }

  page->LSN = lsn;
  pageWriteLSN(page);
  readunlock(page->rwlatch);

}

void pageReallocNoLock(Page *p, int id) {
  p->id = id;
  p->LSN = 0;
  p->dirty = 0;
  p->pending = 0;
  p->waiting = 0;
}

void pageRealloc(Page *p, int id) {
  writelock(p->rwlatch, 10);
  pageReallocNoLock(p,id);
  writeunlock(p->rwlatch);
}


/** 
	Allocate a new page. 
        @param id The id of the new page.
	@return A pointer to the new page.  This memory is part of a pool, 
	        and should never be freed manually.
 */
Page *pageAlloc(int id) {
  Page *page;

  pthread_mutex_lock(&pageAllocMutex);
  
  page = &(pool[nextPage]);
  
  /* We have an implicit lock on rwlatch, since we allocated it, but
     haven't returned yet. */
  page->rwlatch = initlock();
  page->loadlatch = initlock();

  /*  pthread_mutex_init(&page->pending_mutex, NULL);*/
  pthread_cond_init(&page->noMorePending, NULL);

  page->memAddr = malloc(PAGE_SIZE);

  nextPage++;
  assert(nextPage <= MAX_BUFFER_SIZE + 1); /* There's a dummy page that we need to keep around, thus the +1 */

  /* uggh.  Really just want to pass pages by reference */
  /*  page->pending = malloc(sizeof(int));  */

  pthread_mutex_unlock(&pageAllocMutex);


  page->pending = 0;
  page->waiting = 0;

  return page;
}

void printPage(byte *memAddr) {
	int i = 0;
	for (i = 0; i < PAGE_SIZE; i++) {
		if((*(char *)(memAddr+i)) == 0) {
			printf("#");
		}else {
			printf("%c", *(char *)(memAddr+i));
		}
		if((i+1)%4 == 0)
			printf(" ");
	}
}

#define num 20
int pageTest() {

        Page * page = malloc(sizeof(Page));

	recordid rid[num];
	char *str[num] = {"one",
		"two",
		"three",
		"four",
		"five",
		"six",
		"seven",
		"eight",
		"nine",
		"ten",
		"eleven",
		"twelve",
		"thirteen",
		"fourteen",
		"fifteen",
		"sixteen",
		"seventeen",
		"eighteen",
		"nineteen",
		"twenty"};
		int i;

		page->memAddr = (byte *)malloc(PAGE_SIZE);
		memset(page->memAddr, 0, PAGE_SIZE);
		for (i = 0; i < num; i++) {
			rid[i] = pageRalloc(page, strlen(str[i]) + 1);
			pageWriteRecord(0, page, rid[i], 1, (byte*)str[i]);    
		}
		printPage(page->memAddr);

		for (i = 0; i < num; i+= 2)
			pageDeRalloc(page, rid[i]);

		pageCompact(page);
		printf("\n\n\n");
		printPage(page->memAddr);
		return 0;
}

void pageSetSlotType(Page * p, int slot, int type) {
  assert(type > PAGE_SIZE);
  
  /* setSlotLength does the locking for us. */
  setSlotLength(p->memAddr, slot, type);

}

int pageGetSlotType(Page *  p, int slot, int type) {
  int ret = getSlotLength(p->memAddr, slot);
  /* getSlotType does the locking for us. */
  return ret > PAGE_SIZE ? ret : NORMAL_SLOT;
}
