/** $Id$ */


#include "../page.h"
#include "../blobManager.h"  /** So that we can call sizeof(blob_record_t) */
#include "slotted.h"
#include <assert.h>

/* ------------------ STATIC FUNCTIONS.  NONE OF THESE ACQUIRE LOCKS
                      ON THE MEMORY THAT IS PASSED INTO THEM -------------*/

static void __really_do_ralloc(Page * page, recordid rid) ;

/**
   
Move all of the records to the beginning of the page in order to 
increase the available free space.

The caller of this function must have a writelock on the page.
*/

void slottedCompact(Page * page) {

	int i;
	Page bufPage;
	byte buffer[PAGE_SIZE];

	int numSlots;
	int meta_size;

	bufPage.id = -1;
	bufPage.memAddr = buffer;

	/* Can't compact in place, slot numbers can come in different orders than 
	   the physical space allocated to them. */

	memset(buffer, -1, PAGE_SIZE);
	
	meta_size = (((int)page->memAddr) + PAGE_SIZE ) - (int)end_of_usable_space_ptr(page); 
                                /* *slot_length_ptr(page, (*numslots_ptr(page))-1);*/

	memcpy(buffer + PAGE_SIZE - meta_size, page->memAddr + PAGE_SIZE - meta_size, meta_size);

	slottedPageInitialize(&bufPage);

	numSlots = *numslots_ptr(page);
	for (i = 0; i < numSlots; i++) {
	  /*	  ("i = %d\n", i);   */
	  if (isValidSlot(page, i)) {
	    /*	    printf("copying %d\n", i); 
		    fflush(NULL); */
	    /*  		  DEBUG("Buffer offset: %d\n", freeSpace); */
	    recordid rid;

	    rid.page = -1;
	    rid.slot = i;
	    rid.size = *slot_length_ptr(page, i);

	    __really_do_ralloc(&bufPage, rid);

	    memcpy(record_ptr(&bufPage, rid.slot), record_ptr(page, rid.slot), rid.size);
	    
	  } else {
	    *slot_ptr(&bufPage, i) = INVALID_SLOT;
	    *slot_length_ptr(&bufPage, i) = *freelist_ptr(page);
	    *freelist_ptr(page) = i;
	  }
	}
	
	/** The freelist could potentially run past the end of the
	    space that is allocated for slots (this would happen if
	    the number of slots needed by this page just decreased.
	    If we let the list run outside of that area, it could
	    cause inadvertant page corruption.  Therefore, we need to
	    truncate the list before continuing. */

	short next = *freelist_ptr(page);
	while(next >= numSlots) {
	  next = *slot_length_ptr(page, next);
	}

	*freelist_ptr(page) = next;
	
	/* Rebuild the freelist. */
	
	/*	*freelist_ptr(&bufPage) = 0;
	for (i = 0; i < numSlots; i++) { 
	  if (!isValidSlot(&bufPage, i)) {
	    *slot_length_ptr(&bufPage, i) = *freelist_ptr(&bufPage);
	    *freelist_ptr(&bufPage) = i;
	    break;
	  }
	}
	*/

	memcpy(page->memAddr, buffer, PAGE_SIZE);
}

/**
   Invariant: This lock should be held while updating lastFreepage, or
   while performing any operation that may decrease the amount of
   freespace in the page that lastFreepage refers to.  

   Since pageCompact and slottedDeRalloc may only increase this value,
   they do not need to hold this lock.  Since bufferManager is the
   only place where rawPageRallocSlot is called, rawPageRallocSlot does not obtain
   this lock.
   
   If you are calling rawPageRallocSlot on a page that may be the page
   lastFreepage refers to, then you will need to acquire
   lastFreepage_mutex.  (Doing so from outside of slotted.c is almost
   certainly asking for trouble, so lastFreepage_mutex is static.)

*/


/*static pthread_mutex_t lastFreepage_mutex; */
  static unsigned int lastFreepage = -10; 

void slottedPageInit() {
  /*pthread_mutex_init(&lastFreepage_mutex , NULL);  */
  lastFreepage = -1;
}

void slottedPageDeinit() {
  /*  pthread_mutex_destroy(&lastFreepage_mutex); */
}


void slottedPageInitialize(Page * page) {
  /*printf("Initializing page %d\n", page->id);
  fflush(NULL);  */
  memset(page->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(page) = SLOTTED_PAGE;
  *freespace_ptr(page) = 0;
  *numslots_ptr(page)  = 0;
  *freelist_ptr(page)  = INVALID_SLOT;

}

int slottedFreespaceUnlocked(Page * page) {
  return (int)slot_length_ptr(page, *numslots_ptr(page)) - (int)(page->memAddr + *freespace_ptr(page));
}


int slottedFreespace(Page * page) {
  int ret;
  readlock(page->rwlatch, 292);
  ret = slottedFreespaceUnlocked(page);
  readunlock(page->rwlatch);
  return ret;
}


/** @todo slottedPreRalloc ignores it's xid parameter; change the
    interface?  (The xid is there for now, in case it allows some
    optimizations later.  Perhaps it's better to cluster allocations
    from the same xid on the same page, or something...)
*/
recordid slottedPreRalloc(int xid, long size, Page ** pp) {
  
  recordid ret;
  
  int isBlob = 0;

  if(size == BLOB_SLOT) {
    isBlob = 1;
    size = sizeof(blob_record_t);
  }

  assert(size < BLOB_THRESHOLD_SIZE);


  /** @todo is ((unsigned int) foo) == -1 portable?  Gotta love C.*/

  if(lastFreepage == -1) {
    lastFreepage = TpageAlloc(xid);
    *pp = loadPage(lastFreepage);
    assert(*page_type_ptr(*pp) == UNINITIALIZED_PAGE);
    slottedPageInitialize(*pp);
  } else {
    *pp = loadPage(lastFreepage);
  }


  if(slottedFreespace(*pp) < size ) { 
    releasePage(*pp);
    lastFreepage = TpageAlloc(xid);
    *pp = loadPage(lastFreepage);
    slottedPageInitialize(*pp);
  }
  
  ret = slottedRawRalloc(*pp, size);
  
  if(isBlob) {
    *slot_length_ptr(*pp, ret.slot) = BLOB_SLOT;
  }

  DEBUG("alloced rid = {%d, %d, %ld}\n", ret.page, ret.slot, ret.size); 

  return ret;
}

recordid slottedPreRallocFromPage(int xid, long page, long size, Page **pp) {
  recordid ret;
  int isBlob = 0;
  if(size == BLOB_SLOT) {
    isBlob = 1;
    size = sizeof(blob_record_t);
  }

  *pp = loadPage(page);
  
  if(slottedFreespace(*pp) < size) {
    releasePage(*pp);
    *pp = NULL;
    recordid rid;
    rid.page = 0;
    rid.slot = 0;
    rid.size = -1;
    return rid;
  }
  
  if(*page_type_ptr(*pp) == UNINITIALIZED_PAGE) {
    slottedPageInitialize(*pp);
  }
  assert(*page_type_ptr(*pp) == SLOTTED_PAGE);
  ret = slottedRawRalloc(*pp, size);
  if(isBlob) {
    *slot_length_ptr(*pp, ret.slot) = BLOB_SLOT;
  }
  return ret;
  
}

recordid slottedRawRalloc(Page * page, int size) {

	writelock(page->rwlatch, 342);

	recordid rid;

	rid.page = page->id;
	rid.slot = *numslots_ptr(page);
	rid.size = size;

	/* new way - The freelist_ptr points to the first free slot number, which 
	   is the head of a linked list of free slot numbers.*/
	if(*freelist_ptr(page) != INVALID_SLOT) {
	  rid.slot = *freelist_ptr(page);
	  *freelist_ptr(page) = *slot_length_ptr(page, rid.slot);
	  *slot_length_ptr(page, rid.slot) = 0;
	}  
	  
	__really_do_ralloc(page, rid);

	/*	DEBUG("slot: %d freespace: %d\n", rid.slot, freeSpace); */

	writeunlock(page->rwlatch);

	return rid;
}

static void __really_do_ralloc(Page * page, recordid rid) {

  int freeSpace;
  
  int isBlob = 0;

  if(rid.size == BLOB_SLOT) {
    isBlob = 1;
    rid.size = sizeof(blob_record_t);
  }

  assert(rid.size > 0);
  
  if(slottedFreespaceUnlocked(page) < rid.size) {
    slottedCompact(page);
    
    /* Make sure there's enough free space... */
    assert (slottedFreespaceUnlocked(page) >= rid.size);
  }
  
  freeSpace = *freespace_ptr(page);
  

  if(*numslots_ptr(page) <= rid.slot) {
    /*    printf("Incrementing numSlots."); */
    *numslots_ptr(page) = rid.slot + 1;
  }

  DEBUG("Num slots %d\trid.slot %d\n", *numslots_ptr(page), rid.slot);

  *freespace_ptr(page) = freeSpace + rid.size;

  *slot_ptr(page, rid.slot)  = freeSpace;
  /*  assert(!*slot_length_ptr(page, rid.slot) || (-1 == *slot_length_ptr(page, rid.slot)));*/
  if(isBlob) {
    *slot_length_ptr(page, rid.slot = BLOB_SLOT);
  } else {
    *slot_length_ptr(page, rid.slot) = rid.size; 
  }

}

recordid slottedPostRalloc(Page * page, lsn_t lsn, recordid rid) {

	writelock(page->rwlatch, 376);

	if(*page_type_ptr(page) != SLOTTED_PAGE) {
	  /* slottedPreRalloc calls this when necessary.  However, in
	     the case of a crash, it is possible that
	     slottedPreRalloc's updates were lost, so we need to check
	     for that here.  

	     If slottedPreRalloc didn't call slottedPageInitialize,
	     then there would be a race condition:
	
	     Thread 1             Thread 2
	     preAlloc(big record)

	                          preAlloc(big record) // Should check the freespace of the page and fail
	                          postAlloc(big record)

             postAlloc(big record)  // Thread 2 stole my space! => Crash?

	     Note that this _will_ cause trouble if recovery is
	     multi-threaded, and allows the application to begin
	     updating the storefile without first locking any pages
	     that suffer from this problem.

	  */

	  slottedPageInitialize(page);  
	}

	if((*slot_length_ptr(page, rid.slot) == 0) || (*slot_ptr(page, rid.slot) == INVALID_SLOT)) {
	  /*	if(*slot_ptr(page, rid.slot) == INVALID_SLOT) { */

	  __really_do_ralloc(page, rid);
	
       	} else {

	  /*	  int ijk = rid.size;
		  int lmn = *slot_length_ptr(page, rid.slot); */

	   assert((rid.size == *slot_length_ptr(page, rid.slot)) ||
		  (*slot_length_ptr(page, rid.slot) >= PAGE_SIZE));

	}

	pageWriteLSN(page, lsn);

	writeunlock(page->rwlatch);

	return rid;
}

void slottedDeRalloc(Page * page, lsn_t lsn, recordid rid) {

  readlock(page->rwlatch, 443);

  *slot_ptr(page, rid.slot) =  INVALID_SLOT;
  *slot_length_ptr(page, rid.slot) = *freelist_ptr(page); 
  *freelist_ptr(page) = rid.slot;  
  /*  *slot_length_ptr(page, rid.slot) = 0; */

  pageWriteLSN(page, lsn);

  unlock(page->rwlatch);
}

void slottedReadUnlocked(int xid, Page * page, recordid rid, byte *buff) {
  int slot_length;

  assert(page->id == rid.page);
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(buff, record_ptr(page, rid.slot),  rid.size)) {
    perror("memcpy");
    abort();
  }

}

/*
  This should trust the rid (since the caller needs to
  override the size in special circumstances)

  @todo If the rid size has been overridden, we should check to make
  sure that this really is a special record.
*/
void slottedRead(int xid, Page * page, recordid rid, byte *buff) {

  int slot_length;
  readlock(page->rwlatch, 519);

  assert(page->id == rid.page);
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(buff, record_ptr(page, rid.slot),  rid.size)) {
    perror("memcpy");
    abort();
  }

  unlock(page->rwlatch);
  
}

void slottedWrite(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {
  int slot_length;

  readlock(page->rwlatch, 529);  
  

  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, rid.size)) {
    perror("memcpy");
    abort();
  }

  /*page->LSN = lsn;
    *lsn_ptr(page) = lsn * / 
  pageWriteLSN(page); */
  unlock(page->rwlatch); 

}
void slottedWriteUnlocked(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {
  int slot_length;

  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) ||  (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, rid.size)) {
    perror("memcpy");
    abort();
  }
}

/*void slottedSetType(Page * p, int slot, int type) {
  assert(type > PAGE_SIZE);
  writelock(p->rwlatch, 686);
  *slot_length_ptr(p, slot) = type;
  unlock(p->rwlatch);
}

int slottedGetType(Page *  p, int slot) {
  int ret; 
  readlock(p->rwlatch, 693);
  ret = *slot_length_ptr(p, slot);
  unlock(p->rwlatch);

  / * getSlotType does the locking for us. * /
  return ret > PAGE_SIZE ? ret : NORMAL_SLOT;
  }*/
