/** $Id$ */


#include "../page.h"
#include "../blobManager.h"
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
static void pageCompact(Page * page) {

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

	pageInitialize(&bufPage);

	numSlots = *numslots_ptr(page);
	for (i = 0; i < numSlots; i++) {
	  /*	  printf("i = %d\n", i);   */
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

void pageInitialize(Page * page) {
  /*  printf("Initializing page %d\n", page->id);
      fflush(NULL); */
  memset(page->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(page) = SLOTTED_PAGE;
  *freespace_ptr(page) = 0;
  *numslots_ptr(page)  = 0;
  *freelist_ptr(page)  = INVALID_SLOT;
}

int unlocked_freespace(Page * page) {
  return (int)slot_length_ptr(page, *numslots_ptr(page)) - (int)(page->memAddr + *freespace_ptr(page));
}

/**
 * freeSpace() assumes that the page is already loaded in memory.  It takes 
 * as a parameter a Page, and returns an estimate of the amount of free space
 * available to a new slot on this page.  (This is the amount of unused space 
 * in the page, minus the size of a new slot entry.)  This is either exact, 
 * or an underestimate.
 *
 * @todo is it ever safe to call freespace without a lock on the page? 
 * 
 */
int freespace(Page * page) {
  int ret;
  readlock(page->rwlatch, 292);
  ret = unlocked_freespace(page);
  readunlock(page->rwlatch);
  return ret;
}


/**
   @todo pageRalloc's algorithm for reusing slot id's reclaims the
   highest numbered slots first, which encourages fragmentation.
*/
recordid pageRalloc(Page * page, int size) {

	writelock(page->rwlatch, 342);

	recordid rid;

	rid.page = page->id;
	rid.slot = *numslots_ptr(page);
	rid.size = size;

	/* new way */
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

  assert(rid.size > 0);
  
  if(unlocked_freespace(page) < rid.size) {
    pageCompact(page);
    
    /* Make sure there's enough free space... */
    assert (unlocked_freespace(page) >= rid.size);
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
  *slot_length_ptr(page, rid.slot) = rid.size; 

}

/** Only used for recovery, to make sure that consistent RID's are created 
 * on log playback. */
recordid pageSlotRalloc(Page * page, lsn_t lsn, recordid rid) {

	writelock(page->rwlatch, 376);

	if(*slot_length_ptr(page, rid.slot) == 0 /*|| *slot_length_ptr(page, rid.slot) == -1*/) {

	  __really_do_ralloc(page, rid);
	
       	} else {

	   assert((rid.size == *slot_length_ptr(page, rid.slot)) ||
		  (*slot_length_ptr(page, rid.slot) >= PAGE_SIZE));

	}

	writeunlock(page->rwlatch);

	return rid;
}


void pageDeRalloc(Page * page, recordid rid) {

  readlock(page->rwlatch, 443);

  *slot_ptr(page, rid.slot) =  INVALID_SLOT;
  *slot_length_ptr(page, rid.slot) = *freelist_ptr(page);
  *freelist_ptr(page) = rid.slot; 
  
  unlock(page->rwlatch);
}

/*
  This should trust the rid (since the caller needs to
  override the size in special circumstances)

  @todo If the rid size has been overridden, we should check to make
  sure that this really is a special record.
*/
void pageReadRecord(int xid, Page * page, recordid rid, byte *buff) {

  int slot_length;
  readlock(page->rwlatch, 519);

  assert(page->id == rid.page);
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (slot_length >= PAGE_SIZE));

  if(!memcpy(buff, record_ptr(page, rid.slot),  rid.size)) {
    perror("memcpy");
    abort();
  }

  unlock(page->rwlatch);
  
}

void pageWriteRecord(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {
  int slot_length;

  readlock(page->rwlatch, 529);  
  

  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, rid.size)) {
    perror("memcpy");
    abort();
  }

  /*page->LSN = lsn;
    *lsn_ptr(page) = lsn * / 
  pageWriteLSN(page); */
  unlock(page->rwlatch); 

}


/** @todo:  Should the caller need to obtain the writelock when calling pageSetSlotType? */
void pageSetSlotType(Page * p, int slot, int type) {
  assert(type > PAGE_SIZE);
  writelock(p->rwlatch, 686);
  *slot_length_ptr(p, slot) = type;
  unlock(p->rwlatch);
}

int pageGetSlotType(Page *  p, int slot, int type) {
  int ret; 
  readlock(p->rwlatch, 693);
  ret = *slot_length_ptr(p, slot);
  unlock(p->rwlatch);

  /* getSlotType does the locking for us. */
  return ret > PAGE_SIZE ? ret : NORMAL_SLOT;
}


/*
typedef struct {
  int page;
  int slot;
  / ** If pageptr is not null, then it is used by the iterator methods.
      Otherwise, they re-load the pages and obtain short latches for
      each call. * /
  Page * pageptr;  
} page_iterator_t;



void pageIteratorInit(recordid rid, page_iterator_t * pit, Page * p) {
  pit->page = rid.page;
  pit->slot = rid.slot;
  pit->pageptr = p;
  assert((!p) || (p->id == pit->page));
}

int nextSlot(page_iterator_t * pit, recordid * rid) {
  Page * p;
  int numSlots;
  int done = 0;
  int ret;
  if(pit->pageptr) {
    p = pit->pageptr;
  } else {
    p = loadPage(pit->page);
  }

  numSlots = readNumSlots(p->memAddr);
  while(pit->slot < numSlots && !done) {
    
    if(isValidSlot(p->memAddr, pit->slot)) {
      done = 1;
    } else {
      pit->slot ++;
    }

  }
  if(!done) {
    ret = 0;
  } else {
    ret = 1;
    rid->page = pit->page;
    rid->slot = pit->slot;
    rid->size = getSlotLength(p->memAddr, rid->slot);
    if(rid->size >= PAGE_SIZE) {

      if(rid->size == BLOB_SLOT) {
	blob_record_t br;
	pageReadRecord(-1, p, *rid, (byte*)&br);
	rid->size = br.size;
      }
    }
  }

  if(!pit->pageptr) {
    releasePage(p);
  }

  return ret;
  
}
*/

