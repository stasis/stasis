#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include "../page.h"
#include <lladd/operations/pageOperations.h>
#include <assert.h>
/*#include "../page/slotted.h"*/
#include "../page/header.h"
#include "../pageFile.h"

static int freelist;
static int freepage;
static pthread_mutex_t pageAllocMutex;

/*int __pageAlloc(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  int type = *(int*)d;

  *page_type_ptr(p) = type;
  / ** @todo this sort of thing should be done in a centralized way. * /
  if(type == SLOTTED_PAGE) {
    slottedPageInitialize(p);
  }

  return 0;

}

int __pageDealloc(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  *page_type_ptr(p) = UNINITIALIZED_PAGE;
  return 0;
}
*/
int __pageSet(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  memcpy(p->memAddr, d, PAGE_SIZE);
  pageWriteLSN(p, lsn);
  return 0;
}

typedef struct {
  int before;
  int after;
} update_tuple;

int __update_freespace(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  assert(r.page == 0);
  const update_tuple * t = d;
  /*  printf("freespace %d -> %d\n", t->before, t->after); 
      fflush(NULL); */
  * headerFreepage_ptr(p) = t->after;
  freepage = t->after;
  pageWriteLSN(p, lsn);
  return 0;
}

int __update_freespace_inverse(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
#ifdef REUSE_PAGES
  assert(r.page == 0);
  const update_tuple * t = d;
  /*  printf("freespace %d <- %d\n", t->before, t->after);
      fflush(NULL); */

  * headerFreepage_ptr(p) = t->before;
  freepage = t->before;
#endif
  pageWriteLSN(p, lsn);
  return 0;
}
/** @todo need to hold mutex here... */
int __update_freelist(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  assert(r.page == 0);

  const update_tuple * t = d;

  /*  printf("freelist %d -> %d\n", t->before, t->after);
      fflush(NULL); */

  * headerFreepagelist_ptr(p) = t->after;
  freelist = t->after;
  pageWriteLSN(p, lsn);
  return 0;
}
int __update_freelist_inverse(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  assert(r.page == 0);
  const update_tuple * t = d;

  /*  printf("freelist %d <- %d\n", t->before, t->after); 
      fflush(NULL); */

  * headerFreepagelist_ptr(p) = t->before;
  freelist = t->before;
  pageWriteLSN(p, lsn);
  return 0;
}

int __free_page(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  const int * successor = d;
  /*  printf("Unallocing page %d\n", r.page); 
      fflush(NULL); */
  memset(p->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(p) = LLADD_FREE_PAGE;
  *nextfreepage_ptr(p) = *successor;
  pageWriteLSN(p, lsn);
  return 0;
}

int __alloc_freed(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  memset(p->memAddr, 0, PAGE_SIZE);
  pageWriteLSN(p, lsn); 
  return 0;
}

int TpageGet(int xid, int pageid, byte *memAddr) {
  Page * q = loadPage(pageid);
  memcpy(memAddr, q->memAddr, PAGE_SIZE);
  releasePage(q);
  return 0;
}

int TpageSet(int xid, int pageid, byte * memAddr) {
  recordid rid;
  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;
  Tupdate(xid,rid,memAddr, OPERATION_PAGE_SET);
  return 0;
}


/** This needs to be called immediately after the storefile is opened,
    since it needs to perform raw, synchronous I/O on the pagefile for
    bootstrapping purposes. */
void pageOperationsInit() {
  Page p;
  p.rwlatch = initlock();
  p.loadlatch = initlock();
  assert(!posix_memalign((void **)&(p.memAddr), PAGE_SIZE, PAGE_SIZE));
  p.id = 0;

  pageRead(&p);

  if(*page_type_ptr(&p) != LLADD_HEADER_PAGE) {
    headerPageInitialize(&p);
    pageWrite(&p);
  }

  freelist = *headerFreepagelist_ptr(&p);
  freepage = *headerFreepage_ptr(&p);

  assert(freepage);

  /*  free(p.memAddr); */
  
  deletelock(p.loadlatch);
  deletelock(p.rwlatch);

  pthread_mutex_init(&pageAllocMutex, NULL);

}


/** @todo TpageAlloc / TpageDealloc + undo is not multi-transaction / threadsafe. 
    
   Example of the problem:

   T1                              T2
   dealloc(100)
  (a)    list ptr 30 -> 100
  (b)    p(100) nil -> 30
                                   alloc() -> 100       <- Can't allow this to happen!
                                   list_ptr 100 -> 30
                                   alloc() -> 30
                                   list_ptr 30 -> 20
  abort();

  // Really just needs to remove 100 from the linked list.  Instead,
  we use physical, value based locking. 

  list ptr 20 <- 30   <- Oops! Page 30 is in use, and we lose the rest 
                         of the freelist, starting at 20!
      
  The partial solution: dealloc() aquires a lock on the freelist until
  commit / abort.  If other transactions need to allocate when the
  lock is held, then they simply do not reuse pages.  Since locking is
  not yet implemented, we require applications to manually serialize
  transactions that call Talloc() or TdeAlloc

  A better solution: defer the addition of 100 to the freelist until
  commit, and use a 'real' data structure, like a concurrent B-Tree.

*/

int TpageDealloc(int xid, int pageid) {
  recordid rid;
#ifdef REUSE_PAGES

  update_tuple t;

  pthread_mutex_lock(&pageAllocMutex);
#endif

  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;

#ifdef REUSE_PAGES
  assert(freelist != pageid);
    

  t.before = freelist;  

#endif
  

  Tupdate(xid, rid, &freelist, OPERATION_FREE_PAGE);
  
#ifdef REUSE_PAGES
  t.after = pageid;
  freelist = pageid;
  
  rid.page = 0;
  Tupdate(xid, rid, &t,        OPERATION_UPDATE_FREELIST);

  

  /* OLD STUFF:  Page * p = loadPage(pageid);  int type = *page_type_ptr(p);   releasePage(p);   Tupdate(xid, rid, &type, OPERATION_PAGE_DEALLOC); */

  pthread_mutex_unlock(&pageAllocMutex); 
#endif

  return 0;
}

int TpageAlloc(int xid /*, int type */) {
  recordid rid;
  update_tuple t;
  rid.slot = 0;
  rid.size = 0;
  
  pthread_mutex_lock(&pageAllocMutex);
  int newpage;
#ifdef REUSE_PAGES
  if(freelist) {
    
    printf("Re-using old page: %d\n", freelist);
    fflush(NULL);
    newpage = freelist;

    Page * p = loadPage(newpage);  /* Could obtain write lock here,
				      but this is the only function
				      that should ever touch pages of
				      type LLADD_FREE_PAGE, and we
				      already hold a mutex... */
    assert(*page_type_ptr(p) == LLADD_FREE_PAGE);
    t.before = freelist;
    freelist = *nextfreepage_ptr(p);
    t.after = freelist;
    assert(newpage != freelist);
    releasePage(p);

    rid.page = newpage;
    Tupdate(xid, rid, &freelist, OPERATION_ALLOC_FREED);

    rid.page = 0;
    Tupdate(xid, rid, &t,        OPERATION_UPDATE_FREELIST);

    rid.page = newpage;
    
    } else {
#endif
    /*     printf("Allocing new page: %d\n", freepage);
	   fflush(NULL); */
    
    t.before = freepage;
    newpage = freepage;
    freepage++;
    t.after = freepage;
    /* Don't need to touch the new page. */
    
    rid.page = 0;
    Tupdate(xid, rid, &t,        OPERATION_UPDATE_FREESPACE);

    rid.page = newpage;
#ifdef REUSE_PAGES
      } 
#endif

  pthread_mutex_unlock(&pageAllocMutex);

  return newpage;
}

int TpageAllocMany(int xid, int count /*, int type*/) {
  /*  int firstPage = -1;
      int lastPage = -1; */
  recordid rid;
  rid.slot = 0;
  rid.size = 0;
  
  update_tuple t;
  pthread_mutex_lock(&pageAllocMutex);

  t.before = freepage;
  int newpage = freepage;
  freepage += count;
  t.after = freepage;

  /* Don't need to touch the new pages. */
    
  rid.page = 0;
  Tupdate(xid, rid, &t,        OPERATION_UPDATE_FREESPACE);

  rid.page = newpage;

  pthread_mutex_unlock(&pageAllocMutex);
  return newpage;
}

/** Safely allocating and freeing pages is suprisingly complex.  Here is a summary of the process:

   Alloc:

     obtain mutex
        choose a free page using in-memory data
	load page to be used, and update in-memory data.  (obtains lock on loaded page)
	Tupdate() the page, zeroing it, and saving the old successor in the log.
	relase the page (avoid deadlock in next step)
	Tupdate() LLADD's header page (the first in the store file) with a new copy of 
	                              the in-memory data, saving old version in the log.
     release mutex

   Free: 

     obtain mutex
        determine the current head of the freelist using in-memory data
        Tupdate() the page, initializing it to be a freepage, and physically logging the old version
	release the page
	Tupdate() LLADD's header page with a new copy of the in-memory data, saving old version in the log
     release mutex

*/

Operation getUpdateFreespace() {
  Operation o = {
    OPERATION_UPDATE_FREESPACE,
    sizeof(update_tuple),
    OPERATION_UPDATE_FREESPACE_INVERSE,
    &__update_freespace
  };
  return o;
}

Operation getUpdateFreespaceInverse() {
  Operation o = {
    OPERATION_UPDATE_FREESPACE_INVERSE,
    sizeof(update_tuple),
    OPERATION_UPDATE_FREESPACE,
    &__update_freespace_inverse
  };
  return o;
}


Operation getUpdateFreelist() {
  Operation o = {
    OPERATION_UPDATE_FREELIST,
    sizeof(update_tuple),
    OPERATION_UPDATE_FREELIST_INVERSE,
    &__update_freelist
  };
  return o;
}

Operation getUpdateFreelistInverse() {
  Operation o = {
    OPERATION_UPDATE_FREELIST_INVERSE,
    sizeof(update_tuple),
    OPERATION_UPDATE_FREELIST,
    &__update_freelist_inverse
  };
  return o;
}

/** frees a page by zeroing it, setting its type to LLADD_FREE_PAGE,
    and setting the successor pointer. This operation physically logs
    a whole page, which makes it expensive.  Doing so is necessary in
    general, but it is possible that application specific logic could
    avoid the physical logging here. */
Operation getFreePageOperation() {
  Operation o = {
    OPERATION_FREE_PAGE,
    sizeof(int),
    NO_INVERSE_WHOLE_PAGE,
    &__free_page
  };
  return o;
}

/** allocs a page that was once freed by zeroing it. */
Operation getAllocFreedPage() {
  Operation o = {
    OPERATION_ALLOC_FREED,
    sizeof(int),
    OPERATION_UNALLOC_FREED,
    &__alloc_freed
  };
  return o;
}
/** does the same thing as getFreePageOperation, but doesn't log a preimage.  (Used to undo an alloc of a freed page.) */
Operation getUnallocFreedPage() {
  Operation o = {
    OPERATION_UNALLOC_FREED,
    sizeof(int),
    OPERATION_ALLOC_FREED,
    &__free_page
  };
  return o;
}



/*Operation getPageAlloc() {
  Operation o = {
    OPERATION_PAGE_ALLOC,
    sizeof(int),
    OPERATION_PAGE_DEALLOC,
    &__pageAlloc
  };
  return o;
}

Operation getPageDealloc() {
  Operation o = {
    OPERATION_PAGE_DEALLOC,
    sizeof(int),
    OPERATION_PAGE_ALLOC,
    &__pageDealloc
  };
  return o;
  }*/

Operation getPageSet() {
  Operation o = {
    OPERATION_PAGE_SET,
    PAGE_SIZE,              /* This is the type of the old page, for undo purposes */
    /*OPERATION_PAGE_SET, */  NO_INVERSE_WHOLE_PAGE, 
    &__pageSet
  };
  return o;
}


