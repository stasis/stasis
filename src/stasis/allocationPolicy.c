/**
   This file implements allocation policies.  It's (hopefully) general
   enough to be reused by different allocators.

   It's designed to support Hoard style allocators, which are
   described in this paper:

   Emery D. Berger, Kathryn S. McKinley, Robert D. Blumofe and Paul
     R. Wilson.  "Hoard: a scalable memory allocator for multithreaded
     applications", ACM SIGPLAN Vol 35 #11 pp. 117-128, November 2000.

   http://doi.acm.org/10.1145/356989.357000

   The idea is that a transaction is like a thread, but shorter lived.
   For correctness of recovery and abort(), each transaction must
   maintain locks on all pages it has *deallocated* records from.  In
   practice, each transaction gets its own page to improve the
   physical locality of the records it allocates.  Once the
   transaction is over, another transaction may allocate from the
   page.

   Because slot allocation is already pretty heavy weight, this file
   ignores the overhead of tree / hash lookups and mutex acquisition
   for now.  

   @todo: Right now, allocation policies lump all nested top actions within a transaction into a single allocation group.  It could do better if it knew when NTA's began and committed.

*/

#include <assert.h>
#include <stdlib.h>
#include <stasis/allocationPolicy.h>
#include <stasis/lhtable.h>
#include <stasis/redblack.h>
#include <stasis/transactional.h>

#define ALLOCATION_POLICY_SANITY_CHECKS

// Each availablePage should either be in availablePages, or in 
// xidAlloced and pageOwners.  If a transaction allocs and
// deallocs from the same page, then it only has an entry for that
// page in xidAlloced.
//
// xidAlloced is an lhtable of type (int xid) -> (rbtree of availablePage*)
// xidDealloced is an lhtable of type (int xid) -> (lhtable of int pageid -> availablePage *) 
// pageOwners is an lhtable of type (int pageid) -> (int xid)
// availablePages is a rbtree of availablePage*.

struct allocationPolicy { 
  struct LH_ENTRY(table) * xidAlloced;
  struct LH_ENTRY(table) * xidDealloced;
  struct RB_ENTRY(tree)  * availablePages;
  struct LH_ENTRY(table) * pageOwners;
  struct LH_ENTRY(table) * allPages;
};

inline static int cmpPageid(const void * ap, const void * bp, const void * param) {
  const availablePage * a = (const availablePage *)ap;
  const availablePage * b = (const availablePage *)bp;

  if(a->pageid < b->pageid) { 
    return -1;
  } else if (a->pageid > b->pageid) { 
    return 1;
  } else {
    return 0;
  }
}

static int cmpFreespace(const void * ap, const void * bp, const void * param) { 
  const availablePage * a = (const availablePage *) ap;
  const availablePage * b = (const availablePage *) bp;

  if(a->freespace < b->freespace) { 
    return -1;
  } else if (a->freespace > b->freespace) { 
    return 1;
  } else { 
    return cmpPageid(ap,bp,param);
  }
}

inline static availablePage* getAvailablePage(allocationPolicy * ap, int pageid) {
  return  (availablePage*) LH_ENTRY(find)(ap->allPages, &pageid, sizeof(int));
}

inline static void insert_xidAlloced(allocationPolicy * ap, int xid, availablePage * p) { 

  struct RB_ENTRY(tree) * pages = LH_ENTRY(find)(ap->xidAlloced, &xid, sizeof(xid));
  if(!pages) { 
    pages = RB_ENTRY(init)(cmpFreespace, 0);
    LH_ENTRY(insert)(ap->xidAlloced, &xid, sizeof(xid), pages);
  }
  const availablePage * check = RB_ENTRY(search)(p, pages);
  assert(check == p);
}

inline static void remove_xidAlloced(allocationPolicy * ap, int xid, availablePage * p) { 
  struct RB_ENTRY(tree) * pages = LH_ENTRY(find)(ap->xidAlloced, &xid, sizeof(xid));
  assert(pages);
  const availablePage * check = RB_ENTRY(delete)(p, pages);
  assert(check == p);
}

inline static void insert_xidDealloced(allocationPolicy * ap, int xid, availablePage * p) { 
  
  struct RB_ENTRY(tree) * pages = LH_ENTRY(find)(ap->xidDealloced, &xid, sizeof(xid));
  if(!pages) { 
    pages = RB_ENTRY(init)(cmpPageid, 0);
    LH_ENTRY(insert)(ap->xidDealloced, &xid, sizeof(xid), pages);
  }
  assert(pages);
  const availablePage * check = RB_ENTRY(search)(p, pages);
  // XXX Assert that the page wasn't already there?
  assert(check);
}

inline static void remove_xidDealloced(allocationPolicy * ap, int xid, availablePage * p) { 
  
  struct RB_ENTRY(tree) * pages = LH_ENTRY(find)(ap->xidDealloced, &xid, sizeof(xid));
  assert(pages);
  const availablePage * check = RB_ENTRY(delete)(p, pages);
  assert(check == p);
}

inline static int find_xidDealloced(allocationPolicy * ap, int xid, availablePage * p) { 
  struct RB_ENTRY(tree) * pages = LH_ENTRY(find)(ap->xidDealloced, &xid, sizeof(xid));
  if(!pages) { return 0; } 
  const availablePage * check = RB_ENTRY(find)(p, pages);
  if(check) { 
    return 1; 
  } else { 
    return 0; 
  }
}

inline static void     lockAlloced(allocationPolicy * ap, int xid, availablePage * p) {
  const availablePage * check = RB_ENTRY(delete)(p, ap->availablePages);
  assert(check == p);

  assert(p->lockCount == 0);
  p->lockCount = 1;

  int * xidp = malloc(sizeof(int));
  *xidp = xid;
  LH_ENTRY(insert)(ap->pageOwners, &(p->pageid), sizeof(int), xidp);
  insert_xidAlloced(ap, xid, p);
}

inline static void   unlockAlloced(allocationPolicy * ap, int xid, availablePage * p) { 
  remove_xidAlloced(ap, xid, p);

  assert(p->lockCount == 1);
  p->lockCount = 0;

  const availablePage * check = RB_ENTRY(search)(p, ap->availablePages);
  assert(check == p);
  int * xidp = LH_ENTRY(remove)(ap->pageOwners, &(p->pageid), sizeof(p->pageid));
  assert(*xidp == xid);
  free(xidp);

}

inline static void lockDealloced(allocationPolicy * ap, int xid, availablePage * p) { 
  if(p->lockCount == 0) { 
    // xid should own it
    lockAlloced(ap, xid, p);
  } else if(p->lockCount == 1) {
    int * xidp = LH_ENTRY(find)(ap->pageOwners, &(p->pageid), sizeof(int));
    if(!xidp) { 
      
      // The only active transaction that touched this page deallocated from it, 
      // so just add the page to our dealloced table.

      p->lockCount++;
      insert_xidDealloced(ap, xid, p);

    } else if(*xidp != xid) {

      // Remove from the other transaction's "alloced" table.
      remove_xidAlloced(ap, *xidp, p);
      assert(p->lockCount == 1);

      // Place in other transaction's "dealloced" table.
      insert_xidDealloced(ap, *xidp, p);

      // This page no longer has an owner
      LH_ENTRY(remove)(ap->pageOwners, &(p->pageid), sizeof(p->pageid));
      free(xidp);

      // Add to our "dealloced" table, increment lockCount.
      p->lockCount++;
      insert_xidDealloced(ap, xid, p);
    }
  } else { 
    // not owned by anyone... is it already in this xid's Dealloced table? 
    if(!find_xidDealloced(ap, xid, p)) { 
      p->lockCount++;
      insert_xidDealloced(ap, xid, p);
    }
  }
}

inline static void unlockDealloced(allocationPolicy * ap, int xid, availablePage * p) { 
  assert(p->lockCount);
  p->lockCount--;
  assert(p->lockCount >= 0);
  remove_xidDealloced(ap, xid, p);
  if(!p->lockCount) { 
    // put it back into available pages.
    const availablePage * check = RB_ENTRY(search)(p, ap->availablePages);
    assert(check == p);
  }
}

allocationPolicy * allocationPolicyInit() { 
  allocationPolicy * ap = malloc(sizeof(allocationPolicy));

  ap->xidAlloced = LH_ENTRY(create)(10);
  ap->xidDealloced = LH_ENTRY(create)(10);
  ap->availablePages = RB_ENTRY(init)(cmpFreespace, 0);
  ap->pageOwners = LH_ENTRY(create)(10);
  ap->allPages = LH_ENTRY(create)(10);
  return ap;
}

void allocationPolicyDeinit(allocationPolicy * ap) { 
  
  const availablePage * next;
  while(( next = RB_ENTRY(min)(ap->availablePages) )) { 
    RB_ENTRY(delete)(next, ap->availablePages);
    free((void*)next);
  }

  LH_ENTRY(destroy)(ap->xidAlloced);
  RB_ENTRY(destroy)(ap->availablePages);
  LH_ENTRY(destroy)(ap->pageOwners);
  free(ap);
}

void allocationPolicyAddPages(allocationPolicy * ap, availablePage** newPages) {

  for(int i = 0; newPages[i] != 0; i++) {
    const availablePage * ret = RB_ENTRY(search)(newPages[i], ap->availablePages);
    assert(ret == newPages[i]);
    LH_ENTRY(insert)(ap->allPages, &(newPages[i]->pageid), sizeof(newPages[i]->pageid), newPages[i]);

  }
  
}

/// XXX need updateAlloced, updateFree, which remove a page, change
/// its freespace / lockCount, and then re-insert them into the tree.

availablePage * allocationPolicyFindPage(allocationPolicy * ap, int xid, int freespace) {
  // For the best fit amongst the pages in availablePages, call:
  //
  //    rblookup(RB_LUGREAT, key, availablePages) 
  // 
  // For the page with the most freespace, call:
  // 
  //    rbmax(availablePages);
  //
  availablePage tmp = { .freespace = freespace, .pageid = 0, .lockCount = 0 };

  struct RB_ENTRY(tree) * locks;

  // If we haven't heard of this transaction yet, then create an entry
  // for it.
  
  if(0 == (locks = LH_ENTRY(find)(ap->xidAlloced, &xid, sizeof(xid)))) { 
    // Since this is cmpPageid, we can change the amount of freespace
    // without modifying the tree.
    locks = RB_ENTRY(init)(cmpFreespace, 0); 
    LH_ENTRY(insert)(ap->xidAlloced, &xid, sizeof(xid), locks);
 } 

  const availablePage *ret; 

  // Does this transaction already have an appropriate page?

  if(!(ret = RB_ENTRY(lookup)(RB_LUGREAT, &tmp, locks))) {

    // No; get a page from the availablePages. 

    ret = RB_ENTRY(lookup)(RB_LUGREAT, &tmp, ap->availablePages);
    if(ret) {
      assert(ret->lockCount == 0);
      lockAlloced(ap, xid, (availablePage*) ret);
    }
  }

  // Done.  (If ret is null, then it's the caller's problem.)
  return (availablePage*) ret; 
}

void allocationPolicyAllocedFromPage(allocationPolicy *ap, int xid, int pageid) { 
  availablePage * p = getAvailablePage(ap, pageid);
  const availablePage * check1 = RB_ENTRY(find)(p, ap->availablePages);
  int * xidp = LH_ENTRY(find)(ap->pageOwners, &(pageid), sizeof(pageid));
  assert(xidp || check1);
  if(check1) {
    assert(p->lockCount == 0);
    lockAlloced(ap, xid, (availablePage*)p);
  }
}

void allocationPolicyLockPage(allocationPolicy *ap, int xid, int pageid) { 

  availablePage * p = getAvailablePage(ap, pageid);
  lockDealloced(ap, xid, p);

}


void allocationPolicyTransactionCompleted(allocationPolicy * ap, int xid) {

  struct RB_ENTRY(tree) * locks = LH_ENTRY(find)(ap->xidAlloced, &xid, sizeof(int));

  if(locks) {

    const availablePage * next;

    while(( next = RB_ENTRY(min)(locks) )) { 
      unlockAlloced(ap, xid, (availablePage*)next);           // This is really inefficient.  (We're wasting hashtable lookups.  Also, an iterator would be faster.)
    }

    LH_ENTRY(remove)(ap->xidAlloced, &xid, sizeof(int));
    RB_ENTRY(destroy)(locks);

  }

  locks = LH_ENTRY(find)(ap->xidDealloced, &xid, sizeof(int));

  if(locks) {
    const availablePage * next;
    
    while(( next = RB_ENTRY(min)(locks) )) { 
      unlockDealloced(ap, xid, (availablePage*)next);         // This is really inefficient.  (We're wasting hashtable lookups.  Also, an iterator would be faster.)
    }

    LH_ENTRY(remove)(ap->xidDealloced, &xid, sizeof(int));
    RB_ENTRY(destroy)(locks);

  }

}

void allocationPolicyUpdateFreespaceUnlockedPage(allocationPolicy * ap, availablePage * key, int newFree) {
  availablePage * p = (availablePage*) RB_ENTRY(delete)(key, ap->availablePages);
  p->freespace = newFree;
  const availablePage * ret = RB_ENTRY(search)(p, ap->availablePages);
  assert(ret == p);
}

void allocationPolicyUpdateFreespaceLockedPage(allocationPolicy * ap, int xid, availablePage * key, int newFree) {
  struct RB_ENTRY(tree) * locks = LH_ENTRY(find)(ap->xidAlloced, &xid, sizeof(int));
  availablePage * p = (availablePage*) RB_ENTRY(delete)(key, locks);
  assert(p);
  p->freespace = newFree;
  key->freespace = newFree;
  const availablePage * ret = RB_ENTRY(search)(p, locks);
  assert(ret == p);
  assert(p->lockCount == 1);
}
