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

*/

#include <assert.h>
#include <stdlib.h>
#include <lladd/allocationPolicy.h>
#include <lladd/lhtable.h>
#include <lladd/redblack.h>
#include <lladd/transactional.h>

struct allocationPolicy { 
  struct LH_ENTRY(table) * xidLocks;
  struct RB_ENTRY(tree)  * availablePages;
};

static int treecmp(const void * ap, const void * bp, const void * param) { 
  const availablePage * a = (const availablePage *) ap;
  const availablePage * b = (const availablePage *) bp;
  
  if(a->freespace < b->freespace) { 
    return -1;
  } else if (a->freespace > b->freespace) { 
    return 1;
  } else { 
    if(a->pageid < b->pageid) { 
      return -1;
    } else if (a->pageid > b->pageid) { 
      return 1;
    } else {
      return 0;
    }
  }
}


allocationPolicy * allocationPolicyInit() { 
  allocationPolicy * ap = malloc(sizeof(allocationPolicy));

  ap->xidLocks = LH_ENTRY(create)(MAX_TRANSACTIONS);
  ap->availablePages = RB_ENTRY(init)(treecmp, 0);

  return ap;
}

void allocationPolicyAddPages(allocationPolicy * ap, availablePage** newPages) {

  for(int i = 0; newPages[i] != 0; i++) {
    const availablePage * ret = RB_ENTRY(search)(newPages[i], ap->availablePages);
    assert(ret == newPages[i]);
  }
  
}

availablePage * allocationPolicyFindPage(allocationPolicy * ap, int xid, int freespace) {
  // For the best fit amongst the pages in availablePages, call:
  //
  //    rblookup(RB_LUGREAT, key, availablePages) 
  // 
  // For the page with the most freespace, call:
  // 
  //    rbmax(availablePages);
  //
  availablePage tmp = { .freespace = freespace, .pageid = 0 };

  struct RB_ENTRY(tree) * locks;

  // If we haven't heard of this transaction yet, then create an entry
  // for it.
  
  if(0 == (locks = LH_ENTRY(find)(ap->xidLocks, &xid, sizeof(xid)))) { 
    int * xidp = malloc(sizeof(int));
    *xidp = xid;
    locks = RB_ENTRY(init)(treecmp, 0);
    LH_ENTRY(insert)(ap->xidLocks, xidp, sizeof(xid), locks);
  }

  const availablePage *ret; 

  // Does this transaction already have an appropriate page?

  if(!(ret = RB_ENTRY(lookup)(RB_LUGREAT, &tmp, locks))) {

    // No; get a page from the availablePages. 

    ret = RB_ENTRY(lookup)(RB_LUGREAT, &tmp, ap->availablePages);
    if(ret) { 
      RB_ENTRY(delete)(ret, ap->availablePages);
      const availablePage * check = RB_ENTRY(search)(ret, locks);
      assert(check == ret);
    }
  }

  // Done.  (If ret is null, then it's the caller's problem.)
  return (availablePage*) ret; 
}

void allocationPolicyTransactionCompleted(allocationPolicy * ap, int xid) {

  struct RB_ENTRY(tree) * locks = LH_ENTRY(remove)(ap->xidLocks, &xid, sizeof(int));
  
  if(locks) {
    RBLIST * iterator = RB_ENTRY(openlist)(locks);

    const availablePage * next;
    while( ( next = RB_ENTRY(readlist)(iterator) ) ) { 
      const availablePage * check = RB_ENTRY(search)(next, ap->availablePages);
      assert(check == next);
    }
    RB_ENTRY(closelist)(iterator);

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
  struct RB_ENTRY(tree) * locks = LH_ENTRY(find)(ap->xidLocks, &xid, sizeof(int));
  availablePage * p = (availablePage*) RB_ENTRY(delete)(key, locks);
  assert(p);
  p->freespace = newFree;
  key->freespace = newFree;
  const availablePage * ret = RB_ENTRY(search)(p, locks);
  assert(ret == p);
}
