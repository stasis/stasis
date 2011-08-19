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

#include <stasis/common.h>
#include <stasis/allocationPolicy.h>
#include <stasis/util/lhtable.h>
#include <stasis/util/redblack.h>
#include <stasis/transactional.h>



#include <assert.h>
#include <stdio.h>

/**
 * Allocation Policy maintains the following tables:
 *
 *   AllPages: _pageid_, freespace, key1: pageid
 *
 *   XidAlloced:  _xid, pageid_ key1: xid,pageid, key2: pageid,xid
 *   XidDealloced: _xid, pageid_: key1: xid,pageid, key2: pageid,xid
 *
 * And the following views:
 *
 *   AvailablePages: (_pageid_, freespace)
 *
 *   The rows of AllPages with no entries in xidAllloced or xidDealloced.
 *     key1: pageid, key2: freespace, pageid
 *
 *   PageOwners: (xid, freespace, _pageid_)
 *
 *   The rows of AllPages with an entry in xidAlloced, and at most one
 *   entry in xidDealloced, key1: pageid, key2 = xid, freespace, pageid
 */
// Tables:
typedef struct {
  pageid_t pageid;
  size_t freespace;
} allPages_pageid_freespace;
typedef struct {
  int xid;
  pageid_t pageid;
} xidAllocedDealloced_xid_pageid;

// Views:

typedef struct {
  pageid_t pageid;
  size_t freespace;
} availablePages_pageid_freespace;
typedef struct {
  int xid;
  size_t freespace;
  pageid_t pageid;
} pageOwners_xid_freespace_pageid;

struct stasis_allocation_policy_t {
  // views
  struct rbtree * availablePages_key_pageid;
  struct rbtree * availablePages_key_freespace_pageid;
  struct rbtree * pageOwners_key_pageid;
  struct rbtree * pageOwners_key_xid_freespace_pageid;
  // tables
  struct rbtree * allPages_key_pageid;
  struct rbtree * xidAlloced_key_xid_pageid;
  struct rbtree * xidAlloced_key_pageid_xid;
  struct rbtree * xidDealloced_key_xid_pageid;
  struct rbtree * xidDealloced_key_pageid_xid;
  // flags
  char reuseWithinXact;
};

// View maintenance functions.  Called by the table insertion functions.

// ######## Helpers ###############

static int void_single_remove(void * val, struct rbtree * a) {
  const void * old = rbdelete(val, a);
  int found = (old != 0);
  if(found) { free((void*)old); }
  return found;
}

static int void_single_add(void * val, struct rbtree * a) {
  int found = void_single_remove(val, a);
  rbsearch(val, a);
  return found;
}

static int void_double_remove(const void * val, struct rbtree * primary, struct rbtree * secondary) {
  const void * fullTuple= rbdelete(val, primary);
  if(fullTuple != 0) {
    const void * old = rbdelete(fullTuple, secondary);
    assert(old == fullTuple);
    free((void*)fullTuple);
    return 1;
  } else {
    return 0;
  }
}
static void void_double_add(void * val, struct rbtree * a, struct rbtree * b) {
  const void * ao = rbsearch(val, a);
  assert(ao == val);
  const void * bo = rbsearch(val, b);
  assert(bo == val);
}
// ######## AvailablePages ###########
static int availablePages_remove(stasis_allocation_policy_t *ap, pageid_t pageid);
static int availablePages_add(stasis_allocation_policy_t *ap, pageid_t pageid, size_t freespace) {
  int ret = availablePages_remove(ap, pageid);
  availablePages_pageid_freespace* tup= malloc(sizeof(*tup));
  tup->pageid = pageid;
  tup->freespace = freespace;
  void_double_add(tup, ap->availablePages_key_pageid, ap->availablePages_key_freespace_pageid);
  return ret;
}
static int availablePages_remove(stasis_allocation_policy_t *ap, pageid_t pageid) {
  availablePages_pageid_freespace tup = {pageid, 0};
  return void_double_remove(&tup, ap->availablePages_key_pageid, ap->availablePages_key_freespace_pageid);
}

// ######## PageOwners ###########
static int pageOwners_remove(stasis_allocation_policy_t *ap, pageid_t pageid);
static int pageOwners_add(stasis_allocation_policy_t *ap, int xid, size_t freespace, pageid_t pageid) {

  int ret = pageOwners_remove(ap, pageid);

  pageOwners_xid_freespace_pageid * tup = malloc(sizeof(*tup));
  tup->xid = xid;
  tup->freespace = freespace;
  tup->pageid = pageid;

  void_double_add(tup, ap->pageOwners_key_pageid, ap->pageOwners_key_xid_freespace_pageid);
  return ret;
}

static int pageOwners_remove(stasis_allocation_policy_t *ap, pageid_t pageid) {
  pageOwners_xid_freespace_pageid tup = { INVALID_XID, 0, pageid };
  return void_double_remove(&tup, ap->pageOwners_key_pageid, ap->pageOwners_key_xid_freespace_pageid);
}
int pageOwners_lookup_by_xid_freespace(stasis_allocation_policy_t *ap, int xid, size_t freespace, pageid_t* pageid) {
  pageOwners_xid_freespace_pageid query = { xid, freespace, 0 };
  // find lowest numbered page w/ enough freespace.
  const pageOwners_xid_freespace_pageid *tup = rblookup(RB_LUGTEQ, &query, ap->pageOwners_key_xid_freespace_pageid);
  if(tup && tup->xid == xid) {
    assert(tup->freespace >= freespace);
    *pageid = tup->pageid;
    return 1;
  } else {
    return 0;
  }
}
int pageOwners_lookup_by_pageid(stasis_allocation_policy_t* ap, pageid_t pageid, int *xid, size_t *freespace) {
  const pageOwners_xid_freespace_pageid query = { 0, 0, pageid };
  const pageOwners_xid_freespace_pageid *tup = rbfind(&query, ap->pageOwners_key_pageid);
  if(tup) {
    *xid = tup->xid;
    *freespace = tup->freespace;
    return 1;
  } else {
    return 0;
  }
}
/// TABLE METHODS FOLLOW.  These functions perform view maintenance.

// ######## AllPages #############
static int allPages_lookup_by_pageid(stasis_allocation_policy_t *ap, pageid_t pageid, size_t *freespace) {
  allPages_pageid_freespace query = {pageid, 0};
  const allPages_pageid_freespace * tup = rbfind(&query, ap->allPages_key_pageid);
  if(tup) {
    assert(tup->pageid == pageid);
    *freespace = tup->freespace;
    return 1;
  } else {
    return 0;
  }
}
static int allPages_add(stasis_allocation_policy_t *ap, pageid_t pageid, size_t freespace) {
  allPages_pageid_freespace * tup = malloc(sizeof(*tup));
  tup->pageid = pageid;
  tup->freespace = freespace;
  int ret = void_single_add(tup, ap->allPages_key_pageid);
  if(!ret) {
    int ret2 = availablePages_add(ap, pageid, freespace);
    assert(!ret2);
  } else {
    // page may or may not be in availablePages...
  }
  return ret;
}
/** Assumes that the page is not in use by an outstanding xact */
static int allPages_remove(stasis_allocation_policy_t *ap, pageid_t pageid) {
  allPages_pageid_freespace tup = { pageid, 0 };
  int found = void_single_remove(&tup, ap->allPages_key_pageid);
  int found2 = availablePages_remove(ap, pageid);
  assert(found == found2);
  return found;
}
static void allPages_removeAll(stasis_allocation_policy_t *ap) {
  const allPages_pageid_freespace * tup;
  while((tup = rbmin(ap->allPages_key_pageid))) {
    allPages_remove(ap, tup->pageid);
  }
}

static void allPages_set_freespace(stasis_allocation_policy_t *ap, pageid_t pageid, size_t freespace) {
  allPages_pageid_freespace * tup = malloc(sizeof(*tup));
  tup->pageid = pageid;
  tup->freespace = freespace;
  int existed = void_single_add(tup, ap->allPages_key_pageid);
  assert(existed);
  int availableExisted = availablePages_remove(ap, pageid);
  if(availableExisted) {
    availablePages_add(ap, pageid, freespace);
  }
  int xid;
  size_t oldfreespace;
  int ownerExisted = pageOwners_lookup_by_pageid(ap, pageid, &xid, &oldfreespace);
  if(ownerExisted) {
    pageOwners_add(ap, xid, freespace, pageid);
  }
  assert(!(ownerExisted && availableExisted));
}
static int xidAllocedDealloced_helper_lookup_by_xid(struct rbtree *t, int xid, pageid_t **pages, size_t*count) {
  xidAllocedDealloced_xid_pageid query = {xid, 0};
  const xidAllocedDealloced_xid_pageid *tup = rblookup(RB_LUGTEQ, &query, t);
  int ret = 0;
  *pages = 0;
  *count = 0;
  while(tup && tup->xid == xid) {
    ret = 1;
    // add pageid to ret value
    (*count)++;
    *pages = realloc(*pages, *count * sizeof(*pages[0]));
//    printf("pages %x count %x *pages %x len %lld \n", pages, count, *pages, *count * sizeof(*pages[0]));
    fflush(stdout);
    (*pages)[(*count) - 1] = tup->pageid;
    tup = rblookup(RB_LUGREAT, tup, t);
  }
  return ret;
}
static int xidAllocedDealloced_helper_lookup_by_pageid(struct rbtree *t, pageid_t pageid, int ** xids, size_t * count) {
  xidAllocedDealloced_xid_pageid query = {0, pageid};
  const xidAllocedDealloced_xid_pageid *tup = rblookup(RB_LUGTEQ, &query, t);
  int ret = 0;
  *xids = 0;
  *count = 0;
  while (tup && tup->pageid == pageid) {
    ret = 1;
    // add xid to ret value.
    (*count)++;
    *xids = realloc(*xids, *count * sizeof(*xids[0]));
    (*xids)[(*count) - 1] = tup->xid;
    tup = rblookup(RB_LUGREAT, tup, t);
  }
  return ret;
}

static int xidAlloced_lookup_by_pageid(stasis_allocation_policy_t *ap, pageid_t pageid, int **xids, size_t * count);
static int xidDealloced_lookup_by_pageid(stasis_allocation_policy_t *ap, pageid_t pageid, int **xids, size_t * count);

static int update_views_for_page(stasis_allocation_policy_t *ap, pageid_t pageid) {
  size_t xidAllocCount;
  size_t xidDeallocCount;
  int ret = 0;
  int * allocXids;
  int * deallocXids;
  size_t freespace = 0;
  int inAllPages = allPages_lookup_by_pageid(ap, pageid, &freespace);
  if(!inAllPages) {
    stasis_allocation_policy_register_new_page(ap, pageid, 0);
  }
  int inXidAlloced = xidAlloced_lookup_by_pageid(ap, pageid, &allocXids, &xidAllocCount);
  int inXidDealloced = xidDealloced_lookup_by_pageid(ap, pageid, &deallocXids, &xidDeallocCount);
  if(! inXidAlloced) { xidAllocCount = 0; allocXids = 0;}
  if(! inXidDealloced) { xidDeallocCount = 0; deallocXids = 0; }
  if(xidAllocCount == 0 && xidDeallocCount == 0) {
    pageOwners_remove(ap, pageid);
    availablePages_add(ap, pageid, freespace);
  } else if ((xidAllocCount == 1 && xidDeallocCount == 0)
               || (xidAllocCount == 1 && xidDeallocCount == 1 && ap->reuseWithinXact && (allocXids[0] == deallocXids[0]))) {
    pageOwners_add(ap, allocXids[0], freespace, pageid);
    availablePages_remove(ap, pageid);
  } else if ((xidAllocCount ==0 && xidDeallocCount == 1)) {
    pageOwners_add(ap, deallocXids[0], freespace, pageid);
    availablePages_remove(ap, pageid);
  } else {
    pageOwners_remove(ap, pageid);
    availablePages_remove(ap, pageid);
  }
  if(allocXids) { free(allocXids); }
  if(deallocXids) { free(deallocXids); }
  return ret;
}
static int xidAllocedDealloced_helper_remove(stasis_allocation_policy_t *ap, struct rbtree *first, struct rbtree*second, int xid, pageid_t pageid);

static int xidAllocedDealloced_helper_add(stasis_allocation_policy_t *ap, struct rbtree *first, struct rbtree* second, int xid, pageid_t pageid) {
  int existed = xidAllocedDealloced_helper_remove(ap, first, second, xid, pageid);

  xidAllocedDealloced_xid_pageid * tup = malloc(sizeof(*tup));
  tup->xid = xid;
  tup->pageid = pageid;
  void_double_add(tup, first, second);
//  if(!existed) {
  update_views_for_page(ap, pageid);
//  }
  return existed;
}
static int xidAllocedDealloced_helper_remove(stasis_allocation_policy_t *ap, struct rbtree *first, struct rbtree*second, int xid, pageid_t pageid) {
  xidAllocedDealloced_xid_pageid query = { xid, pageid };
  int existed = void_double_remove(&query, first, second);
  if(existed) {
    update_views_for_page(ap, pageid);
  }
  return existed;
}
static int xidAlloced_lookup_by_xid(stasis_allocation_policy_t *ap, int xid, pageid_t ** pages, size_t * count) {
  return xidAllocedDealloced_helper_lookup_by_xid(ap->xidAlloced_key_xid_pageid, xid, pages, count);
}
static int xidAlloced_lookup_by_pageid(stasis_allocation_policy_t *ap, pageid_t pageid, int **xids, size_t * count) {
  return xidAllocedDealloced_helper_lookup_by_pageid(ap->xidAlloced_key_pageid_xid, pageid, xids, count);
}
static int xidAlloced_add(stasis_allocation_policy_t * ap, int xid, pageid_t pageid) {
  return xidAllocedDealloced_helper_add(ap, ap->xidAlloced_key_pageid_xid, ap->xidAlloced_key_xid_pageid, xid, pageid);
}
static int xidAlloced_remove(stasis_allocation_policy_t * ap, int xid, pageid_t pageid) {
  return xidAllocedDealloced_helper_remove(ap, ap->xidAlloced_key_pageid_xid, ap->xidAlloced_key_xid_pageid, xid, pageid);
}
static int xidDealloced_lookup_by_xid(stasis_allocation_policy_t *ap, int xid, pageid_t ** pages, size_t * count) {
  return xidAllocedDealloced_helper_lookup_by_xid(ap->xidDealloced_key_xid_pageid, xid, pages, count);
}
static int xidDealloced_lookup_by_pageid(stasis_allocation_policy_t *ap, pageid_t pageid, int **xids, size_t * count) {
  return xidAllocedDealloced_helper_lookup_by_pageid(ap->xidDealloced_key_pageid_xid, pageid, xids, count);
}
static int xidDealloced_add(stasis_allocation_policy_t * ap, int xid, pageid_t pageid) {
  return xidAllocedDealloced_helper_add(ap, ap->xidDealloced_key_pageid_xid, ap->xidDealloced_key_xid_pageid, xid, pageid);
}
static int xidDealloced_remove(stasis_allocation_policy_t * ap, int xid, pageid_t pageid) {
  return xidAllocedDealloced_helper_remove(ap, ap->xidDealloced_key_pageid_xid, ap->xidDealloced_key_xid_pageid, xid, pageid);
}

#ifdef TSEARCH
static int availablePages_cmp_pageid(const void *ap, const void *bp) {
#else
static int availablePages_cmp_pageid(const void *ap, const void *bp, const void* ign) {
#endif
  const availablePages_pageid_freespace *a = ap, *b = bp;
  return (a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 :
        (0));
}
#ifdef TSEARCH
static int availablePages_cmp_freespace_pageid(const void *ap, const void *bp) {
#else
static int availablePages_cmp_freespace_pageid(const void *ap, const void *bp, const void* ign) {
#endif
  const availablePages_pageid_freespace *a = ap, *b = bp;
  int ret = (a->freespace < b->freespace) ? -1 :
        ((a->freespace > b->freespace) ? 1 :
        ((a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 : 0)));
//  printf("freespace = %d, %d pageid = %d, %d ret = %d\n", a->freespace, b->freespace, (int)a->pageid, (int)b->pageid, ret);
  return ret;
}
int availablePages_lookup_by_freespace(stasis_allocation_policy_t *ap, size_t freespace, pageid_t *pageid) {
  const availablePages_pageid_freespace query = { 0, freespace };
  const availablePages_pageid_freespace *tup = rblookup(RB_LUGTEQ, &query, ap->availablePages_key_freespace_pageid);
  if(tup && tup->freespace >= freespace ) {
    *pageid = tup->pageid;
    return 1;
  } else {
    return 0;
  }
}

#ifdef TSEARCH
static int pageOwners_cmp_pageid(const void *ap, const void *bp) {
#else
static int pageOwners_cmp_pageid(const void *ap, const void *bp, const void* ign) {
#endif
  const pageOwners_xid_freespace_pageid *a = ap, *b = bp;
  return (a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 : 0);
}
#ifdef TSEARCH
static int pageOwners_cmp_xid_freespace_pageid(const void *ap, const void *bp) {
#else
static int pageOwners_cmp_xid_freespace_pageid(const void *ap, const void *bp, const void* ign) {
#endif
  const pageOwners_xid_freespace_pageid *a = ap, *b = bp;
  return (a->xid < b->xid) ? -1 :
        ((a->xid > b->xid) ? 1 :
        ((a->freespace < b->freespace) ? -1 :
        ((a->freespace > b->freespace) ? 1 :
        ((a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 : 0)))));
}
#ifdef TSEARCH
static int allPages_cmp_pageid(const void *ap, const void *bp) {
#else
static int allPages_cmp_pageid(const void *ap, const void *bp, const void* ign) {
#endif
  const allPages_pageid_freespace *a = ap, *b = bp;
  return (a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 : 0);
}
#ifdef TSEARCH
static int xidAllocedDealloced_cmp_pageid_xid(const void *ap, const void *bp) {
#else
static int xidAllocedDealloced_cmp_pageid_xid(const void *ap, const void *bp, const void* ign) {
#endif
  const xidAllocedDealloced_xid_pageid *a = ap, *b = bp;
  return (a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 :
        ((a->xid < b->xid) ? -1 :
        ((a->xid > b->xid) ? 1 : 0)));
}
#ifdef TSEARCH
static int xidAllocedDealloced_cmp_xid_pageid(const void *ap, const void *bp) {
#else
static int xidAllocedDealloced_cmp_xid_pageid(const void *ap, const void *bp, const void* ign) {
#endif
  const xidAllocedDealloced_xid_pageid *a = ap, *b = bp;
  return (a->xid < b->xid) ? -1 :
        ((a->xid > b->xid) ? 1 :
        ((a->pageid < b->pageid) ? -1 :
        ((a->pageid > b->pageid) ? 1 : 0)));
}

stasis_allocation_policy_t * stasis_allocation_policy_init() {
  stasis_allocation_policy_t * ap = malloc(sizeof(*ap));
  ap->availablePages_key_pageid = rbinit(availablePages_cmp_pageid, 0);
  ap->availablePages_key_freespace_pageid = rbinit(availablePages_cmp_freespace_pageid, 0);
  ap->pageOwners_key_pageid = rbinit(pageOwners_cmp_pageid, 0);
  ap->pageOwners_key_xid_freespace_pageid = rbinit(pageOwners_cmp_xid_freespace_pageid, 0);
  ap->allPages_key_pageid = rbinit(allPages_cmp_pageid, 0);
  ap->xidAlloced_key_pageid_xid = rbinit(xidAllocedDealloced_cmp_pageid_xid, 0);
  ap->xidAlloced_key_xid_pageid = rbinit(xidAllocedDealloced_cmp_xid_pageid, 0);
  ap->xidDealloced_key_pageid_xid = rbinit(xidAllocedDealloced_cmp_pageid_xid, 0);
  ap->xidDealloced_key_xid_pageid = rbinit(xidAllocedDealloced_cmp_xid_pageid, 0);
  ap->reuseWithinXact = 0;
  return ap;
}
void stasis_allocation_policy_deinit(stasis_allocation_policy_t * ap) {
  allPages_removeAll(ap);  // frees entries in availablePages, asserts that all pages are available.
  rbdestroy(ap->availablePages_key_pageid);
  rbdestroy(ap->availablePages_key_freespace_pageid);
  rbdestroy(ap->pageOwners_key_pageid);
  rbdestroy(ap->pageOwners_key_xid_freespace_pageid);
  rbdestroy(ap->allPages_key_pageid);
  rbdestroy(ap->xidAlloced_key_pageid_xid);
  rbdestroy(ap->xidAlloced_key_xid_pageid);
  rbdestroy(ap->xidDealloced_key_pageid_xid);
  rbdestroy(ap->xidDealloced_key_xid_pageid);
  free(ap);
}
void stasis_allocation_policy_register_new_page(stasis_allocation_policy_t * ap, pageid_t pageid, size_t freespace) {
  int existed = allPages_add(ap,pageid,freespace);
  if(existed) {
    // it may already exist if it was registered during recovery.
    stasis_allocation_policy_update_freespace(ap, pageid, freespace);
  }
}
pageid_t stasis_allocation_policy_pick_suitable_page(stasis_allocation_policy_t * ap, int xid, size_t freespace) {
  // does the xid have a suitable page?
  pageid_t pageid;
  int found = pageOwners_lookup_by_xid_freespace(ap, xid, freespace, &pageid);
  if(found) {
    assert(stasis_allocation_policy_can_xid_alloc_from_page(ap, xid, pageid));
    return pageid;
  }
  // pick one from global pool.
  found = availablePages_lookup_by_freespace(ap, freespace, &pageid);
  if(found) {
    assert(stasis_allocation_policy_can_xid_alloc_from_page(ap, xid, pageid));
    return pageid;
  } else {
    return INVALID_PAGE;
  }
}
void stasis_allocation_policy_transaction_completed(stasis_allocation_policy_t * ap, int xid) {
  pageid_t *allocPages;
  pageid_t *deallocPages;
  size_t allocCount;
  size_t deallocCount;
  int alloced = xidAlloced_lookup_by_xid(ap, xid, &allocPages, &allocCount);
  int dealloced = xidDealloced_lookup_by_xid(ap, xid, &deallocPages, &deallocCount);

  if(alloced) {
    for(size_t i = 0; i < allocCount; i++) {
      xidAlloced_remove(ap, xid, allocPages[i]);
    }
    free(allocPages);
  }
  if(dealloced) {
    for(size_t i = 0; i < deallocCount; i++) {
      xidDealloced_remove(ap, xid, deallocPages[i]);
    }
    free(deallocPages);
  }
}
void stasis_allocation_policy_update_freespace(stasis_allocation_policy_t *ap, pageid_t pageid, size_t freespace) {
  allPages_set_freespace(ap, pageid, freespace);
}
void stasis_allocation_policy_dealloced_from_page(stasis_allocation_policy_t *ap, int xid, pageid_t pageid) {
  xidDealloced_add(ap, xid, pageid);
}
void stasis_allocation_policy_alloced_from_page(stasis_allocation_policy_t * ap, int xid, pageid_t pageid) {
  if(!stasis_allocation_policy_can_xid_alloc_from_page(ap, xid, pageid)) {
    fprintf(stderr, "A transaction allocated space from a page that contains records deallocated by another active transaction.  "
        "This leads to unrecoverable schedules.  Refusing to continue.");
    fflush(stderr);
    abort();
  }
  xidAlloced_add(ap, xid, pageid);
}
int stasis_allocation_policy_can_xid_alloc_from_page(stasis_allocation_policy_t * ap, int xid, pageid_t pageid) {
  int *xids;
  size_t count;
  int ret = 1;
  int existed = xidDealloced_lookup_by_pageid(ap, pageid, &xids, &count);
  if(!existed) { count = 0;  xids = 0;}
  for(size_t i = 0; i < count ; i++) {
    if(xids[i] != xid) { ret = 0; }
  }
  if(xids) { free(xids); }
//  if(!ret) {
//    assert(! availablePages_remove(ap, pageid));
//  }
  return ret;
}
