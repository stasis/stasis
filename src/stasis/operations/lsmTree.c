#include <string.h>

#include <stasis/operations/lsmTree.h>
#include <stasis/constants.h>
//  XXX including fixed.h breaks page api encapsulation; we need a "last slot"
// call.
#include "../page/fixed.h"
#include <pthread.h>

static lsm_comparator_t comparators[MAX_LSM_COMPARATORS];

void lsmTreeRegisterComparator(int id, lsm_comparator_t i) {
  // XXX need to de-init this somewhere... assert(!comparators[id]);
  comparators[id] = i;
}


#define HEADER_SIZE (2 * sizeof(lsmTreeNodeRecord))

// These two constants only apply to the root page.
#define DEPTH 0
#define COMPARATOR 1

// These two apply to all other pages.
#define PREV_LEAF  0
#define NEXT_LEAF  1

// This one applies to all pages.
#define FIRST_SLOT 2

/**

   The implementation strategy used here is a bit of an experiment.

   LSM tree is updated using a FORCE/STEAL strategy.  In order to do
   this efficiently, its root node overrides fixedPage, adding
   pageLoaded and pageFlushed callbacks.  Those callbacks maintain an
   impl pointer, which tracks dirty pages, a mutex, and other
   information on behalf of the tree.  (Note that the dirtyPage list
   must be stored somewhere in memory if the root is evicted with
   outstanding dirty tree pages...)

   Note that this has a particularly nice, general purpose property
   that may be useful for other data structure implementations; by
   using a mutex associated with the root of the data structure, we
   can get rid of the static locks used by existing implementations.

   @todo Need easy way for operations to store things in p->impl, even
   if the underlying page implementation wants to store something
   there too (second pointer?)

   Page layout information for lsm trees:

   root page layout
   ----------------

   uses fixedPage (for now)

   slot 0: the integer id of the comparator used by this tree.
   slot 1: depth of tree.

   the remainder of the slots contain lsmTreeNodeRecords

   internal node page layout
   -------------------------
   uses fixedPage (for now)

   slot 0: prev page
   slot 1: next page
   the remainder of the slots contain lsmTreeNodeRecords

   leaf page layout
   ----------------

   Defined by client.

*/

static pageid_t defaultAllocator(int xid, void *ignored) {
  return TpageAlloc(xid);
}

static pageid_t (*pageAllocator)(int xid, void *ignored) = defaultAllocator;
static void *pageAllocatorConfig;
void TlsmSetPageAllocator(pageid_t (*allocer)(int xid, void * ignored),
                          void * config) {
  pageAllocator = allocer;
  pageAllocatorConfig = config;
}



typedef struct lsmTreeState {
  pageid_t lastLeaf;
} lsmTreeState;

/** Initialize a page for use as an internal node of the tree.
 * lsmTree nodes are based on fixed.h.  This function allocates a page
 * that can hold fixed length records, and then sets up a tree node
 * header in the first two lsmTreeNodeRecords on the page.
 */
static void initializeNodePage(int xid, Page *p, size_t keylen) {
  stasis_fixed_initialize_page(p, sizeof(lsmTreeNodeRecord)+keylen, 0);
  recordid reserved1 = stasis_record_alloc_begin(xid, p, sizeof(lsmTreeNodeRecord)+keylen);
  stasis_record_alloc_done(xid, p, reserved1);
  recordid reserved2 = stasis_record_alloc_begin(xid, p, sizeof(lsmTreeNodeRecord)+keylen);
  stasis_record_alloc_done(xid, p, reserved2);
}

/**
 *  A macro that hardcodes the page implementation to use fixed.h's
 *  page implementation.
 */

#define readNodeRecord(xid,p,slot,keylen) readNodeRecordFixed(xid,p,slot,keylen)
/**
 *  @see readNodeRecord
 */
#define writeNodeRecord(xid,p,slot,key,keylen,ptr) \
               writeNodeRecordFixed(xid,p,slot,key,keylen,ptr)
/**
 *  @see readNodeRecord
 */
#define getKeySize(xid,p) getKeySizeFixed(xid,p)

/*
#define getKeySize(xid,p) getKeySizeVirtualMethods(xid,p)
#define readNodeRecord(xid,p,slot,keylen) \
            readNodeRecordVirtualMethods(xid,p,slot,keylen)
#define writeNodeRecord(xid,p,slot,key,keylen,ptr) \
            writeNodeRecordVirtualMethods(xid,p,slot,key,keylen,ptr)
*/

static inline size_t getKeySizeFixed(int xid, Page const *p) {
  return (*recordsize_cptr(p)) - sizeof(lsmTreeNodeRecord);
}

static inline size_t getKeySizeVirtualMethods(int xid, Page *p) {
  recordid rid = { p->id, 0, 0 };
  return stasis_record_length_read(xid, p, rid) - sizeof(lsmTreeNodeRecord);
}
/**
 * Read a record from the page node, assuming the nodes are fixed pages.
 */
static inline
const lsmTreeNodeRecord* readNodeRecordFixed(int xid, Page *const p, int slot,
                                              int keylen) {
  return (const lsmTreeNodeRecord*)fixed_record_ptr(p, slot);
}
/**
 * Read a record from the page node, using stasis' general-purpose
 * page access API.
 */
static inline
lsmTreeNodeRecord* readNodeRecordVirtualMethods(int xid, Page * p,
                                                int slot, int keylen) {
  lsmTreeNodeRecord *ret;

  recordid rid = {p->id, slot, sizeof(lsmTreeNodeRecord)};
  const lsmTreeNodeRecord *nr
      = (const lsmTreeNodeRecord*)stasis_record_read_begin(xid,p,rid);
  memcpy(ret, nr, sizeof(lsmTreeNodeRecord) + keylen);
  stasis_record_read_done(xid,p,rid,(const byte*)nr);

  DEBUG("reading {%lld, %d, %d} = %d, %lld\n",
        p->id, slot, sizeof(lsmTreeNodeRecord), ret.key, ret.ptr);

  return ret;
}

/**
   @see readNodeFixed
 */
static inline
void writeNodeRecordFixed(int xid, Page *p, int slot,
                          const byte *key, size_t keylen, pageid_t ptr) {
  lsmTreeNodeRecord *nr = (lsmTreeNodeRecord*)fixed_record_ptr(p,slot);
  nr->ptr = ptr;
  memcpy(nr+1, key, keylen);
  stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?
}

/**
   @see readNodeVirtualMethods
*/
static inline
void writeNodeRecordVirtualMethods(int xid, Page *p, int slot,
                                   const byte *key, size_t keylen,
                                   pageid_t ptr) {
  recordid rid = {p->id, slot, sizeof(lsmTreeNodeRecord)};
  lsmTreeNodeRecord *target = (lsmTreeNodeRecord*)stasis_record_write_begin(xid,p,rid);
  target->ptr = ptr;
  memcpy(target+1,key,keylen);

  DEBUG("Writing to record {%d %d %lld}\n", rid.page, rid.slot, rid.size);
  stasis_record_write_done(xid,p,rid,(byte*)target);
  stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?
}

recordid TlsmCreate(int xid, int comparator, int keySize) {

  // can the pages hold at least two keys?
  assert(HEADER_SIZE + 2 * (sizeof(lsmTreeNodeRecord) +keySize) <
         USABLE_SIZE_OF_PAGE - 2 * sizeof(short));

  pageid_t root = pageAllocator(xid, pageAllocatorConfig);
  DEBUG("Root = %lld\n", root);
  recordid ret = { root, 0, 0 };

  Page *p = loadPage(xid, ret.page);
  writelock(p->rwlatch,0);
  stasis_fixed_initialize_page(p, sizeof(lsmTreeNodeRecord) + keySize, 0);
  *stasis_page_type_ptr(p) = LSM_ROOT_PAGE;

  lsmTreeState *state = malloc(sizeof(lsmTreeState));
  state->lastLeaf = -1; /// XXX define something in constants.h?

  p->impl = state;

  recordid tmp
      = stasis_record_alloc_begin(xid, p, sizeof(lsmTreeNodeRecord) + keySize);
  stasis_record_alloc_done(xid,p,tmp);

  assert(tmp.page == ret.page
         && tmp.slot == DEPTH
         && tmp.size == sizeof(lsmTreeNodeRecord) + keySize);

  tmp = stasis_record_alloc_begin(xid, p, sizeof(lsmTreeNodeRecord) + keySize);
  stasis_record_alloc_done(xid,p,tmp);

  assert(tmp.page == ret.page
         && tmp.slot == COMPARATOR
         && tmp.size == sizeof(lsmTreeNodeRecord) + keySize);

  byte *dummy = calloc(1,keySize);

  writeNodeRecord(xid, p, DEPTH, dummy, keySize, 0);
  writeNodeRecord(xid, p, COMPARATOR, dummy, keySize, comparator);

  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}

static recordid buildPathToLeaf(int xid, recordid root, Page *root_p,
                                int depth, const byte *key, size_t key_len,
                                pageid_t val_page, pageid_t lastLeaf) {
  // root is the recordid on the root page that should point to the
  // new subtree.
  assert(depth);
  DEBUG("buildPathToLeaf(depth=%d) (lastleaf=%lld) called\n",depth, lastLeaf);

  pageid_t child = pageAllocator(xid, pageAllocatorConfig); // XXX Use some other function...
  DEBUG("new child = %lld internal? %d\n", child, depth-1);

  Page *child_p = loadPage(xid, child);
  writelock(child_p->rwlatch,0);
  initializeNodePage(xid, child_p, key_len);

  recordid ret;

  if(depth-1) {
    // recurse: the page we just allocated is not a leaf.
    recordid child_rec = stasis_record_alloc_begin(xid, child_p, sizeof(lsmTreeNodeRecord)+key_len);
    assert(child_rec.size != INVALID_SLOT);
    stasis_record_alloc_done(xid, child_p, child_rec);

    ret = buildPathToLeaf(xid, child_rec, child_p, depth-1, key, key_len,
                      val_page,lastLeaf);

    unlock(child_p->rwlatch);
    releasePage(child_p);

  } else {
    // set leaf

    byte *dummy = calloc(1, key_len);

    // backward link.
    writeNodeRecord(xid,child_p,PREV_LEAF,dummy,key_len,lastLeaf);
    // forward link (initialize to -1)
    writeNodeRecord(xid,child_p,NEXT_LEAF,dummy,key_len,-1);

    recordid leaf_rec = stasis_record_alloc_begin(xid, child_p,
                                       sizeof(lsmTreeNodeRecord)+key_len);

    assert(leaf_rec.slot == FIRST_SLOT);

    stasis_record_alloc_done(xid, child_p, leaf_rec);
    writeNodeRecord(xid,child_p,leaf_rec.slot,key,key_len,val_page);

    ret = leaf_rec;

    unlock(child_p->rwlatch);
    releasePage(child_p);
    if(lastLeaf != -1) {
      // install forward link in previous page
      Page *lastLeafP = loadPage(xid, lastLeaf);
      writelock(lastLeafP->rwlatch,0);
      writeNodeRecord(xid,lastLeafP,NEXT_LEAF,dummy,key_len,child);
      unlock(lastLeafP->rwlatch);
      releasePage(lastLeafP);
    }

    DEBUG("%lld <-> %lld\n", lastLeaf, child);
    free(dummy);
  }

  writeNodeRecord(xid, root_p, root.slot, key, key_len, child);

  return ret;
}

/* adding pages:

  1) Try to append value to lsmTreeState->lastLeaf

  2) If that fails, traverses down the root of the tree, split pages while
     traversing back up.

  3) Split is done by adding new page at end of row (no key
     redistribution), except at the root, where root contents are
     pushed into the first page of the next row, and a new path from root to
     leaf is created starting with the root's immediate second child.

*/

static recordid appendInternalNode(int xid, Page *p,
                                   int depth,
                                   const byte *key, size_t key_len,
                                   pageid_t val_page, pageid_t lastLeaf) {
  assert(*stasis_page_type_ptr(p) == LSM_ROOT_PAGE || 
	 *stasis_page_type_ptr(p) == FIXED_PAGE);
  if(!depth) {
    // leaf node.
    recordid ret = stasis_record_alloc_begin(xid, p, sizeof(lsmTreeNodeRecord)+key_len);
    if(ret.size != INVALID_SLOT) {
      stasis_record_alloc_done(xid, p, ret);
      writeNodeRecord(xid,p,ret.slot,key,key_len,val_page);
    }
    return ret;
  } else {
    // recurse
    int slot = *recordcount_ptr(p)-1;
    assert(slot >= FIRST_SLOT); // there should be no empty nodes
    const lsmTreeNodeRecord *nr = readNodeRecord(xid, p, slot, key_len);
    pageid_t child_id = nr->ptr;
    recordid ret;
    {
      Page *child_page = loadPage(xid, child_id);
      writelock(child_page->rwlatch,0);
      ret = appendInternalNode(xid, child_page, depth-1, key, key_len,
                               val_page, lastLeaf);

      unlock(child_page->rwlatch);
      releasePage(child_page);
    }
    if(ret.size == INVALID_SLOT) { // subtree is full; split
      ret = stasis_record_alloc_begin(xid, p, sizeof(lsmTreeNodeRecord)+key_len);
      if(ret.size != INVALID_SLOT) {
        stasis_record_alloc_done(xid, p, ret);
        ret = buildPathToLeaf(xid, ret, p, depth, key, key_len, val_page,
                              lastLeaf);

        DEBUG("split tree rooted at %lld, wrote value to {%d %d %lld}\n",
              p->id, ret.page, ret.slot, ret.size);
      } else {
        // ret is NULLRID; this is the root of a full tree. Return
        // NULLRID to the caller.
      }
    } else {
      // we inserted the value in to a subtree rooted here.
    }
    return ret;
  }
}

/**
 * Traverse from the root of the page to the right most leaf (the one
 * with the higest base key value).
 */
static pageid_t findLastLeaf(int xid, Page *root, int depth) {
  if(!depth) {
    DEBUG("Found last leaf = %lld\n", root->id);
    return root->id;
  } else {
    // passing zero as length is OK, as long as we don't try to access the key.
    const lsmTreeNodeRecord *nr = readNodeRecord(xid, root,
                                                  (*recordcount_ptr(root))-1,0);
    pageid_t ret;

    Page *p = loadPage(xid, nr->ptr);
    readlock(p->rwlatch,0);
    ret = findLastLeaf(xid,p,depth-1);
    unlock(p->rwlatch);
    releasePage(p);

    return ret;
  }
}

/**
 *  Traverse from the root of the tree to the left most (lowest valued
 *  key) leaf.
 */
static pageid_t findFirstLeaf(int xid, Page *root, int depth) {
  if(!depth) {
    return root->id;
  } else {
    const lsmTreeNodeRecord *nr = readNodeRecord(xid,root,FIRST_SLOT,0);
    Page *p = loadPage(xid, nr->ptr);
    readlock(p->rwlatch,0);
    pageid_t ret = findFirstLeaf(xid,p,depth-1);
    unlock(p->rwlatch);
    releasePage(p);
    return ret;
  }
}
recordid TlsmAppendPage(int xid, recordid tree,
                        const byte *key,
                        long val_page) {
  Page *p = loadPage(xid, tree.page);
  writelock(p->rwlatch, 0);
  lsmTreeState *s = p->impl;

  size_t keySize = getKeySize(xid,p);

  tree.slot = 0;
  tree.size = sizeof(lsmTreeNodeRecord)+keySize;


  const lsmTreeNodeRecord *nr = readNodeRecord(xid, p, DEPTH, keySize);
  int depth = nr->ptr;

  if(s->lastLeaf == -1) {
    s->lastLeaf = findLastLeaf(xid, p, depth);
  }

  Page *lastLeaf;

  if(s->lastLeaf != tree.page) {
    lastLeaf= loadPage(xid, s->lastLeaf);
    writelock(lastLeaf->rwlatch, 0);
  } else {
    lastLeaf = p;
  }

  recordid ret = stasis_record_alloc_begin(xid, lastLeaf,
                                sizeof(lsmTreeNodeRecord)+keySize);

  if(ret.size == INVALID_SLOT) {
    if(lastLeaf->id != p->id) {
      assert(s->lastLeaf != tree.page);
      unlock(lastLeaf->rwlatch);
      releasePage(lastLeaf); // don't need that page anymore...
      lastLeaf = 0;
    }
    // traverse down the root of the tree.

    tree.slot = 0;

    assert(tree.page == p->id);
    ret = appendInternalNode(xid, p, depth, key, keySize, val_page,
                             s->lastLeaf == tree.page ? -1 : s->lastLeaf);

    if(ret.size == INVALID_SLOT) {
      DEBUG("Need to split root; depth = %d\n", depth);

      pageid_t child = pageAllocator(xid, pageAllocatorConfig);
      Page *lc = loadPage(xid, child);
      writelock(lc->rwlatch,0);

      initializeNodePage(xid, lc,keySize);

      for(int i = FIRST_SLOT; i < *recordcount_ptr(p); i++) {

        recordid cnext = stasis_record_alloc_begin(xid, lc,
                                        sizeof(lsmTreeNodeRecord)+keySize);

        assert(i == cnext.slot);
        assert(cnext.size != INVALID_SLOT);

        stasis_record_alloc_done(xid, lc, cnext);

        const lsmTreeNodeRecord *nr = readNodeRecord(xid,p,i,keySize);
        writeNodeRecord(xid,lc,i,(byte*)(nr+1),keySize,nr->ptr);

      }

      // deallocate old entries, and update pointer on parent node.
      recordid pFirstSlot = { p->id, FIRST_SLOT,
                              sizeof(lsmTreeNodeRecord)+keySize };

      // @todo should fixed.h support bulk deallocation directly?
      *recordcount_ptr(p) = FIRST_SLOT+1;

      lsmTreeNodeRecord *nr
          = (lsmTreeNodeRecord*)stasis_record_write_begin(xid, p, pFirstSlot);

      // don't overwrite key...
      nr->ptr = child;
      stasis_record_write_done(xid,p,pFirstSlot,(byte*)nr);
      stasis_page_lsn_write(xid, p, 0); // XXX need real LSN?

      byte *dummy = calloc(1,keySize);
      if(!depth) {
        s->lastLeaf = lc->id;
        writeNodeRecord(xid,lc,PREV_LEAF,dummy,keySize,-1);
        writeNodeRecord(xid,lc,NEXT_LEAF,dummy,keySize,-1);
      }

      unlock(lc->rwlatch);
      releasePage(lc);


      depth ++;
      writeNodeRecord(xid,p,DEPTH,dummy,keySize,depth);
      free(dummy);

      assert(tree.page == p->id);
      ret = appendInternalNode(xid, p, depth, key, keySize, val_page,
                               s->lastLeaf == tree.page ? -1 : s->lastLeaf);

      assert(ret.size != INVALID_SLOT);

    } else {
      DEBUG("Appended new internal node tree depth = %d key = %d\n",
            depth, *(int*)key);
    }
    s->lastLeaf = ret.page;
    DEBUG("lastleaf is %lld\n", s->lastLeaf);
  } else {

    // write the new value to an existing page
    DEBUG("Writing %d to existing page# %lld\n", *(int*)key, lastLeaf->id);

    stasis_record_alloc_done(xid, lastLeaf, ret);

    writeNodeRecord(xid, lastLeaf, ret.slot, key, keySize, val_page);

    if(lastLeaf->id != p->id) {
      assert(s->lastLeaf != tree.page);
      unlock(lastLeaf->rwlatch);
      releasePage(lastLeaf);
    }
  }

  unlock(p->rwlatch);
  releasePage(p);

  return ret;
}

static pageid_t lsmLookup(int xid, Page *node, int depth,
                      const byte *key, size_t keySize, lsm_comparator_t cmp) {

  if(*recordcount_ptr(node) == FIRST_SLOT) { return -1; }
  assert(*recordcount_ptr(node) > FIRST_SLOT);

  const lsmTreeNodeRecord *prev = readNodeRecord(xid,node,FIRST_SLOT,keySize);
  int prev_cmp_key = cmp(prev+1,key);

  // @todo binary search within each page
  for(int i = FIRST_SLOT+1; i < *recordcount_ptr(node); i++) {
    const lsmTreeNodeRecord *rec = readNodeRecord(xid,node,i,keySize);

    int rec_cmp_key  = cmp(rec+1,key);

    if(depth) {

      if(prev_cmp_key <= 0 && rec_cmp_key > 0) {
        pageid_t child_id = prev->ptr;
        Page *child_page = loadPage(xid, child_id);
        readlock(child_page->rwlatch,0);
        long ret = lsmLookup(xid,child_page,depth-1,key,keySize,cmp);
        unlock(child_page->rwlatch);
        releasePage(child_page);
        return ret;
      }

    } else {

      if(prev_cmp_key == 0) {
        return prev->ptr;
      }
    }
    prev = rec;
    prev_cmp_key = rec_cmp_key;
    if(rec_cmp_key > 0) { break; }
  }

  if(depth) {

    if(prev_cmp_key <= 0) {
      pageid_t child_id = prev->ptr;
      Page *child_page = loadPage(xid, child_id);
      readlock(child_page->rwlatch,0);
      long ret = lsmLookup(xid,child_page,depth-1,key,keySize,cmp);
      unlock(child_page->rwlatch);
      releasePage(child_page);
      return ret;
    }

  } else {

    if(prev_cmp_key == 0) {
      return prev->ptr;
    }

  }
  return -1;
}

/**
    Look up the value associated with key.

    @return -1 if key isn't in the tree.
*/
pageid_t TlsmFindPage(int xid, recordid tree, const byte *key) {
  Page *p = loadPage(xid, tree.page);
  readlock(p->rwlatch,0);

  tree.slot = 0;
  tree.size = *recordsize_ptr(p);

  size_t keySize = getKeySize(xid,p);

  const lsmTreeNodeRecord *depth_nr = readNodeRecord(xid, p , 0, keySize);
  const lsmTreeNodeRecord *cmp_nr = readNodeRecord(xid, p , 1, keySize);

  int depth = depth_nr->ptr;

  lsm_comparator_t cmp = comparators[cmp_nr->ptr];

  pageid_t ret = lsmLookup(xid, p, depth, key, keySize, cmp);

  unlock(p->rwlatch);
  releasePage(p);

  return ret;

}

pageid_t TlsmLastPage(int xid, recordid tree) {
  Page * root = loadPage(xid, tree.page);
  readlock(root->rwlatch,0);
  lsmTreeState *state = root->impl;
  int keySize = getKeySize(xid,root);
  if(state->lastLeaf == -1) {
    const lsmTreeNodeRecord *nr = readNodeRecord(xid,root,DEPTH,
                                                 keySize);
    int depth = nr->ptr;
    state->lastLeaf = findLastLeaf(xid,root,depth);
  }
  pageid_t ret = state->lastLeaf;
  unlock(root->rwlatch);

  // ret points to the last internal node at this point.
  releasePage(root);

  Page * p = loadPage(xid, ret);
  readlock(p->rwlatch,0);
  if(*recordcount_ptr(p) == 2) {
    ret = -1;
  } else {
    const lsmTreeNodeRecord *nr = readNodeRecord(xid,p,(*recordcount_ptr(p))-1,keySize);
    ret = nr->ptr;
  }
  unlock(p->rwlatch);
  releasePage(p);

  return ret;
}

/**
    The buffer manager calls this when the lsmTree's root page is
    loaded.  This function allocates some storage for cached values
    associated with the tree.
*/
static void lsmPageLoaded(Page *p) {
  /// XXX should call fixedLoaded, or something...
  lsmTreeState *state = malloc(sizeof(lsmTreeState));
  state->lastLeaf = -1;
  p->impl = state;
}
static void lsmPageFlushed(Page *p) { }
/**
    Free any soft state associated with the tree rooted at page p.
    This is called by the buffer manager.
*/
static void lsmPageCleanup(Page *p) {
  lsmTreeState *state = p->impl;
  free(state);
}
/**
   A page_impl for the root of an lsmTree.
*/
page_impl lsmRootImpl() {
  page_impl pi = fixedImpl();
  pi.pageLoaded = lsmPageLoaded;
  pi.pageFlushed = lsmPageFlushed;
  pi.pageCleanup = lsmPageCleanup;
  pi.page_type = LSM_ROOT_PAGE;
  return pi;
}
///---------------------  Iterator implementation

lladdIterator_t *lsmTreeIterator_open(int xid, recordid root) {
  Page *p = loadPage(xid,root.page);
  readlock(p->rwlatch,0);
  size_t keySize = getKeySize(xid,p);
  const lsmTreeNodeRecord *nr = readNodeRecord(xid,p,DEPTH,keySize);
  int depth = nr->ptr;
  pageid_t leafid = findFirstLeaf(xid, p, depth);
  if(leafid != root.page) {
    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid,leafid);
    readlock(p->rwlatch,0);
    assert(depth != 0);
  } else {
    assert(depth == 0);
  }
  lsmIteratorImpl *impl = malloc(sizeof(lsmIteratorImpl));
  impl->p = p;
  {
    recordid rid = { p->id, 1, keySize };
    impl->current = rid;
  }
  DEBUG("keysize = %d, slot = %d\n", keySize, impl->current.slot);
  impl->t = 0;
  impl->justOnePage = (depth == 0);

  lladdIterator_t *it = malloc(sizeof(lladdIterator_t));
  it->type = -1; // XXX  LSM_TREE_ITERATOR;
  it->impl = impl;
  /*  itdef = {   <-- @todo register lsmTree iterators with stasis someday...
    lsmTreeIterator_close;
    lsmTreeIterator_next;
    lsmTreeIterator_next;
    lsmTreeIterator_key;
    lsmTreeIterator_value;
    lsmTreeIterator_tupleDone;
    lsmTreeIterator_releaseLock;
    } */
  return it;
}
lladdIterator_t *lsmTreeIterator_copy(int xid, lladdIterator_t* i) {
  lsmIteratorImpl *it = i->impl;
  lsmIteratorImpl *mine = malloc(sizeof(lsmIteratorImpl));

  if(it->p) {
    mine->p = loadPage(xid, it->p->id);
    readlock(mine->p->rwlatch,0);
  } else {
    mine->p = 0;
  }
  memcpy(&mine->current, &it->current,sizeof(recordid));
  mine->t = it->t;
  mine->justOnePage = it->justOnePage;
  lladdIterator_t * ret = malloc(sizeof(lladdIterator_t));
  ret->type = -1; // XXX LSM_TREE_ITERATOR
  ret->impl = mine;
  return ret;
}
void lsmTreeIterator_close(int xid, lladdIterator_t *it) {
  lsmIteratorImpl *impl = it->impl;
  if(impl->p) {
    unlock(impl->p->rwlatch);
    releasePage(impl->p);
  }
  free(impl);
  free(it);
}

int lsmTreeIterator_next(int xid, lladdIterator_t *it) {
  lsmIteratorImpl *impl = it->impl;
  size_t keySize = impl->current.size;
  impl->current = fixedNext(xid, impl->p, impl->current);
  if(impl->current.size == INVALID_SLOT) {
    const lsmTreeNodeRecord *next_rec = readNodeRecord(xid,impl->p,NEXT_LEAF,
                                                       keySize);
    unlock(impl->p->rwlatch);
    releasePage(impl->p);

    DEBUG("done with page %lld next = %lld\n", impl->p->id, next_rec->ptr);

    if(next_rec->ptr != -1 && ! impl->justOnePage) {
      impl->p = loadPage(xid, next_rec->ptr);
      readlock(impl->p->rwlatch,0);
      impl->current.page = next_rec->ptr;
      impl->current.slot = 2;
      impl->current.size = keySize;
    } else {
      impl->p = 0;
      impl->current.size = INVALID_SLOT;
    }
  } else {
    assert(impl->current.size == keySize + sizeof(lsmTreeNodeRecord));
    impl->current.size = keySize;
  }
  if(impl->current.size != INVALID_SLOT) {
    impl->t = readNodeRecord(xid,impl->p,impl->current.slot,impl->current.size);
    return 1;
  } else {
    impl->t = 0;
    return 0;
  }
}
