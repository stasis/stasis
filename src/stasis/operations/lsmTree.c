#include <stasis/operations/lsmTree.h>
#include <stasis/constants.h>
//  XXX including fixed.h breaks page api encapsulation; we need a "last slot"
// call.
#include "../page/fixed.h"
#include <pthread.h>

const int MAX_LSM_COMPARATORS = 256;

typedef struct nodeRecord {
  pageid_t ptr;
  int      key;
  //  char funk[1000];
} nodeRecord;

#define HEADER_SIZE (2 * sizeof(nodeRecord))

typedef struct lsmTreeState {
  // pthread_mutex_t mut;
  // pageid_t * dirtyPages;
  pageid_t lastLeaf;
} lsmTreeState;

/** Initialize a page for use as an internal node of the tree.
 * lsmTree nodes are based on fixed.h.  This function allocates a page
 * that can hold fixed length records, and then sets up a tree node
 * header in the first two nodeRecords on the page.
 */
static void initializeNodePage(int xid, Page * p) {
  fixedPageInitialize(p, sizeof(nodeRecord), 0);
  recordid reserved1 = recordPreAlloc(xid, p, sizeof(nodeRecord));
  recordPostAlloc(xid, p, reserved1);
  recordid reserved2 = recordPreAlloc(xid, p, sizeof(nodeRecord));
  recordPostAlloc(xid, p, reserved2);
}

/**
 *  A macro that hardcodes the page implementation to use fixed.h's page implementation.
 */

#define readNodeRecord(xid,p,slot) readNodeRecordFixed(xid,p,slot)
/**
 *  @see readNodeRecord
 */
#define writeNodeRecord(xid,p,slot,key,ptr) writeNodeRecordFixed(xid,p,slot,key,ptr)
//#define readNodeRecord(xid,p,slot) readNodeRecordVirtualMethods(xid,p,slot)
//#define writeNodeRecord(xid,p,slot,key,ptr) writeNodeRecordVirtualMethods(xid,p,slot,key,ptr)

/**
 * Read a record from the page node, assuming the nodes are fixed pages.
 */
static inline nodeRecord readNodeRecordFixed(int xid, Page * const p, int slot) {
  return *(nodeRecord*)fixed_record_ptr(p, slot);
}
/**
 * Read a record from the page node, using stasis' general-purpose page access API.
 */
static inline nodeRecord readNodeRecordVirtualMethods(int xid, Page * const p, int slot) {
  nodeRecord ret;

  recordid rid = {p->id, slot, sizeof(nodeRecord)};
  const nodeRecord * nr = (const nodeRecord*)recordReadNew(xid,p,rid);
  ret = *nr;
  assert(ret.ptr > 1 || slot < 2);
  recordReadDone(xid,p,rid,(const byte*)nr);

  DEBUG("reading {%lld, %d, %d} = %d, %lld\n", p->id, slot, sizeof(nodeRecord), ret.key, ret.ptr);

  return ret;
}

/**
   @see readNodeFixed
 */
static inline void writeNodeRecordFixed(int xid, Page * const p, int slot, int key, pageid_t ptr) { 
  nodeRecord * nr = (nodeRecord*)fixed_record_ptr(p,slot);
  nr->key = key;
  nr->ptr = ptr;
  pageWriteLSN(xid, p, 0); // XXX need real LSN?
}

/**
   @see readNodeVirtualMethods
*/
static inline void writeNodeRecordVirtualMethods(int xid, Page * const p, int slot, int key, pageid_t ptr) {
  nodeRecord src;
  src.key = key;
  src.ptr = ptr;
  assert(src.ptr > 1 || slot < 2);

  recordid rid = {p->id, slot, sizeof(nodeRecord)};
  nodeRecord * target = (nodeRecord*)recordWriteNew(xid,p,rid);
  *target = src;
  DEBUG("Writing to record {%d %d %lld}\n", rid.page, rid.slot, rid.size);
  recordWriteDone(xid,p,rid,(byte*)target);
  pageWriteLSN(xid, p, 0); // XXX need real LSN?
}

/**

   The implementation strategy used here is a bit of an experiment.

   LSM tree is updated using a FORCE/STEAL strategy.  In order to do
   this efficiently, its root node overrides fixedPage, adding
   pageLoaded and pageFlushed callbacks.  Those callbacks maintain an
   impl pointer, which tracks dirty pages, a mutex, and other
   information on behalf of the tree.  (Note that the dirtyPage list
   must be stored in a global hash tree if the root is evicted with
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

   slot 0: depth of tree.
   slot 1: slot id of first key in leaf records.  [unimplemented]

   the remainder of the slots contain nodeRecords

   internal node page layout
   -------------------------
   uses fixedPage (for now)

   slot 0: prev page   [unimplemented]
   slot 1: next page   [unimplemented]
   the remainder of the slots contain nodeRecords

   leaf page layout
   ----------------

   Defined by client, but calling readRecord() on the slot id must
   return the first key stored on the page.

*/
recordid TlsmCreate(int xid, int leafFirstSlot, int keySize) {
  // XXX generalize later
  assert(keySize == sizeof(int));

  // XXX hardcoded to fixed.h's current page layout, and node records
  // that contain the key...

  // can the pages hold at least two keys?  
  assert(HEADER_SIZE + 2 * (sizeof(nodeRecord) /*XXX +keySize*/) <
         USABLE_SIZE_OF_PAGE - 2 * sizeof(short));

  pageid_t root = TpageAlloc(xid);

  recordid ret = { root, 0, 0 };

  Page * const p = loadPage(xid, ret.page);
  writelock(p->rwlatch,0);
  fixedPageInitialize(p, sizeof(nodeRecord), 0);
  *page_type_ptr(p) = LSM_ROOT_PAGE;

  lsmTreeState * state = malloc(sizeof(lsmTreeState));
  state->lastLeaf = -1; /// constants.h
  //  pthread_mutex_init(&(state->mut),0);
  //  state->dirtyPages = malloc(sizeof(Page*)*2);
  //  state->dirtyPages[0] = ret.page;
  //  state->dirtyPages[1] = -1; // XXX this should be defined in constants.h

  p->impl = state;

  recordid treeDepth = recordPreAlloc(xid, p, sizeof(nodeRecord));
  recordPostAlloc(xid,p,treeDepth);

  assert(treeDepth.page == ret.page
         && treeDepth.slot == 0
         && treeDepth.size == sizeof(nodeRecord));

  recordid slotOff = recordPreAlloc(xid, p, sizeof(nodeRecord));
  recordPostAlloc(xid,p,slotOff);

  assert(slotOff.page == ret.page
         && slotOff.slot == 1
         && slotOff.size == sizeof(nodeRecord));

  // ptr is zero because tree depth starts out as zero.
  writeNodeRecord(xid, p, 0, 0, 0);
  // ptr = slotOff (which isn't used, for now...)
  writeNodeRecord(xid, p, 1, 0, leafFirstSlot);

  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}

static recordid buildPathToLeaf(int xid, recordid root, Page * const root_p,
                                int depth, const byte * key, size_t key_len,
                                pageid_t val_page) {
  // root is the recordid on the root page that should point to the
  // new subtree.
  assert(depth);
  DEBUG("buildPathToLeaf(depth=%d) called\n",depth);

  pageid_t child = TpageAlloc(xid); // XXX Use some other function...

  Page * const child_p = loadPage(xid, child);
  writelock(child_p->rwlatch,0);
  initializeNodePage(xid, child_p);

  recordid ret;

  if(depth-1) {
    // recurse: the page we just allocated is not a leaf.
    recordid child_rec = recordPreAlloc(xid, child_p, sizeof(nodeRecord));
    assert(child_rec.size != INVALID_SLOT);
    recordPostAlloc(xid, child_p, child_rec);

    ret = buildPathToLeaf(xid, child_rec, child_p, depth-1, key, key_len,
                      val_page);
  } else {
    // set leaf
    recordid leaf_rec = recordPreAlloc(xid, child_p, sizeof(nodeRecord));
    assert(leaf_rec.slot == 2); // XXX
    recordPostAlloc(xid, child_p, leaf_rec);
    writeNodeRecord(xid,child_p,leaf_rec.slot,*(int*)key,val_page);

    ret = leaf_rec;
  }
  unlock(child_p->rwlatch);
  releasePage(child_p);

  writeNodeRecord(xid, root_p, root.slot, *(int*)key, child);

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

static recordid appendInternalNode(int xid, Page * const p,
                                   int depth,
                                   const byte *key, size_t key_len,
                                   pageid_t val_page) {
  if(!depth) {
    // leaf node.
    recordid ret = recordPreAlloc(xid, p, sizeof(nodeRecord));
    if(ret.size != INVALID_SLOT) {
      recordPostAlloc(xid, p, ret);
      writeNodeRecord(xid,p,ret.slot,*(int*)key,val_page);
      assert(val_page); // XXX
    }
    return ret;
  } else {
    // recurse
    int slot = *recordcount_ptr(p)-1;
    assert(slot >= 2); // XXX
    nodeRecord nr = readNodeRecord(xid, p, slot);
    pageid_t child_id = nr.ptr;
    recordid ret;
    {
      Page * const child_page = loadPage(xid, child_id);
      writelock(child_page->rwlatch,0);
      ret = appendInternalNode(xid, child_page, depth-1,
                                        key, key_len, val_page);
      unlock(child_page->rwlatch);
      releasePage(child_page);
    }
    if(ret.size == INVALID_SLOT) { // subtree is full; split
      if(depth > 1) {
        DEBUG("subtree is full at depth %d\n", depth);
      }

      ret = recordPreAlloc(xid, p, sizeof(nodeRecord));
      if(ret.size != INVALID_SLOT) {
        recordPostAlloc(xid, p, ret);
        ret = buildPathToLeaf(xid, ret, p, depth, key, key_len, val_page);

        DEBUG("split tree rooted at %lld, wrote value to {%d %d %lld}\n", p->id, ret.page, ret.slot, ret.size);
      } else {
        // ret is NULLRID; this is the root of a full tree. Return NULLRID to the caller.
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
static pageid_t findLastLeaf(int xid, Page * const root, int depth) {
  if(!depth) {
    DEBUG("Found last leaf = %lld\n", root->id);
    return root->id;
  } else {
    nodeRecord nr = readNodeRecord(xid, root, (*recordcount_ptr(root))-1);
    pageid_t ret;
    {
      Page * const p = loadPage(xid, nr.ptr);
      writelock(p->rwlatch,0);
      ret = findLastLeaf(xid,p,depth-1);
      unlock(p->rwlatch);
      releasePage(p);
    }
    return ret;
  }
}

recordid TlsmAppendPage(int xid, recordid tree,
                        const byte *key, size_t keySize,
                        long val_page) {
  Page * const p = loadPage(xid, tree.page);
  writelock(p->rwlatch, 0);
  lsmTreeState * s = p->impl;
      //  pthread_mutex_lock(&(s->mut));

  tree.slot = 0;
  tree.size = sizeof(nodeRecord);

  nodeRecord nr = readNodeRecord(xid,p,0);
  int depth = nr.ptr;
  //  const nodeRecord * nr = (const nodeRecord*)recordReadNew(xid,p,tree);
  //  int depth = nr->ptr;
  //  recordReadDone(xid,p,tree,(const byte*)nr);

  if(s->lastLeaf == -1) {
    s->lastLeaf = findLastLeaf(xid, p, depth);
  }
  Page * lastLeaf;
  if(s->lastLeaf != tree.page) {
    lastLeaf= loadPage(xid, s->lastLeaf);
    writelock(lastLeaf->rwlatch, 0);  // tree depth is in slot zero of root
  } else {
    lastLeaf = p;
  }

  recordid ret = recordPreAlloc(xid, lastLeaf, sizeof(nodeRecord));

  if(ret.size == INVALID_SLOT) {
    if(lastLeaf->id != p->id) {
      unlock(lastLeaf->rwlatch);
      releasePage(lastLeaf); // don't need that page anymore...
    }
    // traverse down the root of the tree.

    tree.slot = 0;

    assert(tree.page == p->id);
    ret = appendInternalNode(xid, p, depth, key, keySize,
                             val_page);

    if(ret.size == INVALID_SLOT) {
      DEBUG("Need to split root; depth = %d\n", depth);

      pageid_t child = TpageAlloc(xid);

      Page * lc = loadPage(xid, child);

      writelock(lc->rwlatch,0);

      initializeNodePage(xid, lc);

      for(int i = 2; i < *recordcount_ptr(p); i++) {

        recordid cnext = recordPreAlloc(xid, lc, sizeof(nodeRecord));

        assert(i == cnext.slot); // XXX hardcoded to current node format...
        assert(cnext.size != INVALID_SLOT);

        recordPostAlloc(xid, lc, cnext);

        nodeRecord nr = readNodeRecord(xid,p,i);
        writeNodeRecord(xid,lc,i,nr.key,nr.ptr);

      }

      // deallocate old entries, and update pointer on parent node.
      // XXX this is a terrible way to do this.
      recordid pFirstSlot = {p->id, 2, sizeof(nodeRecord)};
      *recordcount_ptr(p) = 3; 
      nodeRecord * nr = (nodeRecord*)recordWriteNew(xid, p, pFirstSlot);
      // don't overwrite key...
      nr->ptr = child;
      assert(nr->ptr > 1);///XXX
      recordWriteDone(xid,p,pFirstSlot,(byte*)nr);
      pageWriteLSN(xid, p, 0); // XXX need real LSN?

      unlock(lc->rwlatch);
      releasePage(lc);

      depth ++;
      writeNodeRecord(xid,p,0,0,depth);

      assert(tree.page == p->id);
      ret = appendInternalNode(xid, p, depth, key, keySize,
                               val_page);
      assert(ret.size != INVALID_SLOT);

    } else {
      DEBUG("Appended new internal node tree depth = %d key = %d\n", depth, *(int*)key);
    }
    s->lastLeaf = ret.page;
    DEBUG("lastleaf is %lld\n", s->lastLeaf);
  } else {

    // write the new value to an existing page
    DEBUG("Writing %d to existing page# %lld\n", *(int*)key, lastLeaf->id);

    recordPostAlloc(xid, lastLeaf, ret);

    writeNodeRecord(xid, lastLeaf, ret.slot, *(int*)key, val_page);

    if(lastLeaf->id != p->id) {
      unlock(lastLeaf->rwlatch);
      releasePage(lastLeaf);
    }
  }

  // XXX do something to make this transactional...
  //  pthread_mutex_unlock(&(s->mut));
  unlock(p->rwlatch);
  releasePage(p);

  return ret;
}

static pageid_t lsmLookup(int xid, Page * const node, int depth,
                      const byte *key, size_t keySize) {
  // Start at slot 2 to skip reserved slots on page...
  if(*recordcount_ptr(node) == 2) { return -1; }
  assert(*recordcount_ptr(node) > 2);
  nodeRecord prev = readNodeRecord(xid,node,2);

  // should do binary search instead.
  for(int i = 3; i < *recordcount_ptr(node); i++) {
    nodeRecord rec = readNodeRecord(xid,node,i);

    if(depth) {

      if(prev.key <= *(int*)key && rec.key > *(int*)key) {
        pageid_t child_id = prev.ptr;
        Page * const child_page = loadPage(xid, child_id);
        readlock(child_page->rwlatch,0);
        long ret = lsmLookup(xid,child_page,depth-1,key,keySize);
        unlock(child_page->rwlatch);
        releasePage(child_page);
        return ret;
      }

    } else {

      if(prev.key == *(int*)key) {
        return prev.ptr;
      }
    }
    prev = rec;

    if(prev.key > *(int*)key) { break; }
  }

  if(depth) {

    if(prev.key <= *(int*)key) {
      pageid_t child_id = prev.ptr;
      Page * const child_page = loadPage(xid, child_id);
      readlock(child_page->rwlatch,0);
      long ret = lsmLookup(xid,child_page,depth-1,key,keySize);
      unlock(child_page->rwlatch);
      releasePage(child_page);
      return ret;
    }

  } else {

    if(prev.key == *(int*)key) {
      return prev.ptr;
    }

  }
  return -1;
}

pageid_t TlsmFindPage(int xid, recordid tree, const byte * key, size_t keySize) {
  Page * const p = loadPage(xid, tree.page);
  readlock(p->rwlatch,0);
  //lsmTreeState * s = p->impl;
  //  pthread_mutex_lock(&(s->mut));

  tree.slot = 0;
  tree.size = *recordsize_ptr(p);

  nodeRecord nr = readNodeRecord(xid, p , 0);
  //  const nodeRecord * nr = (const nodeRecord*)recordReadNew(xid, p, tree);

  int depth = nr.ptr;

  pageid_t ret = lsmLookup(xid, p, depth, key, keySize);

  //  recordReadDone(xid, p, tree, (const byte*)nr);
  //pthread_mutex_unlock(&(s->mut));
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
  lsmTreeState * state = malloc(sizeof(lsmTreeState));
  state->lastLeaf = -1;
  //pthread_mutex_init(&(state->mut),0);
  p->impl = state;
}
/**
    Free any soft state associated with the tree rooted at page p.
    This is called by the buffer manager.
*/
static void lsmPageFlushed(Page *p) {
  lsmTreeState * state = p->impl;
  //pthread_mutex_destroy(&(state->mut));
  free(state);
}
/**
   A page_impl for the root of an lsmTree.
*/
page_impl lsmRootImpl() {
  page_impl pi = fixedImpl();
  pi.pageLoaded = lsmPageLoaded;
  pi.pageFlushed = lsmPageFlushed;
  pi.page_type = LSM_ROOT_PAGE;
  return pi;
}
