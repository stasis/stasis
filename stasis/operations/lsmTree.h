#ifndef _LSMTREE_H__
#define _LSMTREE_H__
/**
   @file

   A log structured merge tree implementation.  This implementation
   performs atomic bulk append operations to reduce logging overheads,
   and does not support in place updates.

   However, once written, the page format of internal nodes is similar
   to that of a conventional b-tree, while leaf nodes may be provided
   by any page type that allows records to be appendend to a page, and
   read by slot id.

   For now, LSM-trees only support fixed length keys; this restriction
   will be lifted in the future.
*/
#include <assert.h>
#include <stasis/iterator.h>
typedef struct {
  recordid treeRoot;
  recordid pos;
} lladd_lsm_iterator;

typedef int(*lsm_comparator_t)(const void* a, const void* b);

void lsmTreeRegisterComparator(int id, lsm_comparator_t i);

/**
   Initialize a new LSM tree.

   @param comparator.  The id of the comparator this tree should use.
          (It must have been registered with lsmTreeRegisterComparator
          before TlsmCreate() is called.
*/
recordid TlsmCreate(int xid, int comparator, int keySize);
/**
   Free the space associated with an LSM tree.
 */
recordid TlsmDealloc(int xid, recordid tree);
/**
   Append a new leaf page to an LSM tree.  Leaves must be appended in
   ascending order; LSM trees do not support update in place.
*/
recordid TlsmAppendPage(int xid, recordid tree,
                        const byte *key,
                        long pageid);

/**
   Override the page allocation algorithm that LSM tree uses by default
*/
void TlsmSetPageAllocator(pageid_t (*allocer)(int xid, void * ignored),
                          void * config);
/**
   Lookup a leaf page.

   @param key The value you're looking for.  The first page that may
              contain this value will be returned.  (lsmTree supports
              duplicate keys...)

   @param keySize Must match the keySize passed to TlsmCreate.
                  Currently unused.
*/
pageid_t TlsmFindPage(int xid, recordid tree,
                 const byte *key);

/// ---------------  Iterator implementation

typedef struct lsmTreeNodeRecord {
  pageid_t ptr;
} lsmTreeNodeRecord;

typedef struct lsmIteratorImpl {
  Page * p;
  recordid current;
  const lsmTreeNodeRecord *t;
  int justOnePage;
} lsmIteratorImpl;

/**
    Return a forward iterator over the tree's leaf pages (*not* their
    contents).  The iterator starts before the first leaf page.

   @see iterator.h for documentation of lsmTree's iterator interface.
*/
lladdIterator_t * lsmTreeIterator_open(int xid, recordid tree);

/*
   These are the functions that implement lsmTree's iterator.

   They're public so that performance critical code can call them
   without paying for a virtual method invocation.
*/
void lsmTreeIterator_close(int xid, lladdIterator_t * it);
int  lsmTreeIterator_next (int xid, lladdIterator_t * it);

static inline int lsmTreeIterator_key  (int xid, lladdIterator_t *it,
                                        byte **key) {
  lsmIteratorImpl * impl = (lsmIteratorImpl*)it->impl;
  *key = (byte*)(impl->t+1);
  return impl->current.size;

}
static inline int lsmTreeIterator_value(int xid, lladdIterator_t *it,
                                         byte **value) {
  lsmIteratorImpl * impl = (lsmIteratorImpl*)it->impl;
  *value = (byte*)&(impl->t->ptr);
  return sizeof(impl->t->ptr);
}
static inline void lsmTreeIterator_tupleDone(int xid, void *it) { }
static inline void lsmTreeIterator_releaseLock(int xid, void *it) { }
#endif  // _LSMTREE_H__
