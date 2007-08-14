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

typedef struct {
  int id;
  // fcn pointer...
} comparator_impl;

void lsmTreeRegisterComparator(comparator_impl i);
extern const int MAX_LSM_COMPARATORS;

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
                        const byte *key, size_t keySize,
                        long pageid);
/**
   Lookup a leaf page.

   @param key The value you're looking for.  The first page that may
              contain this value will be returned.  (lsmTree supports
              duplicate keys...)

   @param keySize Must match the keySize passed to TlsmCreate.
                  Currently unused.
*/
pageid_t TlsmFindPage(int xid, recordid tree,
                 const byte *key, size_t keySize);

/**
    Return a forward iterator over the tree's leaf pages (*not* their
    contents).
*/
lladdIterator_t * TlsmIterator(int xid, recordid hash);

/**
   These are the functions that implement lsmTree's iterator.

   They're public so that performance critical code can call them
   without paying for a virtual method invocation.

   XXX should they be public?
*/
void lsmTreeIterator_close(int xid, void * it);
int  lsmTreeIterator_next (int xid, void * it);
int  lsmTreeIterator_key  (int xid, void * it, byte **key);
int  lsmTreeIterator_value(int xid, void * it, byte **value);

#endif  // _LSMTREE_H__
