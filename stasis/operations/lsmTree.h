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
#include <stasis/common.h>
#include <stasis/iterator.h>
#include <assert.h>
typedef struct {
  recordid treeRoot;
  recordid pos;
} lladd_lsm_iterator;

typedef int(*lsm_comparator_t)(const void* a, const void* b);
typedef void*(*lsm_page_initializer_t)(Page *, void *);
typedef pageid_t(*lsm_page_allocator_t)(int, void *);
typedef void(*lsm_page_deallocator_t)(int, void *);
typedef void(*lsm_page_forcer_t)(int, void *);

void lsmTreeRegisterComparator(int id, lsm_comparator_t i);
void lsmTreeRegisterPageInitializer(int id, lsm_page_initializer_t i);

pageid_t TlsmRegionAlloc(int xid, void *conf);
pageid_t TlsmRegionAllocRid(int xid, void *conf);
void TlsmRegionForceRid(int xid, void *conf);
typedef struct {
  recordid regionList;
  pageid_t regionCount;
  pageid_t nextPage;
  pageid_t endOfRegion;
  pageid_t regionSize;
} TlsmRegionAllocConf_t;

void TlsmRegionDeallocRid(int xid, void *conf);

extern TlsmRegionAllocConf_t LSM_REGION_ALLOC_STATIC_INITIALIZER;

/**
   Initialize a new LSM tree.

   @param xid The tranasction that is creating the tree.

   @param comparator  The id of the comparator this tree should use.
          (It must have been registered with lsmTreeRegisterComparator
          before TlsmCreate() is called.

   @param allocator A callback that will return an allocated page when called.
          This is used as the tree is extended.

   @param allocator_state A pointer that will be paseed into allocator
          when it is called.

   @param keySize
*/
recordid TlsmCreate(int xid, int comparator,
		    lsm_page_allocator_t allocator, void *allocator_state,
		    int keySize);
/**
   Free the space associated with an LSM tree.
 */
recordid TlsmDealloc(int xid,
		     lsm_page_allocator_t allocator, void *allocator_state,
		     recordid tree);
/**
   Append a new leaf page to an LSM tree.  Leaves must be appended in
   ascending order; LSM trees do not support update in place.
*/
recordid TlsmAppendPage(int xid, recordid tree,
			const byte *key,
			lsm_page_allocator_t allocator, void *allocator_state,
			long pageid);
void TlsmForce(int xid, recordid tree, lsm_page_forcer_t force,
	       void *allocator_state);
void TlsmFree(int xid, recordid tree, lsm_page_deallocator_t dealloc,
	 void *allocator_state);
/**
   Lookup a leaf page.

   @param xid The transaction that is looking up this page

   @param tree The tree to be queried.

   @param key The value you're looking for.  The first page that may
              contain this value will be returned.  (lsmTree supports
              duplicate keys...)  LSM trees currently store fixed
              length keys, so there is no keySize parameter.
*/
pageid_t TlsmFindPage(int xid, recordid tree,
                 const byte *key);
/**
   @todo TlsmFirstPage for symmetry?
 */
pageid_t TlsmLastPage(int xid, recordid tree);
/// ---------------  Iterator implementation

typedef struct lsmTreeNodeRecord {
  pageid_t ptr;
} lsmTreeNodeRecord;

typedef struct lsmIteratorImpl {
  Page * p;
  recordid current;
  lsmTreeNodeRecord *t;
  int justOnePage;
} lsmIteratorImpl;

/**
    Return a forward iterator over the tree's leaf pages (*not* their
    contents).  The iterator starts before the first leaf page.

   @see iterator.h for documentation of lsmTree's iterator interface.
*/
lladdIterator_t* lsmTreeIterator_open(int xid, recordid tree);
/**
   Return a forward iterator over the tree's leaf pages, starting
   on the given page.

 */
lladdIterator_t* lsmTreeIterator_openAt(int xid, recordid tree, const byte* key);
/*
   These are the functions that implement lsmTree's iterator.

   They're public so that performance critical code can call them
   without paying for a virtual method invocation.
*/
void lsmTreeIterator_close(int xid, lladdIterator_t * it);
int  lsmTreeIterator_next (int xid, lladdIterator_t * it);
lladdIterator_t *lsmTreeIterator_copy(int xid, lladdIterator_t* i);
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
