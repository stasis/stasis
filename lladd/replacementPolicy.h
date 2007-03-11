/** 
    @file

    Implements cache replacement policies.  Eventually, this could be
    extended to support application specific caching schemes.

    @todo Stasis used to use LRU-2S.  LRU-2S is described in Markatos
    "On Caching Searching Engine Results".  (This needs to be
    re-implemented properly.)

    For now, Stasis uses plain-old LRU.  DB-MIN would be an interesting
    extension.

    If you would like to implement your own caching policy, implement
    the functions below.  They are relatively straightforward.  Note
    that replacementPolicy implementations do not perform any file I/O
    of their own.

    The implementation of this module does not need to be threadsafe.

*/

#include <lladd/doubleLinkedList.h>

typedef struct replacementPolicy {
  struct replacementPolicy* (*init)();
  void (*deinit)  (struct replacementPolicy* impl);
  void (*hit)     (struct replacementPolicy* impl, void * page);
  void*(*getStale)(struct replacementPolicy* impl);
  void*(*remove)  (struct replacementPolicy* impl, void * page);
  void (*insert)  (struct replacementPolicy* impl, void * page);
  void * impl;
} replacementPolicy;

replacementPolicy * lruInit();
replacementPolicy * lruFastInit(
   struct LL_ENTRY(node_t) * (*getNode)(void * page, void * conf),
   void (*setNode)(void * page, 
		   struct LL_ENTRY(node_t) * n,
		   void * conf),
   void * conf);

