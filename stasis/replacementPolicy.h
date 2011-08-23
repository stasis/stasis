/** 
    @file

    Implements cache replacement policies.  Eventually, this could be
    extended to support application specific caching schemes.

    @todo Stasis used to use LRU-2S.  LRU-2S is described in Markatos
    "On Caching Searching Engine Results".  (This needs to be
    re-implemented properly.)

    For now, Stasis uses plain-old LRU.  DB-MIN would be an interesting
    extension.
*/

typedef struct replacementPolicy {
  /** Factory method */
  struct replacementPolicy* (*init)();
  /** Destructor */
  void (*deinit)  (struct replacementPolicy* impl);
  /** The page has been touched.  Reflect this fact (currently not called) */
  void (*hit)     (struct replacementPolicy* impl, Page* page);
  /** Find a page that is "stale".  Do not remove it.
   *
   *  @deprecated This function is essentially impossible to use correctly in a concurrent setting, and is not necessarily threadsafe.
   * */
  Page* (*getStale)(struct replacementPolicy* impl);
  /** Remove a page from consideration.  This method needs to increment the
   *  "pinCount" field of Page (or perform other bookkeeping), since the page
   *  may be removed multiple times before it is re-inserted.  Pages that
   *  have been removed should not be returned as "stale". */
  Page* (*remove)  (struct replacementPolicy* impl, Page* page);
  /** Atomically getStale(), and remove() the page it returns.
   *
   *  @return the page that was stale, and that has now been removed.
   */
  Page* (*getStaleAndRemove)(struct replacementPolicy* impl);
  /** Insert a page into the replacement policy, and decrement the pinCount.
   *  The page has just been "hit", and is now a candidate for getStale() to
   *  consider (unless it has a non-zero pincount).
   */
  void (*insert)  (struct replacementPolicy* impl, Page* page);
  void * impl;
} replacementPolicy;

replacementPolicy * stasis_replacement_policy_lru_init();
replacementPolicy * lruFastInit();
replacementPolicy* replacementPolicyThreadsafeWrapperInit(replacementPolicy* rp);
replacementPolicy* replacementPolicyConcurrentWrapperInit(replacementPolicy** rp, int count);
replacementPolicy* replacementPolicyClockInit(Page * pageArray, int page_count);
