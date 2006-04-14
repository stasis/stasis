#ifndef __PAGECACHE_H
#define __PAGECACHE_H

#include <lladd/bufferManager.h>

#define RO 0
#define RW 1

//Page * getPage(int pageid, int locktype);
/** 
    Implements lladd's caching policy.  Looks up pageid in the cache.
    If pageid doesn't exist, then allocate a new slot for it.  If
    there are no new slots, then callback into bufferManager's
    pageRead() function.  Eventually, this could be extended to
    support application specific caching schemes.

    Currently, LLADD uses LRU-2S from Markatos "On Caching Searching
    Engine Results"

    If you would like to implement your own caching policy, implement
    the three functions below.  They are relatively straightforward.
    Note that pageCache does not perform any file I/O of its own.

    @todo pageCache should not include page.h at all.  It should treat
    pages as (int, void*) pairs.  (But the page struct contains the
    pointers that pageCache manipulates..)

*/
void pageCacheInit(Page * first);
void pageCacheDeinit();

void cacheHitOnPage(Page * ret);
void cacheRemovePage(Page * ret);
void cacheInsertPage (Page * ret);
/** Return a page that is a reasonable candidate for replacement. This
    function does not actually remove the page from cache.*/
Page * cacheStalePage();

#define INITIAL 0
#define FULL    1



extern int cache_state;

#endif
