#ifndef __PAGECACHE_H
#define __PAGECACHE_H

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

*/
#include <lladd/page.h>
void pageCacheInit();
void pageCacheDeinit();
Page * loadPagePtr(int pageid);

#endif
