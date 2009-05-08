#ifndef __PAGECACHE_H
#define __PAGECACHE_H

#include <stasis/bufferManager.h>

//Page * getPage(int pageid, int locktype);
/** 
    Implements lladd's caching policy.  Looks up pageid in the cache.
    If pageid doesn't exist, then allocate a new slot for it.  If
    there are no new slots, then callback into bufferManager's
    read_page() function.  Eventually, this could be extended to
    support application specific caching schemes.

    If you would like to implement your own caching policy, implement
    the functions below.  They are relatively straightforward.
    Note that pageCache does not perform any file I/O of its own.

    The implementation of this module does not need to be threadsafe.

    @param first The caller should manually read this page by calling
    read_page() before calling pageCacheInit. 

    @todo pageCacheInit should not take a page as a parameter.

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
