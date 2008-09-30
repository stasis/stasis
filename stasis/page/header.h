/**
   @file

   @todo Is header.[ch] used for anything any more?  No one seems to call
   into them.  Delete them?
*/

//void headerPageInitialize(Page * p);
int headerPageInitialize();
void freePageInitialize(Page * freepage, Page *headerpage);
#define headerFreepage_ptr(page)       ints_from_end((page), 1)
#define headerFreepagelist_ptr(page)   ints_from_end((page), 2)

/** @todo allow for effecient freeing of blocks of multiple pages, and
    implement worst-fit page reclamation (as a special case, use
    best-fit for single page allocations.) */

#define nextfreepage_ptr(page)   ints_from_end((page), 1)
