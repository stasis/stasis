/**
   Allocates and deallocates regions of pages.  The page before each
   region is of type BOUNDARY_TAG.  All regions except the last one in
   the page file have a BOUNDARY_TAG page immediately after the end of
   the region.

   Each region is managed by an allocation manager, which manages the
   allocation of pages within a region.  The contents of pages within
   a newly allocated region are undefined.
*/

int TregionAlloc(int xid, int pageCount);
void TregionFree(int xid, int firstPage);
int TregionSize(int xid, int firstPage);

Operation getRegionAlloc();
Operation getRegionFree();

// XXX need callbacks to handle transaction commit/abort.
