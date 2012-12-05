#ifndef _ROSE_COMPRESSION_PSTAR_IMPL_H__
#define _ROSE_COMPRESSION_PSTAR_IMPL_H__

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

#include <string.h>
#include <assert.h>

#include "pstar.h"
#include "for.h"

namespace rose {

/**
   Appends a value to a page managed by pstar.  For now, most of the
   "real work" is handled by the compression algorithm.

   This function simply checks the return value from the plugin.  If
   the value should be stored as an exception, then it is prepended to
   the list of exceptions at end of the page.  The compressed data is kept
   at the beginning of the page.
*/
template <class COMPRESSOR, class TYPE>
slot_index_t
Pstar<COMPRESSOR, TYPE>::append(int xid, const TYPE dat) {

  slot_index_t ret = plug_.append(xid, dat, freespace_ptr(), p_->memAddr,
                                  &free_bytes_);

  return free_bytes_ >= 0 ? ret : NOSPACE;
}

//  The rest of this file interfaces with Stasis -------------------------

/**
  Implementation of the Stasis pageLoaded() callback.

  @see stasis/page.h
*/
template <class COMPRESSOR, class TYPE>
static void pStarLoaded(Page * p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  p->impl = new Pstar<COMPRESSOR, TYPE>(p);
}
/**
  Implementation of the Stasis pageFlushed() callback.
*/
template <class COMPRESSOR, class TYPE>
static void pStarFlushed(Page * p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
}

template <class COMPRESSOR, class TYPE>
static void pStarCleanup(Page * p) {
  delete (Pstar<COMPRESSOR, TYPE>*)p->impl;
}
/**
   Basic page_impl for pstar

   @see stasis/page.h

*/
static const page_impl pstar_impl = {
  -1,
  0,  // pStarRead,
  0,       // pStarWrite,
  0,  // pStarReadDone,
  0,       // pStarWriteDone,
  0,  // pStarGetType,
  0,  // pStarSetType,
  0,  // pStarGetLength,
  0,  // pStarFirst,
  0,  // pStarNext,
  0,  // pStarIsBlockSupported,
  0,  // pStarBlockFirst,
  0,  // pStarBlockNext,
  0,  // pStarBlockDone,
  0,  // pStarFreespace,
  0,       // pStarCompact,
  0,       // pStarPreRalloc,
  0,       // pStarPostRalloc,
  0,       // pStarFree,
  0,       // dereference_identity,
  0,  // pStarLoaded,
  0,  // pStarFlushed
  0,  // pStarCleanup
};

/**
   Be sure to call "registerPageType(Pstar<...>::impl())" once for
   each template instantiation that Stasis might encounter, even if a
   particular binary might not use that instantiation.  This must be
   done before calling Tinit().

   @see registerPageType() from Stasis.
*/
template<class COMPRESSOR, class TYPE>
page_impl
Pstar<COMPRESSOR, TYPE>::impl(void) {
  page_impl ret   = pstar_impl;
  ret.page_type   = plugin_id<Pstar<COMPRESSOR,TYPE>,COMPRESSOR,TYPE>();
  ret.pageLoaded  = pStarLoaded<COMPRESSOR, TYPE>;
  ret.pageFlushed = pStarFlushed<COMPRESSOR, TYPE>;
  ret.pageCleanup = pStarCleanup<COMPRESSOR, TYPE>;
  return ret;
}

} // namespace rose
#endif  // _ROSE_COMPRESSION_PSTAR_IMPL_H__
