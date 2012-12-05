/*
 * segment.c
 *
 *  Created on: Jul 9, 2009
 *      Author: sears
 */

#include <stasis/page.h>

static int notSupported(int xid, Page * p) { return 0; }

page_impl segmentImpl(void) {
static page_impl pi =  {
    SEGMENT_PAGE,
    0,  // has header
    0,
    0,
    0,// readDone
    0,// writeDone
    0,
    0,
    0,
    0,
    0,
    0,
    notSupported, // is block supported
    stasis_block_first_default_impl,
    stasis_block_next_default_impl,
    stasis_block_done_default_impl,
    0,
    0,
    0,
    0,
    0,
    0, //XXX page_impl_dereference_identity,
    0,
    0,
    0,
    0,
    0
  };
  return pi;
}
