/**
 * @file A special-purpose slotted page implementation for latch-free data structures
 *
 * This page format is similar to that provided by slotted.h (and
 * uses an identical layout).  The difference is that this format
 * allows readers and writers to access the same page concurrently.
 *
 * It does so by making use of GCC atomic operations, and by providing
 * a restricted API.
 *
 * Only one thread may write to a given page at a time (this restriction
 * may be limited in the future), records may only be written once, and
 * the space taken up by freed records is not reused.  Instead, the writer
 * thread must (somhehow) ensure that no readers are accessing the page,
 * and then must either reinitialize the page (losing its contents), or
 * simply free the page, allowing Stasis' page allocation routines to
 * reuse the underlying storage.
 *
 * Readers are latch-free; they do not perform any explicit synchronization
 * with the writer, except when they synchronize so that space can be reused.
 *
 *  Created on: Aug 19, 2010
 *      Author: sears
 */
#ifndef LFSLOTTED_H_
#define LFSLOTTED_H_

#include <stasis/page/slotted.h>

void stasis_page_slotted_latch_free_init();
void stasis_page_slotted_latch_free_deinit();
page_impl stasis_page_slotted_latch_free_impl();

#endif /* LFSLOTTED_H_ */
