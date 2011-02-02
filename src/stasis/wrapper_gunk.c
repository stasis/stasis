/**
 * This file is a hack that allows -Wl,--wrap to work without conditional compilation.
 *
 * It defines empty declarations of __real_foo() functions that will never be resolved
 * when the __wrap_foo()'s are called.  If this doesn't make any sense, read ld(1)'s
 * coverage of the --wrap argument.
 *
 *  Created on: Feb 1, 2011
 *      Author: sears
 */
#include <assert.h>
#include <stasis/bufferManager.h>
static int not_called = 0;
Page* __real_loadPage(int xid, pageid_t pid) { assert(not_called); abort(); }
void __real_releasePage(Page* p) { assert(not_called); }
