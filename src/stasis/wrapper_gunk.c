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

#define LINKER_STUB { assert(not_called); abort(); }

Page*  __real_loadPage(int xid, pageid_t pageid)                     LINKER_STUB
Page * __real_loadPageOfType(int xid, pageid_t pageid, pagetype_t type)
                                                                     LINKER_STUB
Page * __real_loadUninitializedPage(int xid, pageid_t pageid)        LINKER_STUB
Page * __real_loadPageForOperation(int xid, pageid_t pageid, int op, int is_recovery)
                                                                     LINKER_STUB
void   __real_releasePage(Page* p)                                   LINKER_STUB
Page * __real_getCachedPage(int xid, pageid_t pageid)                LINKER_STUB
