/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/
/*******************************
 * $Id$
 *
 * implementation of the page buffer
 * *************************************************/

#include <config.h>

#ifdef PROFILE_LATCHES_WRITE_ONLY



#define _GNU_SOURCE
#include <stdio.h>  // Need _GNU_SOURCE for asprintf
#include <stasis/lhtable.h>

#endif

#include <stasis/common.h>

#include <assert.h>

#include <stasis/bufferManager.h>
#include <stasis/bufferManager/pageArray.h>
#include <stasis/bufferManager/bufferHash.h>

//#include <stasis/bufferManager/legacy/pageCache.h>
#include <stasis/bufferManager/legacy/legacyBufferManager.h>

#include <stasis/bufferPool.h>

#include <stasis/lockManager.h>
#include <stasis/pageHandle.h>

#undef loadPage
#undef releasePage
#undef Page

#ifdef LONG_TEST
#define PIN_COUNT
#endif

#ifdef PROFILE_LATCHES_WRITE_ONLY

// These should only be defined if PROFILE_LATCHES_WRITE_ONLY is set.

#undef loadPage
#undef releasePage

pthread_mutex_t profile_load_mutex = PTHREAD_MUTEX_INITIALIZER;
struct LH_ENTRY(table) * profile_load_hash = 0;
struct LH_ENTRY(table) * profile_load_pins_hash = 0;

#endif

#ifdef PIN_COUNT
pthread_mutex_t pinCount_mutex = PTHREAD_MUTEX_INITIALIZER;
int pinCount = 0;
#endif

#ifdef PROFILE_LATCHES_WRITE_ONLY

compensated_function Page * __profile_loadPage(int xid, pageid_t pageid, char * file, int line) {

  Page * ret = loadPage(xid, pageid);


  pthread_mutex_lock(&profile_load_mutex);

  char * holder = LH_ENTRY(find)(profile_load_hash, &ret, sizeof(void*));
  int * pins = LH_ENTRY(find)(profile_load_pins_hash, &ret, sizeof(void*));

  if(!pins) {
    pins = malloc(sizeof(int));
    *pins = 0;
    LH_ENTRY(insert)(profile_load_pins_hash, &ret, sizeof(void*), pins);
  }

  if(*pins) {
    assert(holder);
    char * newHolder;
    asprintf(&newHolder, "%s\n%s:%d", holder, file, line);
    free(holder);
    holder = newHolder;
  } else {
    assert(!holder);
    asprintf(&holder, "%s:%d", file, line);
  }
  (*pins)++;
  LH_ENTRY(insert)(profile_load_hash, &ret, sizeof(void*), holder);
  pthread_mutex_unlock(&profile_load_mutex);

  return ret;

}


compensated_function void  __profile_releasePage(Page * p) {
  pthread_mutex_lock(&profile_load_mutex);

  int * pins = LH_ENTRY(find)(profile_load_pins_hash, &p, sizeof(void*));

  assert(pins);

  if(*pins == 1) {

    char * holder = LH_ENTRY(remove)(profile_load_hash, &p, sizeof(void*));
    assert(holder);
    free(holder);

  }

  (*pins)--;

  pthread_mutex_unlock(&profile_load_mutex);

  releasePage(p);


}

#endif

Page * (*loadPageImpl)(int xid, pageid_t pageid, pagetype_t type) = 0;
Page * (*loadUninitPageImpl)(int xid, pageid_t pageid) = 0;
void   (*releasePageImpl)(Page * p) = 0;
void (*writeBackPage)(Page * p) = 0;
void (*forcePages)() = 0;
void (*forcePageRange)(pageid_t start, pageid_t stop) = 0;
void   (*stasis_buffer_manager_close)()  = 0;
void   (*stasis_buffer_manager_simulate_crash)()  = 0;

Page * loadPage(int xid, pageid_t pageid) {
  // This lock is released at Tcommit()
  if(globalLockManager.readLockPage) { globalLockManager.readLockPage(xid, pageid); }
  return loadPageImpl(xid, pageid, UNKNOWN_TYPE_PAGE);

}
Page * loadPageOfType(int xid, pageid_t pageid, pagetype_t type) {
  if(globalLockManager.readLockPage) { globalLockManager.readLockPage(xid, pageid); }
  return loadPageImpl(xid, pageid, type);
}
Page * loadUninitializedPage(int xid, pageid_t pageid) {
  // This lock is released at Tcommit()
  if(globalLockManager.readLockPage) { globalLockManager.readLockPage(xid, pageid); }

  return loadUninitPageImpl(xid, pageid);

}

void releasePage(Page * p) {
  releasePageImpl(p);
}

int stasis_buffer_manager_open(int type, stasis_page_handle_t * ph) {
  bufferManagerType = type;
  static int lastType = 0;
  if(type == BUFFER_MANAGER_REOPEN) {
    type = lastType;
  }
  lastType = type;
  if(type == BUFFER_MANAGER_DEPRECATED_HASH) {
    stasis_buffer_manager_deprecated_open(ph);
    return 0;
  } else if (type == BUFFER_MANAGER_MEM_ARRAY) {
    stasis_buffer_manager_mem_array_open();
    ph->close(ph); // XXX should never have been opened in the first place!
    return 0;
  } else if (type == BUFFER_MANAGER_HASH) {
    stasis_buffer_manager_hash_open(ph);
    return 0;
  } else {
    // XXX error handling
    abort();
  }
}
