/*
 * check_dirtyPageTable.c
 *
 *  Created on: Aug 5, 2009
 *      Author: sears
 */
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

#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/pageArray.h>

#include <stasis/dirtyPageTable.h>
#include <stasis/util/random.h>

#include <sys/time.h>
#include <time.h>
#include <assert.h>

#define LOG_NAME   "check_dirtyPageTable.log"

/**
   @test
*/

#define NUM_WORKERS 100
#ifdef LONG_TEST
#define NUM_STEPS 10000000
#define NUM_PAGES 1000
#else
#define NUM_STEPS 100000
#define NUM_PAGES 100
#endif
void * worker(void*arg) {
  stasis_dirty_page_table_t * dpt = (stasis_dirty_page_table_t *)stasis_runtime_dirty_page_table();
  for(int i = 0; i < NUM_STEPS; i++) {
    pageid_t page = stasis_util_random64(NUM_PAGES);
    Page * p = loadPage(-1, page);
    writelock(p->rwlatch, 0);
    if(! (i % 100000)) { printf("."); fflush(stdout); }
    switch(stasis_util_random64(6)) {
    case 0: {
      stasis_dirty_page_table_set_dirty(dpt, p);
    } break;
    case 1: {
      stasis_dirty_page_table_set_clean(dpt, p);
    } break;
    case 2: {
      stasis_dirty_page_table_is_dirty(dpt, p);
    } break;
    case 3: {
      stasis_dirty_page_table_flush(dpt);
    } break;
    case 4: {
      stasis_dirty_page_table_minRecLSN(dpt);
    } break;
    case 5: {
      unlock(p->rwlatch);
      releasePage(p);
      pageid_t start = stasis_util_random64(NUM_PAGES);
      pageid_t stop = stasis_util_random64(NUM_PAGES);
      if(start > stop) {
        page = start;
        start = stop;
        stop = page;
      }
      stasis_dirty_page_table_flush_range(dpt, start, stop);
      p = loadPage(-1, start);
      writelock(p->rwlatch, 0);
    } break;
    default: abort();
    };
    unlock(p->rwlatch);
    releasePage(p);
  }
  return 0;
}
START_TEST(dirtyPageTable_randomTest) {
  stasis_buffer_manager_factory = stasis_buffer_manager_mem_array_factory;
  Tinit();
  worker(0);
  Tdeinit();
} END_TEST
START_TEST(dirtyPageTable_threadTest) {
  stasis_buffer_manager_factory = stasis_buffer_manager_mem_array_factory;
  Tinit();
  pthread_t thread[NUM_WORKERS];
  for(int i = 0; i < NUM_WORKERS; i++) {
    pthread_create(&thread[i], 0, worker, 0);
  }
  for(int i = 0; i < NUM_WORKERS; i++) {
    pthread_join(thread[i], 0);
  }
  Tdeinit();
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("allocationPolicy");
  /* Begin a new test */
  TCase *tc = tcase_create("allocationPolicy");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, dirtyPageTable_randomTest);
  tcase_add_test(tc, dirtyPageTable_threadTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
