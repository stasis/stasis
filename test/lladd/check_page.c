
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

#include <config.h>
#include <check.h>

#include <lladd/page.h>
#include <lladd/bufferManager.h>
#include <lladd/transactional.h>

#include "../../src/lladd/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"



#define LOG_NAME   "check_page.log"

#define RECORD_SIZE sizeof(int)

pthread_mutex_t random_mutex;

static lsn_t lsn;
static pthread_mutex_t lsn_mutex;
static void* worker_thread(void * arg_ptr) {
  Page p = *(Page*)arg_ptr;
  int i;
  lsn_t this_lsn;
  int j;
  int first = 1;
  recordid rid;
  for(i = 0; i < 1000; i++) {
    pthread_mutex_lock(&lsn_mutex);
    this_lsn = lsn;
    lsn++;
    pthread_mutex_unlock(&lsn_mutex);

    if(! first ) {
      /*      addPendingEvent(p); */
      pageReadRecord(1, p, rid, (byte*)&j);
      assert((j + 1) ==  i);
      pageDeRalloc(p, rid);
      sched_yield();
    } 
    
    first = 0;
    
    rid = pageRalloc(p, sizeof(int));
    pageWriteRecord(1, p, rid, lsn, (byte*)&i);
    sched_yield();

    assert(pageReadLSN(p) <= lsn);
  }
  
  return NULL;
}

/**
   @test 

   just run one the worker_thread function once to make sure that it passes
   without interference from other threads.

   The number of slots allocated by the page tests is too low to check
   that freed space is recovered.

   @todo While space is being reclaimed by page.c, it does not reclaim
   slots, so freeing records still does not work properly.

*/
START_TEST(pageNoThreadTest)
{
  Page p;
  p.id = 0;


  pthread_mutex_init(&lsn_mutex, NULL);

  Tinit();

  p = loadPage(0);

  worker_thread(&p);

  Tdeinit();

}
END_TEST

/** 
    Check the page implementation in the multi-threaded case.
*/
START_TEST(pageThreadTest) {

#define  THREAD_COUNT 50
  pthread_t workers[THREAD_COUNT];
  int i;
  pthread_mutex_init(&random_mutex, NULL);

  Tinit();

  Page p = loadPage(1);

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, worker_thread, &p);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }
  Tdeinit();

} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("page");
  /* Begin a new test */
  TCase *tc = tcase_create("pagethreads");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, pageNoThreadTest);
  tcase_add_test(tc, pageThreadTest); 

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
