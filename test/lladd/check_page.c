
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

#include "../../src/lladd/page.h"
#include "../../src/lladd/page/slotted.h"
#include <lladd/bufferManager.h>
#include <lladd/transactional.h>

#include "../../src/lladd/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"


#define LOG_NAME   "check_page.log"

#define RECORD_SIZE sizeof(int)
/** @todo check_page needs to use loadPage, so it contains its own
    delcaration of loadPage. (Otherwise, loadPage would clutter the
    interface, which is especially bad, since we hide the Page struct
    ffrom the user for locking purposes.)*/
Page * loadPage(int pageid);

pthread_mutex_t random_mutex;
static lsn_t lsn;
static pthread_mutex_t lsn_mutex;

static void * multiple_simultaneous_pages ( void * arg_ptr) {
  Page * p = (Page*)arg_ptr;
  int i;
  lsn_t this_lsn;
  short j;
  int first = 1;
  int k;
  recordid rid[100];
  
  for(i = 0; i < 10000; i++) {
    pthread_mutex_lock(&lsn_mutex);
    this_lsn = lsn;
    lsn++;
    pthread_mutex_unlock(&lsn_mutex);

    if(! first ) {
      for(k = 0; k < 100; k++) {
	readRecord(1, p, rid[k], (byte*)&j);

	assert((j + 1) ==  i + k);
	slottedDeRalloc(p, rid[k]);
	sched_yield();
      }
    } 
    
    first = 0;
    
    for(k = 0; k < 100; k++) {
    
      rid[k] = slottedRawRalloc(p, sizeof(short));
      i +=k;
      /*       printf("Slot %d = %d\n", rid[k].slot, i);  */
      writeRecord(1, p, lsn, rid[k], (byte*)&i);
      i -=k;
      sched_yield();
    }
      
    assert(pageReadLSN(p) <= lsn);
  }
  
  return NULL;
}


static void* worker_thread(void * arg_ptr) {
  Page * p = (Page*)arg_ptr;
  int i;
  lsn_t this_lsn;
  int j;
  int first = 1;
  recordid rid;
  for(i = 0; i < 10000; i++) {
    pthread_mutex_lock(&lsn_mutex);
    this_lsn = lsn;
    lsn++;
    pthread_mutex_unlock(&lsn_mutex);

    if(! first ) {
      readRecord(1, p, rid, (byte*)&j);
      assert((j + 1) ==  i);
      slottedDeRalloc(p, rid);
      sched_yield();
    } 
    
    first = 0;
    
    rid = slottedRawRalloc(p, sizeof(int));
    writeRecord(1, p, lsn, rid, (byte*)&i);
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
  Page * p;
  /*    p->id = 0;*/


  pthread_mutex_init(&lsn_mutex, NULL);
  
  Tinit();

  p = loadPage(0);
  slottedPageInitialize(p);
  worker_thread(p);

  unlock(p->loadlatch);

  Tdeinit();

}
END_TEST

/**
   @test
*/

START_TEST(pageCheckMacros) {
  Page p;
  byte buffer[PAGE_SIZE];
  memset(buffer, -1, PAGE_SIZE);

  p.memAddr = buffer;

  lsn_t lsn = 5;
  
  *lsn_ptr(&p) = lsn;
  *page_type_ptr(&p) = 10;
  *freespace_ptr(&p) = 15;
  *numslots_ptr(&p)  = 20;
  *slot_ptr(&p, 0)   = 30;
  *slot_ptr(&p, 1)   = 35;
  *slot_ptr(&p, 40)   = 40;
  *slot_length_ptr(&p, 0)   = 31;
  *slot_length_ptr(&p, 1)   = 36;
  *slot_length_ptr(&p, 40)   = 41;

  *bytes_from_start(&p, 0) = 50;
  *bytes_from_start(&p, 1) = 51;
  *bytes_from_start(&p, 2) = 52;
  *bytes_from_start(&p, 3) = 53;
  *bytes_from_start(&p, 4) = 54;

  assert(*lsn_ptr(&p) == lsn);
  assert(*page_type_ptr(&p) == 10);
  assert(*end_of_usable_space_ptr(&p) == 10);
  assert(*freespace_ptr(&p) == 15);
  assert(*numslots_ptr(&p)  == 20);
  assert(*slot_ptr(&p, 0) == 30);
  assert(*slot_ptr(&p, 1) == 35);
  assert(*slot_ptr(&p, 40) == 40);
  assert(*slot_length_ptr(&p, 0) == 31);
  assert(*slot_length_ptr(&p, 1) == 36);
  assert(*slot_length_ptr(&p, 40) == 41);
  
  assert(*bytes_from_start(&p, 0) == 50);
  assert(*bytes_from_start(&p, 1) == 51);
  assert(*bytes_from_start(&p, 2) == 52);
  assert(*bytes_from_start(&p, 3) == 53);
  assert(*bytes_from_start(&p, 4) == 54);

  assert(isValidSlot(&p, 0));
  assert(isValidSlot(&p, 1));
  assert(isValidSlot(&p, 40));

  /*  invalidateSlot(&p, 0);
  invalidateSlot(&p, 1);
  invalidateSlot(&p, 40);
  
  assert(!isValidSlot(&p, 0));
  assert(!isValidSlot(&p, 1));
  assert(!isValidSlot(&p, 40));*/


} END_TEST
/**
   @test 
   
   Page test that allocates multiple records
   
*/
START_TEST(pageNoThreadMultPageTest)
{
  Page * p;
  /*    p->id = 0;*/


  pthread_mutex_init(&lsn_mutex, NULL);

  Tinit();

  p = loadPage(1);
  slottedPageInitialize(p);
  multiple_simultaneous_pages(p);
  releasePage(p);
  /*  unlock(p->loadlatch); */

  Tdeinit();

}
END_TEST

/** 
    Check the page implementation in the multi-threaded case.
*/
START_TEST(pageThreadTest) {

#define  THREAD_COUNT 10
  pthread_t workers[THREAD_COUNT];
  int i;
  pthread_mutex_init(&random_mutex, NULL);
  pthread_mutex_init(&lsn_mutex, NULL);

  fail_unless(1, NULL);
  Tinit();
  Tdeinit();
  Tinit();
  fail_unless(1, NULL);

  Page * p = loadPage(2);
  slottedPageInitialize(p);
  fail_unless(1, NULL);

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, worker_thread, p);
  }
  fail_unless(1, NULL);

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  unlock(p->loadlatch);

  Tdeinit();

} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("page");
  /* Begin a new test */
  TCase *tc = tcase_create("pagethreads");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, pageCheckMacros);

  

  tcase_add_test(tc, pageNoThreadMultPageTest);
  tcase_add_test(tc, pageNoThreadTest);
  tcase_add_test(tc, pageThreadTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
