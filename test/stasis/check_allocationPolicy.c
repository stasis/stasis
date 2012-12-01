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

#include <stasis/allocationPolicy.h>
#include <stasis/constants.h>
#include <stasis/util/random.h>

#include <sys/time.h>
#include <time.h>
#include <assert.h>

#define LOG_NAME   "check_lhtable.log"

/**
   @test
*/

START_TEST(allocationPolicy_smokeTest)
{

  stasis_allocation_policy_t * ap = stasis_allocation_policy_init();
  stasis_allocation_policy_register_new_page(ap, 0, 100);
  stasis_allocation_policy_register_new_page(ap, 1, 100);
  stasis_allocation_policy_register_new_page(ap, 2, 50);
  stasis_allocation_policy_register_new_page(ap, 3, 25);

  pageid_t pageid1 = stasis_allocation_policy_pick_suitable_page(ap, 1, 51);
  assert(pageid1 != INVALID_PAGE);
  stasis_allocation_policy_alloced_from_page(ap, 1, pageid1);
  assert(pageid1 == 0 || pageid1 == 1);
  stasis_allocation_policy_update_freespace(ap, pageid1, 0);

  pageid_t pageid2 = stasis_allocation_policy_pick_suitable_page(ap, 1, 21);
  assert(pageid2 != INVALID_PAGE);
  stasis_allocation_policy_alloced_from_page(ap, 1, pageid2);
  assert(pageid2 == 3);
  stasis_allocation_policy_update_freespace(ap, pageid2, 0);

  pageid_t pageid3 = stasis_allocation_policy_pick_suitable_page(ap, 2, 51);
  stasis_allocation_policy_alloced_from_page(ap, 2, pageid3);
  assert(pageid1 != pageid3);
  stasis_allocation_policy_update_freespace(ap, pageid3, 0);

  pageid_t pageid4 = stasis_allocation_policy_pick_suitable_page(ap, 2, 51);
  assert(pageid4 == INVALID_PAGE);

  pageid_t pageid5 = stasis_allocation_policy_pick_suitable_page(ap, 2, 50);
  stasis_allocation_policy_alloced_from_page(ap, 2, pageid5);
  assert(pageid5== 2);
  stasis_allocation_policy_update_freespace(ap, pageid5, 0);

  stasis_allocation_policy_update_freespace(ap, pageid1, 100);
  stasis_allocation_policy_transaction_completed(ap, 1);
  stasis_allocation_policy_update_freespace(ap, pageid2, 25);

  pageid_t pageid6 = stasis_allocation_policy_pick_suitable_page(ap, 2, 50);
  stasis_allocation_policy_alloced_from_page(ap, 2, pageid6);
  assert(pageid6 == 1 || pageid6 == 0);
  stasis_allocation_policy_update_freespace(ap, pageid6, 0);

  pageid_t pageid7 = stasis_allocation_policy_pick_suitable_page(ap, 2, 50);
  assert(pageid7 == INVALID_PAGE);

  stasis_allocation_policy_update_freespace(ap, 3, 51);

  pageid_t pageid8 =stasis_allocation_policy_pick_suitable_page(ap, 2, 51);
  assert(pageid8 == 3);
  stasis_allocation_policy_alloced_from_page(ap, 2, pageid8);

  stasis_allocation_policy_update_freespace(ap, pageid8, 0);

  stasis_allocation_policy_transaction_completed(ap, 2);

  stasis_allocation_policy_deinit(ap);

} END_TEST

#define AVAILABLE_PAGE_COUNT_A 1000
#define AVAILABLE_PAGE_COUNT_B 10
#define FREE_MUL 100
#define XACT_COUNT 1000
static const int MAX_DESIRED_FREESPACE =
  (AVAILABLE_PAGE_COUNT_A + AVAILABLE_PAGE_COUNT_B) * FREE_MUL;

#define PHASE_ONE_COUNT 100000
#define PHASE_TWO_COUNT 500000
static int nextxid = 0;
int activexidcount = 0;
static void takeRandomAction(stasis_allocation_policy_t * ap, int * xids,
			     pageid_t * pages1, pageid_t * pages2) {
  switch(stasis_util_random64(5)) {
  case 0 : {   // find page
    int thexid = stasis_util_random64(XACT_COUNT);
    if(xids[thexid] == -1) {
      xids[thexid] = nextxid;
      nextxid++;
      activexidcount++;
      DEBUG("xid begins\n");
    }
    int thefreespace = stasis_util_random64(MAX_DESIRED_FREESPACE);
    pageid_t p =
      stasis_allocation_policy_pick_suitable_page(ap, xids[thexid], thefreespace);
    if(p != INVALID_PAGE) {
      DEBUG("alloc succeeds\n");
      // xxx validate returned value...
    } else {
      DEBUG("alloc fails\n");
    }
  } break;
  case 1 : {   // xact completed
    if(!activexidcount) { break; }
    int thexid;
    while(xids[thexid = stasis_util_random64(XACT_COUNT)] == -1) { }
    stasis_allocation_policy_transaction_completed(ap, xids[thexid]);
    xids[thexid] = -1;
    activexidcount--;
    DEBUG("complete");
  } break;
  case 2 : {   // update freespace unlocked
    int thespacediff = stasis_util_random64(MAX_DESIRED_FREESPACE) - (MAX_DESIRED_FREESPACE/2);
    int thexid;
    if(!activexidcount) { break; }
    while(xids[thexid = stasis_util_random64(XACT_COUNT)] == -1) { }
    int minfreespace;
    if(thespacediff < 0) {
      minfreespace = 0-thespacediff;
    } else {
      minfreespace = 0;
    }
    pageid_t p = stasis_allocation_policy_pick_suitable_page(ap, xids[thexid],
                                                 minfreespace);
    if(p != INVALID_PAGE) {
      int thenewfreespace = stasis_util_random64(MAX_DESIRED_FREESPACE/2)+thespacediff;
      stasis_allocation_policy_update_freespace(ap, p, thenewfreespace);
      //      printf("updated freespace unlocked");
    }
  } break;
  case 3 : {   // dealloc from page
    int thexid;
    if(!activexidcount) { break; }
    while(xids[thexid = stasis_util_random64(XACT_COUNT)] == -1) {}
    pageid_t p = pages1[stasis_util_random64(AVAILABLE_PAGE_COUNT_A)];
    stasis_allocation_policy_dealloced_from_page(ap, xids[thexid], p);
  } break;
  case 4 : {   // alloced from page
    int thexid;
    if(!activexidcount) { break; }
    while(xids[thexid = stasis_util_random64(XACT_COUNT)] == -1) {}
    pageid_t p = pages1[stasis_util_random64(AVAILABLE_PAGE_COUNT_A)];
    if(stasis_allocation_policy_can_xid_alloc_from_page(ap, xids[thexid], p)) {
      stasis_allocation_policy_alloced_from_page(ap, xids[thexid], p);
    }
  } break;
  default: abort();
  }
}

START_TEST(allocationPolicy_randomTest) {

  struct timeval time;
  gettimeofday(&time,0);
  long seed =  time.tv_usec + time.tv_sec * 1000000;
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  pageid_t * pages1 = stasis_malloc(AVAILABLE_PAGE_COUNT_A, pageid_t);
  pageid_t * pages2 = stasis_malloc(AVAILABLE_PAGE_COUNT_B, pageid_t);

  int * xids = stasis_malloc(XACT_COUNT, int);

  for(int i = 0; i < XACT_COUNT; i++) {
    xids[i] = -1;
  }


  stasis_allocation_policy_t * ap = stasis_allocation_policy_init();

  for(int i = 0; i < AVAILABLE_PAGE_COUNT_A; i++) {
    pages1[i] = i;
    stasis_allocation_policy_register_new_page(ap, i, i * FREE_MUL);
  }

  for(int k = 0; k < PHASE_ONE_COUNT; k++) {
    // Don't pass in pages2; ap doesn't know about them yet!
    takeRandomAction(ap, xids, pages1, 0);
  }

  for(int i = 0 ; i < AVAILABLE_PAGE_COUNT_B; i++) {
    pages2[i] = AVAILABLE_PAGE_COUNT_A + i;
    stasis_allocation_policy_register_new_page(ap, pages2[i], (AVAILABLE_PAGE_COUNT_A + i) * FREE_MUL);
  }

  for(int k = 0; k < PHASE_TWO_COUNT; k++) {
    takeRandomAction(ap, xids, pages1, pages2);
  }
  for(int i = 0; i < XACT_COUNT; i++) {
    if(xids[i] != INVALID_XID) { stasis_allocation_policy_transaction_completed(ap, xids[i]); }
  }

  stasis_allocation_policy_deinit(ap);

  free(pages1);
  free(pages2);

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("allocationPolicy");
  /* Begin a new test */
  TCase *tc = tcase_create("allocationPolicy");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
  // XXX this test might be flawed.
  tcase_add_test(tc, allocationPolicy_smokeTest);
  tcase_add_test(tc, allocationPolicy_randomTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
