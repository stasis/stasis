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

#include <stasis/util/min.h>
#include <stasis/util/random.h>

#include <assert.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_min.log"

/**
   @test

*/

START_TEST(minSmokeTest) {
  stasis_aggregate_min_t * small = stasis_aggregate_min_init(0);
  stasis_aggregate_min_t * large = stasis_aggregate_min_init(1);

  lsn_t i1 = 1;
  lsn_t i2 = 2;
  lsn_t i3 = 3;
  stasis_aggregate_min_add(small, &i2);
  stasis_aggregate_min_add(large, &i2);
  assert(2 == * stasis_aggregate_min_compute(large));
  assert(2 == * stasis_aggregate_min_compute(small));

  stasis_aggregate_min_remove(small, &i2);
  stasis_aggregate_min_remove(large, &i2);
  assert(! stasis_aggregate_min_compute(large));
  assert(! stasis_aggregate_min_compute(small));

  stasis_aggregate_min_add(small, &i1);
  stasis_aggregate_min_add(large, &i1);

  assert(1 == * stasis_aggregate_min_compute(large));
  assert(1 == * stasis_aggregate_min_compute(small));

  stasis_aggregate_min_add(small, &i3);
  stasis_aggregate_min_add(large, &i3);

  assert(1 == * stasis_aggregate_min_compute(large));
  assert(1 == * stasis_aggregate_min_compute(small));

  stasis_aggregate_min_remove(small, &i1);
  stasis_aggregate_min_remove(large, &i1);

  assert(3 == * stasis_aggregate_min_compute(large));
  assert(3 == * stasis_aggregate_min_compute(small));

  stasis_aggregate_min_add(small, &i1);
  stasis_aggregate_min_add(large, &i1);

  assert(1 == * stasis_aggregate_min_compute(large));
  assert(1 == * stasis_aggregate_min_compute(small));

  stasis_aggregate_min_add(small, &i2);
  stasis_aggregate_min_add(large, &i2);

  assert(1 == * stasis_aggregate_min_compute(large));
  assert(1 == * stasis_aggregate_min_compute(small));

  stasis_aggregate_min_remove(small, &i1);
  stasis_aggregate_min_remove(large, &i1);

  assert(2 == * stasis_aggregate_min_compute(large));
  assert(2 == * stasis_aggregate_min_compute(small));

} END_TEST

START_TEST(minRandomTest) {
  stasis_aggregate_min_t * a = stasis_aggregate_min_init(0);
  stasis_aggregate_min_t * b = stasis_aggregate_min_init(0);
  const int COUNT = 10000;

  lsn_t * vals = malloc(sizeof(lsn_t) * COUNT);
  lsn_t * bits = malloc(sizeof(lsn_t) * COUNT);
  for(int i = 0; i < COUNT; i++) {
    vals[i] = i;
    bits[i] = 0;
  }
  for(int i = 0; i < COUNT; i++) {
    if(! (i & 1023)) { printf("%d\n", i); }
    switch(stasis_util_random64(3)) {
    case 0:
    {
      int j;
      int tries = 0;
      while((j = stasis_util_random64(i))) {
        if(!bits[j]) {
          bits[j] = 1;

          stasis_aggregate_min_add(a, &vals[j]);
          stasis_aggregate_min_add(b, &vals[j]);

          break;
        }
        tries ++;
        if(tries == 100) break;
      }
    } break;
    case 1:
    {
      int j;
      int tries = 0;
      while((j = stasis_util_random64(i))) {
        if(bits[j]) {
          bits[j] = 0;

          stasis_aggregate_min_remove(a, &vals[j]);
          stasis_aggregate_min_remove(b, &vals[j]);

          break;
        }
        tries ++;
        if(tries == 100) break;
      }
    } break;
    case 2:
    {
      const lsn_t * ap = stasis_aggregate_min_compute(a);
      const lsn_t * bp = stasis_aggregate_min_compute(b);
      assert(ap == bp);
    } break;
    }
  }
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("ringBuffer");
  /* Begin a new test */
  TCase *tc = tcase_create("ringBuffer");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, minSmokeTest);
  tcase_add_test(tc, minRandomTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
