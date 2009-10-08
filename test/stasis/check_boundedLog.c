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

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#define LOG_NAME   "check_boundedLog.log"
#define NUM_XACTS 10000
#define NUM_CONCURRENT_XACTS 250

/** @test  Simplest case of log w/ hard bound; one short xact at a time.
*/
START_TEST(boundedLogTest) {
  stasis_log_type = LOG_TO_MEMORY;
  stasis_log_in_memory_max_entries = 1000;
  stasis_log_softcommit = 1;
  Tinit();
  int xid = Tbegin();
  pageid_t region_start = TregionAlloc(xid, NUM_XACTS, 0);
  Tcommit(xid);

  for(uint64_t i = 0; i < NUM_XACTS; i++) {
    xid = Tbegin();
    TinitializeFixedPage(xid, region_start + i, sizeof(uint64_t));
    recordid rid = {region_start + i, 0, sizeof(uint64_t)};
    Tset(xid, rid, &i);
    Tcommit(xid);
  }

  Tdeinit();

}END_TEST

/** @test  More complicated case of log w/ hard bound; many xacts at a time.
*/
START_TEST(boundedLogConcurrentTest) {
  stasis_log_type = LOG_TO_MEMORY;
  stasis_log_in_memory_max_entries = 1000;
  stasis_log_softcommit = 1;
  Tinit();
  int xid = Tbegin();
  pageid_t region_start = TregionAlloc(xid, NUM_XACTS, 0);
  Tcommit(xid);

  for(uint64_t i = 0; i < NUM_XACTS; i++) {
    int xids[NUM_CONCURRENT_XACTS];
    for(int j = 0; j < NUM_CONCURRENT_XACTS; j++) {
      xids[j] = Tbegin();
    }
    for(int j = 0; j < NUM_CONCURRENT_XACTS; j++) {
      TinitializeFixedPage(xids[j], region_start + i, sizeof(uint64_t));
      recordid rid = {region_start + i, 0, sizeof(uint64_t)};
      Tset(xids[j], rid, &i);
      i++;
    }
    for(int j = 0; j < NUM_CONCURRENT_XACTS; j++) {
      Tcommit(xids[j]);
    }
  }

  Tdeinit();

}END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("boundedLog");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");

  tcase_set_timeout(tc, 0); // disable timeouts if test takes more than 2 sec - it would get killed

  /* Sub tests are added, one per line, here */
   tcase_add_test(tc, boundedLogTest);
   tcase_add_test(tc, boundedLogConcurrentTest);
  /* --------------------------------------------- */

   tcase_add_checked_fixture(tc, setup, teardown);// leave stuff below here.

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
