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
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_groupBy.log"

START_TEST(groupBySmokeTest) {
  Tinit();
  int xid = Tbegin();
  stasis_group_t * handle = TlogStructuredGroup(xid, 1024*1024*40);
  for(int i =0; i < 10000; i++) {
    for(int j = 0; j < 100; j++) {
      int val = i * 100 + j;
      handle->put(handle, (byte*)&i, sizeof(i), (byte*)&val, sizeof(val));
    }
  }
  Tcommit(xid);
  Tdeinit();
  lladdIterator_t * it = handle->done(handle);
  int numGroups = 0;
  int numTups = 0;
  int groupSize = -1;

  int oldj = -1;

  while(Titerator_next(xid, it)) {
    int *j;
    Titerator_key(xid, it, (byte**)&j);
    Titerator_tupleDone(xid, it);
    if(*j == oldj) {
      groupSize++;
    } else {
      assert(groupSize == -1 || groupSize == 100);
      numGroups++;
      groupSize = 1;
      oldj = *j;
    }
    numTups++;
  }
  assert(numGroups == 10000);
  assert(numTups = numGroups * 100);
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("linearHashNTA");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");

  tcase_set_timeout(tc, 1200); // 20 minute timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, groupBySmokeTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
