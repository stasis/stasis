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
#include "../check_includes.h"

#include <lladd/transactional.h>

#include <assert.h>
#include <limits.h>
#include <math.h>

#define LOG_NAME   "check_linkedListNTA.log"
START_TEST(linkedListNTAtest)
{
  Tinit();

  int xid = Tbegin();
  
  recordid linkedList = TlinkedListCreate(xid, sizeof(int), sizeof(recordid));
  int i;
  for(i = 0; i < 1000; i++) {
    recordid val;
    recordid * val2 = NULL;
    val.page = i * 1000;
    val.slot = val.page * 1000;
    val.size = val.slot * 1000;
    assert(-1==TlinkedListFind(xid, linkedList, (byte*)(&i), sizeof(int), (byte**)&val2));
    TlinkedListInsert(xid, linkedList, (byte*)&i, sizeof(int), (byte*)&val, sizeof(recordid));
    assert(sizeof(recordid)==TlinkedListFind(xid, linkedList, (byte*)(&i), sizeof(int), (byte**)&val2));
    assert(!memcmp(&val, val2, sizeof(recordid)));
    free(val2);
  }
  
  Tcommit(xid);
  
  xid = Tbegin();
  for(i = 0; i < 1000; i+=10) {
    recordid * val2 = NULL;
    assert(sizeof(recordid)==TlinkedListFind(xid, linkedList, (byte*)(&i), sizeof(int), (byte**)&val2));
    assert(val2->page == i * 1000);
    assert(val2->slot == i * 1000 * 1000);
    assert(val2->size == i * 1000 * 1000 * 1000);
    free(val2);
    assert(TlinkedListRemove(xid, linkedList, (byte*)&i, sizeof(int)));
    assert(-1==TlinkedListFind(xid, linkedList, (byte*)(&i), sizeof(int), (byte**)&val2));
    assert(!TlinkedListRemove(xid, linkedList, (byte*)&i, sizeof(int)));
  }
  Tabort(xid);
  xid = Tbegin();
  for(i = 0; i < 1000; i++) {
    recordid * val2;
    assert(sizeof(recordid)==TlinkedListFind(xid, linkedList, (byte*)(&i), sizeof(int), (byte**)&val2));
    assert(val2->page == i * 1000);
    assert(val2->slot == i * 1000 * 1000);
    assert(val2->size == i * 1000 * 1000 * 1000);
    free(val2);
  }
  Tcommit(xid);
  Tdeinit();
} END_TEST



Suite * check_suite(void) {
  Suite *s = suite_create("linkedListNTA");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");


  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, linkedListNTAtest);
  
  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
