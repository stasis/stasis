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

#include <check.h>

#include <lladd/allocationPolicy.h>

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#include "../check_includes.h"
/*
#include <sys/time.h>
#include <time.h>
*/
#define LOG_NAME   "check_lhtable.log"

long myrandom(long x) {
  double xx = x;
  double r = random();
  double max = ((int64_t)RAND_MAX)+1;
  max /= xx;
  return (long)((r/max));
}

/**
   @test 
*/

START_TEST(allocationPolicy_smokeTest)
{

  availablePage ** pages = malloc(5 * sizeof(availablePage*));

  for(int i = 0; i < 4; i++) {
    pages[i] = malloc(sizeof(availablePage));
  }

  availablePage p;

  p.pageid = 0;
  p.freespace = 100;
  p.lockCount = 0;
  (*pages[0]) = p;
  
  p.pageid = 1;
  p.freespace = 100;
  (*pages[1]) = p;

  p.pageid = 2;
  p.freespace = 50;
  (*pages[2]) = p;

  p.pageid = 3;
  p.freespace = 25;
  (*pages[3]) = p;

  pages[4] = 0;
  
  allocationPolicy * ap = allocationPolicyInit();
  allocationPolicyAddPages(ap, pages);

  availablePage * p1 = allocationPolicyFindPage(ap, 1, 51);
  assert(p1);
  assert(p1->pageid == 0 || p1->pageid == 1);
  allocationPolicyUpdateFreespaceLockedPage(ap, 1, p1, 0);

  availablePage * p2 = allocationPolicyFindPage(ap, 1, 21);
  assert(p2->pageid == 3);
  allocationPolicyUpdateFreespaceLockedPage(ap, 1, p2, 0);
  
  availablePage * p3 = allocationPolicyFindPage(ap, 2, 51);
  assert(p1->pageid != p3->pageid);
  allocationPolicyUpdateFreespaceLockedPage(ap, 2, p3, 0);

  availablePage * p4 = allocationPolicyFindPage(ap, 2, 51);
  assert(!p4);

  availablePage * p5 = allocationPolicyFindPage(ap, 2, 50);
  assert(p5 && p5->pageid == 2);
  allocationPolicyUpdateFreespaceLockedPage(ap, 2, p5, 0);

  allocationPolicyUpdateFreespaceLockedPage(ap, 1, p1, 100);
  allocationPolicyTransactionCompleted(ap, 1);
  allocationPolicyUpdateFreespaceUnlockedPage(ap, p2, 25);

  availablePage * p6 = allocationPolicyFindPage(ap, 2, 50);
  assert(p6->pageid == 1 || p6->pageid == 0);
  allocationPolicyUpdateFreespaceLockedPage(ap, 2, p6, 0);
 
  availablePage * p7 = allocationPolicyFindPage(ap, 2, 50);
  assert(!p7);

  allocationPolicyUpdateFreespaceUnlockedPage(ap, pages[3], 51);

  availablePage * p8 =allocationPolicyFindPage(ap, 2, 51);
  assert(p8->pageid == 3);
  allocationPolicyUpdateFreespaceLockedPage(ap, 2, p8, 0);
  
  allocationPolicyTransactionCompleted(ap, 2);

  allocationPolicyDeinit(ap);

  free(pages);


} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("lhtable");
  /* Begin a new test */
  TCase *tc = tcase_create("lhtable");

  tcase_set_timeout(tc, 0); // disable timeouts


  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, allocationPolicy_smokeTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
