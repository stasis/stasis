
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

#include "../../src/stasis/page.h"
#include <stasis/bufferManager.h>
#include <stasis/transactional.h>
#include <stasis/truncation.h>
#include <stasis/logger/logger2.h>

#include "../../src/stasis/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"

#define LOG_NAME   "check_pageOperations.log"
#include "../../src/stasis/logger/logWriter.h"
extern int numActiveXactions;

START_TEST(pageOpCheckRecovery) {
  Tinit();

  int xid = Tbegin();

  int pageid1 = TpageAlloc(xid);

  int pageid2 =  TpageAlloc(xid);
  
  Page p;
  byte memAddr[PAGE_SIZE];

  p.memAddr = memAddr;

  memset(p.memAddr, 1, PAGE_SIZE);
  // Reset the page type after overwriting it with memset.  Otherwise, Stasis
  // will try to interpret it when it flushes the page to disk.
  *stasis_page_type_ptr(&p) = 0;

  TpageSet(xid, pageid1, p.memAddr);

  memset(p.memAddr, 2, PAGE_SIZE);
  *stasis_page_type_ptr(&p) = 0;

  TpageSet(xid, pageid2, p.memAddr);
  
  Tcommit(xid);

  xid = Tbegin();

  TpageAlloc(xid);  /* This test doesn't check for leaks, so we don't need to remember this pageid. */
  TpageDealloc(xid, pageid1);
  TpageDealloc(xid, pageid2);
  TuncleanShutdown();

  Tinit();

  xid = Tbegin();
  
  int pageid3 = TpageAlloc(xid);
  memset(p.memAddr, 3, PAGE_SIZE);
  *stasis_page_type_ptr(&p) = 0;
  TpageSet(xid, pageid3, p.memAddr);
  

  byte newAddr[PAGE_SIZE];

  memset(p.memAddr, 1, PAGE_SIZE);
  *stasis_page_type_ptr(&p) = 0;
  TpageGet(xid, pageid1, newAddr);
  assert(!memcmp(p.memAddr, newAddr, PAGE_SIZE-sizeof(lsn_t)));

  memset(p.memAddr, 2, PAGE_SIZE);
  *stasis_page_type_ptr(&p) = 0;
  TpageGet(xid, pageid2, newAddr);
  assert(!memcmp(p.memAddr, newAddr, PAGE_SIZE-sizeof(lsn_t)));

  memset(p.memAddr, 3, PAGE_SIZE);
  *stasis_page_type_ptr(&p) = 0;
  TpageGet(xid, pageid3, newAddr);
  assert(!memcmp(p.memAddr, newAddr, PAGE_SIZE-sizeof(lsn_t)));
  Tcommit(xid);
  Tdeinit();

} END_TEST

/**
   @test
*/

START_TEST(pageOpCheckAllocDealloc) {

#ifdef REUSE_PAGES

  Tinit();

  int xid = Tbegin();

  int pageid = TpageAllocMany(xid, 100);

  fail_unless(pageid == 1, NULL);

  pageid = TpageAlloc(xid);
  fail_unless(pageid == 101, NULL);

  TpageDealloc(xid, 52);
  
  pageid = TpageAlloc(xid);

  fail_unless(pageid == 52, NULL);
  printf("\nA\n"); fflush(NULL);
  Tcommit(xid);
  xid = Tbegin();
  printf("\nEverything below this aborts\n"); fflush(NULL);
  for(int i = 1; i < 102; i++) {
    TpageDealloc(xid, i);
  }
  printf("\nB\n"); fflush(NULL);
  for(int i = 0; i < 50; i++) {
    pageid = TpageAlloc(xid);
    assert(pageid < 102);
  }
  printf("\nC - aborting\n"); fflush(NULL);
  Tabort(xid);
  printf("\nD - aborted\n"); fflush(NULL);
  xid = Tbegin();

  pageid = TpageAlloc(xid);
  printf("\nE\n"); fflush(NULL);
  fail_unless(pageid == 102, NULL);

  Tcommit(xid);

  Tdeinit();

#else

  printf(" Skipping 1 check for page leaks since page reuse is diabled.\n");

#endif

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("pageOperations");
  /* Begin a new test */
  TCase *tc = tcase_create("pageOperations");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, pageOpCheckAllocDealloc);
  if(LOG_TO_MEMORY != loggerType) { 
    tcase_add_test(tc, pageOpCheckRecovery);
  }
  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
