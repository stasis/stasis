
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
#include <lladd/bufferManager.h>
#include "../../src/lladd/page/indirect.h"

#include <assert.h>

#define LOG_NAME   "check_indirect.log"

int calculate_level(unsigned int pages);

START_TEST(indirectCalculateLevelTest)
{
  fail_unless(calculate_level(2) == 1, NULL);
  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE) == 1, NULL);

  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE+1) == 2, NULL);

  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE) == 2, NULL);

  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE+1) == 3, NULL);
  
  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE) == 3, NULL);

  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE+1) == 4, NULL);

  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE) == 4, NULL);
  fail_unless(calculate_level(INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE
			      *INDIRECT_POINTERS_PER_PAGE+1) == 5, NULL);
  fail_unless(calculate_level(0xFFFFFFFF) == 5, NULL);
					    
}
END_TEST


START_TEST(indirectAlloc) {
  Tinit();
  int xid = Tbegin();
  int page;
  recordid rid = rallocMany(xid, 1, 255);

  page = rid.page;
  
  fail_unless(rid.page == page, NULL);
  fail_unless(rid.slot == RECORD_ARRAY, NULL);
  fail_unless(rid.size == 1, NULL);

  Page * p = loadPage(page);

  int page_type = *page_type_ptr(p);

  assert(page_type == SLOTTED_PAGE);

  fail_unless(page_type == SLOTTED_PAGE, NULL);
  
  releasePage(p);
  
  /* ------------------------------- */


  rid = rallocMany(xid, 2000, 255);

  page = rid.page;

  fail_unless(rid.page == page, NULL);
  fail_unless(rid.slot == RECORD_ARRAY, NULL);
  fail_unless(rid.size == 2000, NULL);

  p = loadPage(page);

  page_type = *page_type_ptr(p);

  assert(page_type == INDIRECT_PAGE);

  fail_unless(page_type == INDIRECT_PAGE, NULL);
  


  printf("{page = %d, slot = %d, size = %ld}\n", rid.page, rid.slot, rid.size);

  releasePage(p);

  /*----------------- */

  rid = rallocMany(xid, 2, 1000000);

  page = rid.page;

  fail_unless(rid.page == page, NULL);
  fail_unless(rid.slot == RECORD_ARRAY, NULL);
  fail_unless(rid.size == 2, NULL);

  p = loadPage(page);

  page_type = *page_type_ptr(p);

  assert(page_type == INDIRECT_PAGE);

  fail_unless(page_type == INDIRECT_PAGE, NULL); 
 


  printf("{page = %d, slot = %d, size = %ld}\n", rid.page, rid.slot, rid.size);

  releasePage(p);
  
  Tcommit(xid);

  Tdeinit();


} END_TEST

START_TEST(indirectAccessDirect) {

  Tinit();

  int page;
  int xid = Tbegin();
  recordid rid = rallocMany(xid, sizeof(int), 500);
  page = rid.page;
  /* Make sure that it didn't create any indirect pages. */

  Page * p = loadPage(page);

  int page_type = *page_type_ptr(p);

  assert(page_type == SLOTTED_PAGE);

  fail_unless(page_type == SLOTTED_PAGE, NULL);
  
  releasePage(p);

  Tcommit(xid);

  xid = Tbegin();
  
  for(int i = 0; i < 500; i++) {
    rid.slot = i;
    Tset(xid, dereferenceRID(rid), &i);
  }
  
  Tcommit(xid);
  xid = Tbegin();
  
  for(int i = 0; i < 500; i++) {
    rid.slot = i;
    int j;
    Tread(xid, dereferenceRID(rid), &j);
    assert(j == i);
  }
  
  Tcommit(xid);

} END_TEST

START_TEST(indirectAccessIndirect) {

  Tinit();

  int page;

  int xid = Tbegin();

  recordid rid = rallocMany(xid, sizeof(int), 500000);
  page = rid.page;
  /* Make sure that it didn't create any indirect pages. */

  Page * p = loadPage(page);

  int page_type = *page_type_ptr(p);

  assert(page_type == INDIRECT_PAGE);

  fail_unless(page_type == INDIRECT_PAGE, NULL);
 
  Tcommit(xid);
  xid = Tbegin();
  releasePage(p);

  for(int i = 0; i < 500000; i++) {
    rid.slot = i;
    Tset(xid, dereferenceRID(rid), &i);
  }
  
  Tcommit(xid);
  xid = Tbegin();
  
  for(int i = 0; i < 500000; i++) {
    rid.slot = i;
    int j;
    Tread(xid, dereferenceRID(rid), &j);
    assert(j == i);
  }
  
  Tcommit(xid);

  Tdeinit();
  
} END_TEST

/** @test check that the indirectPageRecordCount() function works
    properly for both INDIRECT_PAGES and for SLOTTED_PAGES. */
START_TEST(indirectSizeTest) {

  Tinit();

  int xid = Tbegin();

  recordid rid = rallocMany(xid, sizeof(int), 20);
  int count = indirectPageRecordCount(rid);
  assert(count == 20);

  recordid rid2 = rallocMany(xid, sizeof(int), 5000);
  
  count = indirectPageRecordCount(rid2);
  assert(count == 5000);

  Tcommit(xid);

  Tdeinit();

} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("indirect");
  /* Begin a new test */
  TCase *tc = tcase_create("indirect");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, indirectCalculateLevelTest);
  tcase_add_test(tc, indirectAlloc);
  tcase_add_test(tc, indirectAccessDirect);
  tcase_add_test(tc, indirectAccessIndirect);
  tcase_add_test(tc, indirectSizeTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
