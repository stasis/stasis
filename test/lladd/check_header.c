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
#include <stdio.h>
#include <lladd/transactional.h>
#include "../check_includes.h"
#include <assert.h>

#define LOG_NAME   "check_header.log"

/** 
    @test
    Does some tests to make sure that LLADD's header page isn't list between 
    restarts. (This bug hurt!)
*/
START_TEST (header_test) {
  
 int xid;
  recordid rid1, rid2;
  int p1; int p2;

  Tinit();

  xid = Tbegin();

  p1 = TpageAlloc(xid);

  Tcommit(xid);

  Tdeinit();

  Tinit();

  xid = Tbegin();

  p2 = TpageAlloc(xid);

  Tcommit(xid);

  Tdeinit();

  printf("Page 1 = %d, Page 2 = %d\n", p1, p2);

  assert(p1 != p2);

  Tinit();
  
  xid = Tbegin();
  rid1 = Talloc(xid, 100);
  //TpageAlloc(xid);
  char buf[100];
  memset(buf, 10, 100);
  Tset(xid, rid1, buf);

  Tcommit(xid);

  Tdeinit(); 
  Tinit();

  xid = Tbegin();
  rid2 = Talloc(xid, 100);
  rid2 = Talloc(xid, 100);
  rid2 = Talloc(xid, 100);
  Tcommit(xid);
  
/*  printf("rid1={%d,%d,%ld} rid2={%d,%d,%ld}\n", 
	 rid1.page, rid1.slot, rid1.size,
	 rid2.page, rid2.slot, rid2.size);  */
  
  assert(memcmp(&rid1, &rid2, sizeof(recordid)));
  
  // Tdeinit();
  
} END_TEST


/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("recovery_suite");
  /* Begin a new test */
  TCase *tc = tcase_create("recovery");
  /* void * foobar; */  /* used to supress warnings. */
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, header_test);

  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);

  return s;
}

#include "../check_setup.h"
