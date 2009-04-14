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
#include "check_includes.h"
#include <assert.h>
#include <unistd.h>

#ifndef NULL
#define NULL 0
#endif
#define LOG_NAME   "check_check.log"

/** @test A trivial test case for check_check
*/
START_TEST(core_succ)
{
	fail_unless(1==1, "core test suite");
}
END_TEST

/**
   @test A second trivial test case for check_check.
*/
START_TEST(core_fail)
{
  fail_unless(0==0, "i shouldn't fail anymore");
}
END_TEST

/**
   @test A second trivial test case for check_check.
*/
START_TEST(core_last)
{
  fail_unless(0==0, "i shouldn't fail anymore");
}
END_TEST

START_TEST(slep) {
  sleep(1);
}END_TEST

/**
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("check");
  /* Begin a new test */
  TCase *tc = tcase_create("core");

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, core_succ);
  tcase_add_test(tc, core_fail);
  /* --------------------------------------------- */
  suite_add_tcase(s, tc);

  tc = tcase_create("second");

  tcase_add_test(tc, core_last);
  tcase_add_test(tc, slep);

  suite_add_tcase(s, tc);
  return s;
}

#include "check_setup.h"
