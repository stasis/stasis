
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
#include <stdlib.h>
#include <lladd/compensations.h>

#include <assert.h>
#include "../check_includes.h"


#define LOG_NAME   "check_compensations.log"

void decrement(void * j) {
  int * i = (int*) j;
  *i = *i-1;
}

void nested(int * i);
void happy_nest(int * i);
void nested2(int * i) {
  begin_action(NULL, NULL) {
    nested(i);
  } compensate;
  assert(0);
}
void nested3(int * i) {
  begin_action(NULL, NULL) {
    happy_nest(i);
  } compensate;
}

void nested(int * i) {
  begin_action(decrement, i) {
    (*i)++;
    compensation_set_error(1);
    break; 
    assert(0);
  } end_action;
  assert(0);
}
void happy_nest(int * i) {
  begin_action(decrement, i) {
    (*i)++;
  } compensate;
}

/**
   @test 

*/
START_TEST(compensationTest)
{
  compensations_init();
  
  int * i = malloc (sizeof(int));
  *i = 0;
  begin_action(decrement, i) {
    (*i)++;
    assert(*i == 1);
  } compensate;

  assert(*i == 0);

  begin_action(decrement, i) {
    (*i)++;
    assert(*i == 1);
  } end_action;

  assert(*i == 1);

  nested(i); // has no effect.
  assert(compensation_error() == 1);
  assert(*i == 1);

  compensation_clear_error();

  nested2(i); // has no effect.
  assert(compensation_error() == 1);
  assert(*i == 1);
  compensation_clear_error();

  nested3(i); // has no effect.
  assert(compensation_error() == 0);
  assert(*i == 1);

  free(i);

  compensations_deinit();

}
END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("compensations");
  /* Begin a new test */
  TCase *tc = tcase_create("simple_compensations");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, compensationTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
