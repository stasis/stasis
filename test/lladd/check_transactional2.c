/*--- This software is copyrighted by the Regents of the University of
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
#include <assert.h>

#include <lladd/transactional.h>
#include "../check_includes.h"
#define LOG_NAME   "check_transactional2.log"
/**
   Assuming that the Tset() operation is implemented correctly, checks
   that doUpdate, redoUpdate and undoUpdate are working correctly, for
   operations that use physical logging.
*/
START_TEST(transactional_smokeTest) {

  int xid;
  recordid rid;
  int foo = 2;
  int bar;

  Tinit();

  xid = Tbegin();

  rid = Talloc(xid, sizeof(int));

  Tset(xid, rid, &foo);

  bar = 4;

  Tread(xid, rid, &bar);

  fail_unless(bar == foo, NULL);

  Tcommit(xid);

  xid = Tbegin();

  bar = 4;
  Tread(xid, rid, &bar);
  fail_unless(bar == foo, NULL);


  Tabort(xid);

  Tdeinit();
}
END_TEST

/**
   Just like transactional_smokeTest, but check blobs instead.
*/
START_TEST(transactional_blobSmokeTest) {

#define ARRAY_SIZE 10000
  int xid;
  recordid rid;
  int foo[ARRAY_SIZE];
  int bar[ARRAY_SIZE];
  int i;
  for(i = 0; i < ARRAY_SIZE; i++) {
    foo[i] = i;
    bar[i] = 2 * i;
  }

  Tinit();

  xid = Tbegin();

  rid = Talloc(xid, ARRAY_SIZE * sizeof(int));

  fail_unless(rid.size == ARRAY_SIZE * sizeof(int), NULL);

  printf("TSet starting.\n"); fflush(NULL);
  Tset(xid, rid, &foo);
  printf("TSet returning.\n"); fflush(NULL);

  Tread(xid, rid, &bar);

  for(i = 0 ; i < ARRAY_SIZE; i++) {
    assert(bar[i] == foo[i]);
    fail_unless(bar[i] == foo[i], NULL);
  }

  Tcommit(xid);

  xid = Tbegin();

  for(i = 0; i < ARRAY_SIZE; i++) {
    bar[i] = 2 * i;
  }

  Tread(xid, rid, &bar);


  for(i = 0 ; i < ARRAY_SIZE; i++) {
    fail_unless(bar[i] == foo[i], NULL);
  }

  Tabort(xid);

  Tdeinit();
}
END_TEST

/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("transactional");
  /* Begin a new test */
  TCase *tc = tcase_create("transactional_smokeTest");

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, transactional_smokeTest);
  tcase_add_test(tc, transactional_blobSmokeTest);
  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
