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
#include <lladd/bufferManager.h>

#include "../check_includes.h"

#define LOG_NAME   "check_operations.log"
/**
   Assuming that the Tset() operation is implemented correctly, checks
   that doUpdate, redoUpdate and undoUpdate are working correctly, for
   operations that use physical logging.
*/
START_TEST(operation_physical_do_undo) {
  int xid = 1;
  recordid rid;
  lsn_t lsn = 0;
  int buf;
  int arg;
  LogEntry * setToTwo;

  Tinit();
  
  rid  = ralloc(xid, sizeof(int));
  buf = 1;
  arg = 2;
  setToTwo = allocUpdateLogEntry(-1, xid, OPERATION_SET, rid, (void*)&arg, sizeof(int), (void*)&buf);

  /* Do, undo and redo operation without updating the LSN field of the page. */

  writeLSN(lsn, rid.page);
  writeRecord(xid, rid, &buf);

  setToTwo->LSN = 1;
  
  doUpdate(setToTwo);

  readRecord(xid, rid, &buf);

  fail_unless(buf == 2, NULL);

  undoUpdate(setToTwo);  /* Should fail, LSN wasn't updated. */

  readRecord(xid, rid, &buf);

  fail_unless(buf == 2, NULL);
  
  redoUpdate(setToTwo);
  

  readRecord(xid, rid, &buf);

  fail_unless(buf == 2, NULL);
  
  writeLSN(3,rid.page);

  /* Now, simulate scenarios from normal operation:
         do the operation, and update the LSN, (update happens)
	 then undo, and update the LSN again.  (undo happens)
	 attempt a redo, don't update lsn      (nothing happens)

     Lower the LSN
         attempt redo                          (redo succeeds)

  */

  lsn = 0;
  buf = 1;

  writeLSN(lsn, rid.page);
  writeRecord(xid, rid, &buf);

  setToTwo->LSN = 1;
  
  doUpdate(setToTwo);
  writeLSN(setToTwo->LSN, rid.page);

  readRecord(xid, rid, &buf);

  fail_unless(buf == 2, NULL);

  undoUpdate(setToTwo);        /* Succeeds */

  readRecord(xid, rid, &buf);

  fail_unless(buf == 1, NULL);
  
  redoUpdate(setToTwo);        /* Fails */

  readRecord(xid, rid, &buf);

  fail_unless(buf == 1, NULL);
  
  writeLSN(0,rid.page);

  redoUpdate(setToTwo);        /* Succeeds */

  readRecord(xid, rid, &buf);

  fail_unless(buf == 2, NULL);

  Tdeinit();
}
END_TEST


/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("operations");
  /* Begin a new test */
  TCase *tc = tcase_create("operations_simple");

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, operation_physical_do_undo);

  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
