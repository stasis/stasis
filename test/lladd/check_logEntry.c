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
#include <assert.h>

#include <lladd/transactional.h>

#include "../check_includes.h"

#define LOG_NAME   "check_logEntry.log"

START_TEST(rawLogEntryAlloc)
{
  LogEntry * log = allocCommonLogEntry(200, 1, 4);
  fail_unless(log->LSN == -1, NULL);
  fail_unless(log->prevLSN == 200, NULL);
  fail_unless(log->xid == 1,NULL);
  fail_unless(log->type == 4,NULL);
  fail_unless(sizeofLogEntry(log) == sizeof(struct __raw_log_entry),NULL);
  free(log);
}
END_TEST

START_TEST(clrLogEntryAlloc)
{
  recordid rid = { 3, 4, 5 };
  LogEntry * log = allocCLRLogEntry(200, 1, 7, rid, 8);
  fail_unless(log->LSN == -1, NULL);
  fail_unless(log->prevLSN == 200, NULL);
  fail_unless(log->xid == 1, NULL);
  fail_unless(log->type == CLRLOG, NULL);
  fail_unless(sizeofLogEntry(log) == sizeof(struct __raw_log_entry) + sizeof(CLRLogEntry), NULL);
  
  fail_unless(log->contents.clr.thisUpdateLSN == 7, NULL);
  fail_unless(log->contents.clr.rid.page == 3, NULL);
  fail_unless(log->contents.clr.rid.slot == 4, NULL);
  fail_unless(log->contents.clr.rid.size == 5, NULL);
  fail_unless(log->contents.clr.undoNextLSN == 8, NULL);

  free(log);

}
END_TEST

/** @test
    
    Quick test of allocUpdateLogEntry

    @todo  It would be nice if this test used actual operatations table instead of making up values.*/

START_TEST(updateLogEntryAlloc)
{
  
  int * preImageCpy;
  int preImage[] = {10000, 20000, 30000};
  char args[] = {'a', 'b', 'c'};
  recordid rid = { 3 , 4, sizeof(int)*3 };

  LogEntry * log;

  Tinit();  /* Needed because it sets up the operations table. */
  log = allocUpdateLogEntry(200, 1, OPERATION_SET,
				       rid, 
				       (const byte*)args, 3*sizeof(char), (const byte*)preImage);
  fail_unless(log->LSN == -1, NULL);
  fail_unless(log->prevLSN == 200, NULL);
  fail_unless(log->xid == 1, NULL);
  fail_unless(log->type == UPDATELOG, NULL);
  
  fail_unless(log->contents.update.funcID    == OPERATION_SET, NULL);
  /* fail_unless(log->contents.update.invertible == 0, NULL); */ 
  fail_unless(log->contents.update.rid.page   == 3, NULL);
  fail_unless(log->contents.update.rid.slot   == 4, NULL);
  fail_unless(log->contents.update.rid.size   == 3*sizeof(int), NULL);
  fail_unless(log->contents.update.argSize    == 3*sizeof(char), NULL);
  
  fail_unless(getUpdateArgs(log) != NULL, NULL);
  fail_unless(args[0] == ((char*)getUpdateArgs(log))[0], NULL);
  fail_unless(args[1] == ((char*)getUpdateArgs(log))[1], NULL);
  fail_unless(args[2] == ((char*)getUpdateArgs(log))[2], NULL);
  preImageCpy = (int*)getUpdatePreImage(log);
  fail_unless(preImageCpy != NULL, NULL);

  fail_unless(preImage[0] == preImageCpy[0], NULL);
  fail_unless(preImage[1] == preImageCpy[1], NULL);
  fail_unless(preImage[2] == preImageCpy[2], NULL);

  fail_unless(sizeofLogEntry(log) == (sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + 3 * (sizeof(int)+sizeof(char))), NULL);
  free(log);
  Tdeinit();
}
END_TEST


START_TEST(updateLogEntryAllocNoExtras)
{
  int * preImageCpy;
  int preImage[] = {10000, 20000, 30000};
  char args[] = {'a', 'b', 'c'};
  recordid rid = { 3 , 4, sizeof(int)*3 };

  LogEntry * log = allocUpdateLogEntry(200, 1, OPERATION_LHINSERT,
				       rid, 
				       (byte*)args, 0, (byte*)preImage);
  fail_unless(log->LSN == -1, NULL);
  fail_unless(log->prevLSN == 200, NULL);
  fail_unless(log->xid == 1, NULL);
  fail_unless(log->type == UPDATELOG, NULL);
  
  fail_unless(log->contents.update.funcID    == OPERATION_LHINSERT, NULL);
  /*  fail_unless(log->contents.update.invertible == 1, NULL); */
  fail_unless(log->contents.update.rid.page   == 3, NULL);
  fail_unless(log->contents.update.rid.slot   == 4, NULL);
  fail_unless(log->contents.update.rid.size   == 3*sizeof(int), NULL);
  fail_unless(log->contents.update.argSize    == 0, NULL);
  
  fail_unless(getUpdateArgs(log) == NULL, NULL);
  preImageCpy = (int*)getUpdatePreImage(log);
  fail_unless(preImageCpy == NULL, NULL);

  fail_unless(sizeofLogEntry(log) == (sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + 0 * (sizeof(int)+sizeof(char))), NULL);
  free(log);
}
END_TEST




Suite * check_suite(void) {
  Suite *s = suite_create("logEntry");
  /* Begin a new test */
  TCase *tc = tcase_create("allocate");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, rawLogEntryAlloc);
  tcase_add_test(tc, clrLogEntryAlloc);
  tcase_add_test(tc, updateLogEntryAlloc);
  tcase_add_test(tc, updateLogEntryAllocNoExtras);


  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
