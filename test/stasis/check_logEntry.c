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
#include "../check_includes.h"

#include <stasis/transactional.h>

#include <assert.h>

#define LOG_NAME   "check_logEntry.log"

START_TEST(rawLogEntryAlloc)
{
  Tinit();
  stasis_log_t *l = (stasis_log_t *)stasis_log();
  LogEntry * log = allocCommonLogEntry(l, 200, 1, XABORT);
  assert(log->prevLSN == 200);
  assert(log->xid == 1);
  assert(log->type == XABORT);
  assert(sizeofLogEntry(0, log) == sizeof(struct __raw_log_entry));

  l->write_entry(l, log);
  l->write_entry_done(l, log);
  Tdeinit();
}
END_TEST

/** @test

    Quick test of allocUpdateLogEntry

    @todo  It would be nice if this test used actual operatations table instead of making up values.*/

START_TEST(updateLogEntryAlloc)
{

  char args[] = {'a', 'b', 'c'};
  recordid rid = { 3 , 4, sizeof(int)*3 };

  LogEntry * log;

  Tinit();  /* Needed because it sets up the operations table. */
  stasis_log_t *l = (stasis_log_t *)stasis_log();

  log = allocUpdateLogEntry(l, 200, 1, OPERATION_SET,
                            rid.page, 3*sizeof(char));
  memcpy(stasis_log_entry_update_args_ptr(log), args, 3*sizeof(char));
  assert(log->prevLSN == 200);
  assert(log->xid == 1);
  assert(log->type == UPDATELOG);

  assert(log->update.funcID    == OPERATION_SET);
  assert(log->update.page   == 3);
  assert(log->update.arg_size   == 3*sizeof(char));

  assert(stasis_log_entry_update_args_ptr(log) != NULL);
  assert(args[0] == ((char*)stasis_log_entry_update_args_ptr(log))[0]);
  assert(args[1] == ((char*)stasis_log_entry_update_args_ptr(log))[1]);
  assert(args[2] == ((char*)stasis_log_entry_update_args_ptr(log))[2]);

  //  printf("sizes %d %d\n",sizeofLogEntry(log),(sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + (sizeof(char))));

  assert(sizeofLogEntry(0, log) == (sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + 3 * (sizeof(char))));

  l->write_entry(l, log);
  l->write_entry_done(l, log);
  Tdeinit();
}
END_TEST


START_TEST(updateLogEntryAllocNoExtras)
{
  Tinit();

  recordid rid = { 3 , 4, sizeof(int)*3 };

  stasis_log_t *l = (stasis_log_t *)stasis_log();
  LogEntry * log = allocUpdateLogEntry(l, 200, 1, OPERATION_SET,
                                       rid.page, 0);
  assert(log->prevLSN == 200);
  assert(log->xid == 1);
  assert(log->type == UPDATELOG);

  assert(log->update.funcID    == OPERATION_SET);
  assert(log->update.page   == 3);
  assert(log->update.arg_size    == 0);

  assert(stasis_log_entry_update_args_ptr(log) == NULL);

  assert(sizeofLogEntry(0, log) == (sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + 0 * (sizeof(int)+sizeof(char))));

  l->write_entry(l, log);
  l->write_entry_done(l, log);

  Tdeinit();
}
END_TEST




Suite * check_suite(void) {
  Suite *s = suite_create("logEntry");
  /* Begin a new test */
  TCase *tc = tcase_create("allocate");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, rawLogEntryAlloc);
  tcase_add_test(tc, updateLogEntryAlloc);
  tcase_add_test(tc, updateLogEntryAllocNoExtras);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
