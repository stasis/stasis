/*
 * check_filePool.c
 *
 *  Created on: Apr 15, 2011
 *      Author: sears
 */

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

#include <stasis/logger/filePool.h>
#include <stasis/flags.h>
#include <assert.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_filePool.log"

/**
   @test
*/
START_TEST(filePoolDirTest){
  stasis_log_t * log = stasis_log_file_pool_open(
      stasis_log_dir_name,
      stasis_log_file_mode,
      stasis_log_file_permissions);

  const int rec_len = 950;
  int rec_count = 100;

  lsn_t last_lsn = -1;
  for(int i = 0; i < rec_count * rec_len; i++) {
    LogEntry * e = log->reserve_entry(log, rec_len);
    e->type = UPDATELOG;
    e->update.arg_size = rec_len- sizeof(struct __raw_log_entry) - sizeof(UpdateLogEntry);
    last_lsn = e->LSN;
    log->write_entry_done(log, e);
    if(!(i & 15)) { log->force_tail(log, 0); } // xxx
  }

  log->close(log);

  log = stasis_log_file_pool_open(
      stasis_log_dir_name,
      stasis_log_file_mode,
      stasis_log_file_permissions
      );

  lsn_t eol = log->next_available_lsn(log);

  assert(eol == last_lsn + rec_len + 2 * sizeof(uint32_t));

  lsn_t last_l = INVALID_LSN, l = log->truncation_point(log);
  const LogEntry * e;
  lsn_t truncated = 0;
  int i = 0;

  while((e = log->read_entry(log, l))) {
    last_l = e->LSN;
    l = log->next_entry(log, e);
    i++;
    if(!truncated && i > (rec_count * rec_len * 9) / 10) {
      truncated = e->LSN;
      printf("truncating to %lld\n", (long long)truncated);
      log->truncate(log, truncated);
      assert(log->truncation_point(log) > 0 && log->truncation_point(log) <= truncated);
      i = 1;
    }
    log->read_entry_done(log, e);
  }
  assert(last_l == last_lsn);

  int j = 0;
  last_l = INVALID_LSN;
  l = truncated;
  while((e = log->read_entry(log, l))) {
    last_l = e->LSN;
    l = log->next_entry(log, e);
    j++;
    log->read_entry_done(log, e);
  }
  assert(i == j);
  assert(last_l == last_lsn);

  log->close(log);
} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("filePool");
  /* Begin a new test */
  TCase *tc = tcase_create("filePool");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, filePoolDirTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
