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

#include <lladd/logger/logEntry.h>
#include <lladd/logger/logHandle.h>
#include <lladd/transactional.h>

#define LOG_NAME   "check_logWriter.log"

static void setup_log() {
  int i;
  lsn_t prevLSN = -1;
  int xid = 100;
  deleteLogWriter();
  openLogWriter();
  
  for(i = 0 ; i < 1000; i++) {
    LogEntry * e = allocCommonLogEntry(prevLSN, xid, XBEGIN);
    LogEntry * f;
    recordid rid;
    byte * args = (byte*)"Test 123.";
    size_t args_size = 10;  /* Including null */
    unsigned long preImage = 42;

    rid.page = 0;
    rid.slot = 0;
    rid.size = sizeof(unsigned long);

    writeLogEntry(e);
    prevLSN = e->LSN;
    
    f = readLSNEntry(prevLSN);
    fail_unless(sizeofLogEntry(e) == sizeofLogEntry(f), "Log entry changed size!!");
    fail_unless(0 == memcmp(e,f,sizeofLogEntry(e)), "Log entries did not agree!!");

    free (e);
    free (f);

    e = allocUpdateLogEntry(prevLSN, xid, 1, rid, args, args_size, (byte*) &preImage);
    writeLogEntry(e);
    prevLSN = e->prevLSN;
    f = allocCLRLogEntry(100, 1, 200, rid, prevLSN);

    prevLSN = f->prevLSN; 
    
    writeLogEntry(f);
    free (e);
    free (f);
  }
}
/**
   @test 

   Quick test of log writer and log handler.  Not very extensive.
   Just writes out 3000 log entries, checks that 1000 of them make
   sense, and then closes, opens and iterates over the resulting log
   file to make sure that it contains 3000 entries, and none of its
   builtin assertions fail.

   In particular, logWriter checks to make sure that each log entry's
   size matches the size that it recorded before the logEntry.  Also,
   when checking the 1000 of 3000 entries, this test uses
   readLSNEntry, which tests the logWriter's ability to succesfully
   manipulate LSN's.

   @todo Test logHandle more thoroughly. (Still need to test the guard mechanism.)

*/
START_TEST(logWriterTest)
{
  LogEntry * e;
  LogHandle h;
  int i = 0;
  setup_log();
  syncLog();
  closeLogWriter();

  openLogWriter();

  
  h = getLogHandle();
  /*  readLSNEntry(sizeof(lsn_t)); */

  while((e = nextInLog(&h))) {
    i++;
  }


  fail_unless(i = 3000, "Wrong number of log entries!");

  deleteLogWriter();


}
END_TEST

/** 
    @test
    Checks for a bug ecountered during devlopment.  What happens when
    previousInTransaction is called immediately after the handle is
    allocated? */

START_TEST(logHandleColdReverseIterator) {
  LogEntry * e;
  LogHandle lh = getLogHandle();
  int i = 0;
  setup_log();


  while(((e = nextInLog(&lh)) && (i < 100)) ) {
    i++;
  }
  
  i = 0;
  lh = getLogHandle(e->LSN);

  while((e = previousInTransaction(&lh))) {
    i++;
  }
  /*  printf("i = %d\n", i); */
  fail_unless( i == 1 , NULL);  /* The 1 is because we immediately hit a clr that goes to the beginning of the log... */

 deleteLogWriter();

}
END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, logWriterTest);
  tcase_add_test(tc, logHandleColdReverseIterator);

  /* --------------------------------------------- */

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
