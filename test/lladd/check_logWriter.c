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

#include <lladd/transactional.h>
/*#include <lladd/logger/logEntry.h> */
#include "../../src/lladd/logger/logHandle.h"
#include "../../src/lladd/logger/logWriter.h"

#include "../../src/lladd/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"



#define LOG_NAME   "check_logWriter.log"

static void setup_log() {
  int i;
  lsn_t prevLSN = -1;
  int xid = 100;

  Tinit();

  deleteLogWriter();
  openLogWriter();
  
  for(i = 0 ; i < 1000; i++) {
    LogEntry * e = allocCommonLogEntry(prevLSN, xid, XBEGIN);
    LogEntry * f;
    recordid rid;
    byte * args = (byte*)"Test 123.";
    long args_size = 10;  /* Including null */
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

/** 
    @test

    Build a simple log, truncate it, and then test the logWriter routines against it.
*/
START_TEST(logWriterTruncate) {
  LogEntry * le;
  LogEntry * le2;
  LogEntry * le3 = NULL;
  LogEntry * tmp;

  LogHandle lh = getLogHandle();
  int i = 0;
  setup_log();

  while(i < 234) {
    i++;
    le = nextInLog(&lh);
  }
 
  le2 = nextInLog(&lh);
  i = 0;
  while(i < 23) {
    i++;
    le3 = nextInLog(&lh);
  }
  

  truncateLog(le->LSN);
  
  tmp = readLSNEntry(le->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le->LSN, NULL);
  
  tmp = readLSNEntry(le2->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le2->LSN, NULL);

  tmp = readLSNEntry(le3->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le3->LSN, NULL);


  lh = getLogHandle();
  
  i = 0;

  while((le = nextInLog(&lh))) {
    i++;
  }


  fail_unless(i == (3000 - 234 + 1), NULL);
  

} END_TEST

#define ENTRIES_PER_THREAD 1000

pthread_mutex_t random_mutex;

static void* worker_thread(void * arg) {
  int key = *(int*)arg;
  int i = 0;
  int truncated_to = 4;

  LogEntry * le = allocCommonLogEntry(-1, -1, XBEGIN);

  int lsns[ENTRIES_PER_THREAD];


  /*  fail_unless(NULL != le, NULL); */

  while(i < ENTRIES_PER_THREAD) {
    int threshold;
    int entry;

    pthread_mutex_lock(&random_mutex);

    threshold = (int) (2000.0*random()/(RAND_MAX+1.0));
    entry = (int) (ENTRIES_PER_THREAD*random()/(RAND_MAX+1.0));

    pthread_mutex_unlock(&random_mutex);

    /*    fail_unless(threshold <= 100, NULL); */

    if(threshold < 3) {
      if(i > 10) {
	/* Truncate the log .15% of the time; result in a bit over 100 truncates per test run.*/
	/*	fail_unless(1, NULL); */

	truncateLog(lsns[i - 10]);
	truncated_to = i - 10;
      } 
      /*      fail_unless(1, NULL); */
    } else {

      /*      DEBUG("i = %d, le = %x\n", i, (unsigned int)le); */
      /*      fail_unless(1, NULL); */
      le->xid = i+key;
      writeLogEntry(le);
      lsns[i] = le->LSN;
      i++;
    }
    /*    fail_unless(1, NULL); */
    if(entry > truncated_to && entry < i) {
      assert(readLSNEntry(lsns[entry])->xid == entry+key);
      /*      fail_unless(readLSNEntry(lsns[entry])->xid == entry+key, NULL); */
    }
    /*    fail_unless(1, NULL); */
	
    /* Try to interleave requests as much as possible */
    /*pthread_yield(); */
    sched_yield();

  }

  free(le);

  return 0;
}

START_TEST(logWriterCheckWorker) {
  int four = 4;

  pthread_mutex_init(&random_mutex, NULL);

  Tinit();
  worker_thread(&four);
  Tdeinit();

} END_TEST

START_TEST(logWriterCheckThreaded) {

#define  THREAD_COUNT 50
  pthread_t workers[THREAD_COUNT];
  int i;
  pthread_mutex_init(&random_mutex, NULL);

  Tinit();

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, worker_thread, &i);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }
  Tdeinit();

} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, logWriterTest);
  tcase_add_test(tc, logHandleColdReverseIterator);
  tcase_add_test(tc, logWriterTruncate);
  tcase_add_test(tc, logWriterCheckWorker);
  tcase_add_test(tc, logWriterCheckThreaded);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
