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

/** 
    @file check_logWriter
    
    Tests logWriter.  

    @todo Get rid of include for logWriter.h (stop calling deleteLogWriter, syncLog_logWriter...)
*/
#include <config.h>
#include <check.h>

#include <lladd/transactional.h>
#include "../../src/lladd/logger/logHandle.h"
#include <lladd/logger/logger2.h>
#include "../../src/lladd/logger/logWriter.h"

#include "../../src/lladd/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"
#include <lladd/truncation.h>

extern int numActiveXactions;

#define LOG_NAME   "check_logWriter.log"

//static int logType = LOG_TO_MEMORY;

static void setup_log() {
  int i;
  lsn_t prevLSN = -1;
  int xid = 42;
  deleteLogWriter();
  lladd_enableAutoTruncation = 0;
  Tinit();
  lsn_t firstLSN = -1;
  int  first = 1;

  for(i = 0 ; i < 1000; i++) {
    LogEntry * e = allocCommonLogEntry(prevLSN, xid, XBEGIN);
    const LogEntry * f;
    recordid rid;
    byte * args = (byte*)"Test 123.";
    long args_size = 10;  /* Including null */
    unsigned long preImage = 42;

    rid.page = 0;
    rid.slot = 0;
    rid.size = sizeof(unsigned long);

    LogWrite(e);
    prevLSN = e->LSN;

    if(first) { 
      first = 0;
      firstLSN = prevLSN;
    }

    f = LogReadLSN(prevLSN);

    fail_unless(sizeofLogEntry(e) == sizeofLogEntry(f), "Log entry changed size!!");
    fail_unless(0 == memcmp(e,f,sizeofLogEntry(e)), "Log entries did not agree!!");

    FreeLogEntry (e);
    FreeLogEntry (f);

    e = allocUpdateLogEntry(prevLSN, xid, 1, rid, args, args_size, (byte*) &preImage);

    LogWrite(e);
    prevLSN = e->prevLSN;

    //    LogEntry * g = allocCLRLogEntry(100, 1, 200, rid, 0); //prevLSN);
    LogEntry * g = allocCLRLogEntry(e); // XXX will probably break
    g->prevLSN = firstLSN;
    LogWrite(g);
    assert (g->type == CLRLOG);
    prevLSN = g->LSN; 
    
    FreeLogEntry (e);
    FreeLogEntry (g);
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
   LogReadLSN, which tests the logWriter's ability to succesfully
   manipulate LSN's.

   @todo Test logHandle more thoroughly. (Still need to test the guard mechanism.)

*/
START_TEST(loggerTest)
{
  const LogEntry * e;
  LogHandle h;
  int i = 0;

  setup_log();
  h = getLogHandle();

  while((e = nextInLog(&h))) {
    FreeLogEntry(e);
    i++;
    assert(i < 4000);
  }

  assert(i == 3000);

  deleteLogWriter();
  Tdeinit();
}
END_TEST

/** 
    @test
    Checks for a bug ecountered during devlopment.  What happens when
    previousInTransaction is called immediately after the handle is
    allocated? */

START_TEST(logHandleColdReverseIterator) {
  const LogEntry * e;
  setup_log();
  LogHandle lh = getLogHandle();
  int i = 0;


  while(((e = nextInLog(&lh)) && (i < 100)) ) {
    FreeLogEntry(e);
    i++;
  }
  
  i = 0;
  lh = getLSNHandle(e->LSN);
  while((e = previousInTransaction(&lh))) {
    i++;
    FreeLogEntry(e);
  }
  assert(i <= 4); /* We should almost immediately hit a clr that goes to the beginning of the log... */
  Tdeinit();
}
END_TEST

/** 
    @test

    Build a simple log, truncate it, and then test the logWriter routines against it.
*/
START_TEST(loggerTruncate) {
  const LogEntry * le;
  const LogEntry * le2;
  const LogEntry * le3 = NULL;
  const LogEntry * tmp;
  setup_log();

  LogHandle lh = getLogHandle();
  int i = 0;

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
  
  LogTruncate(le->LSN);
  
  tmp = LogReadLSN(le->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le->LSN, NULL);
  
  FreeLogEntry(tmp);
  tmp = LogReadLSN(le2->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le2->LSN, NULL);

  FreeLogEntry(tmp);
  tmp = LogReadLSN(le3->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le3->LSN, NULL);
  
  FreeLogEntry(tmp);

  lh = getLogHandle();
  
  i = 0;
  
  FreeLogEntry(le);
  FreeLogEntry(le2);
  FreeLogEntry(le3);

  while((le = nextInLog(&lh))) {
    if(le->type != INTERNALLOG) { 
      i++;
    }
    FreeLogEntry(le);
  }
  assert(i == (3000 - 234 + 1));

  Tdeinit();

} END_TEST

#define ENTRIES_PER_THREAD 200

pthread_mutex_t random_mutex;

lsn_t truncated_to = 4;

#undef NO_CONCURRENCY
#ifdef NO_CONCURRENCY
pthread_mutex_t big = PTHREAD_MUTEX_INITIALIZER;
#endif
static void* worker_thread(void * arg) {
  long key = *(int*)arg;
  long i = 0;

  lsn_t lsns[ENTRIES_PER_THREAD];

  for(i = 0; i < ENTRIES_PER_THREAD; i++) { 
    lsns[i] = 0;
  }
  i = 0;

  while(i < ENTRIES_PER_THREAD) {
    LogEntry * le = allocCommonLogEntry(-1, -1, XBEGIN);
    int threshold;
    long entry;
    int needToTruncate = 0;
    lsn_t myTruncVal = 0;
    pthread_mutex_lock(&random_mutex);

    threshold = (int) (2000.0*random()/(RAND_MAX+1.0));
    entry = (long) (ENTRIES_PER_THREAD*random()/(RAND_MAX+1.0));

    if(threshold < 3) { 
      if(i > 10) {
	needToTruncate = 1;
	if(lsns[i - 10] > truncated_to) {
	  truncated_to = lsns[i - 10];
	  myTruncVal = truncated_to;
	}
      }
    }

    pthread_mutex_unlock(&random_mutex);

    if(needToTruncate) { 
#ifdef NO_CONCURRENCY      
      pthread_mutex_lock(&big);
#endif
      LogTruncate(myTruncVal);
#ifdef NO_CONCURRENCY      
      pthread_mutex_unlock(&big);
#endif      
      assert(LogTruncationPoint() >= myTruncVal);
    }

    if(threshold < 3) {
    } else {
      le->xid = i+key;
#ifdef NO_CONCURRENCY      
      pthread_mutex_lock(&big);
#endif
      LogWrite(le);
#ifdef NO_CONCURRENCY      
      pthread_mutex_unlock(&big);
#endif
      lsns[i] = le->LSN;
      i++;
    }
    pthread_mutex_lock(&random_mutex);
#ifdef NO_CONCURRENCY      
    pthread_mutex_lock(&big);
#endif
    if(lsns[entry] > truncated_to && entry < i) {
      lsn_t lsn = lsns[entry];
      pthread_mutex_unlock(&random_mutex);

      const LogEntry * e = LogReadLSN(lsn);

      assert(e->xid == entry+key);
      FreeLogEntry(e);
    } else { 
      pthread_mutex_unlock(&random_mutex);
    }
#ifdef NO_CONCURRENCY      
    pthread_mutex_unlock(&big);
#endif
	
    /* Try to interleave requests as much as possible */
    sched_yield();
    FreeLogEntry(le);
  }



  return 0;
}

START_TEST(loggerCheckWorker) {
  int four = 4;

  pthread_mutex_init(&random_mutex, NULL);

  Tinit();
  worker_thread(&four);
  Tdeinit();

} END_TEST

START_TEST(loggerCheckThreaded) {

#define  THREAD_COUNT 100
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

void reopenLogWorkload(int truncating) { 

  lladd_enableAutoTruncation = 0;
  
  const int ENTRY_COUNT = 1000;
  const int SYNC_POINT = 900;
  lladd_enableAutoTruncation = 0;

  numActiveXactions = 0;

  LogInit(loggerType);
  int xid = 1;
  TransactionLog l = LogTransBegin(xid);
  lsn_t startLSN = 0;

  LogEntry * entries[ENTRY_COUNT];

  for(int i = 0; i < ENTRY_COUNT; i++) {

    entries[i] = LogUpdate(&l, NULL, NULLRID, OPERATION_NOOP, NULL); 

    if(i == SYNC_POINT) {
      if(truncating) { 
	LogTruncate(entries[i]->LSN);
	startLSN = entries[i]->LSN;
      }
    }
  }

  LogDeinit();
  LogInit(loggerType);

  LogHandle h;
  int i;

  if(truncating) { 
    h = getLogHandle();
    i = SYNC_POINT;
  } else { 
    h = getLogHandle();
    i = 0;
  } 

  const LogEntry * e;
  while((e = nextInLog(&h))) { 
    if(e->type != INTERNALLOG) { 
      assert(sizeofLogEntry(e) == sizeofLogEntry(entries[i]));
      assert(!memcmp(e, entries[i], sizeofLogEntry(entries[i])));
      assert(i < ENTRY_COUNT);
      i++;
    }
  }
  
  assert(i == (ENTRY_COUNT));

  LogEntry * entries2[ENTRY_COUNT];
  for(int i = 0; i < ENTRY_COUNT; i++) {
    entries2[i] = LogUpdate(&l, NULL, NULLRID, OPERATION_NOOP, NULL); 
    if(i == SYNC_POINT) { 
      syncLog_LogWriter();
    }
  }


  if(truncating) { 
    h = getLSNHandle(startLSN);
    i = SYNC_POINT;
  } else { 
    h = getLogHandle();
    i = 0;
  } 

  while((e = nextInLog(&h))) { 
    if(e->type != INTERNALLOG) { 
      if( i < ENTRY_COUNT) { 
	assert(sizeofLogEntry(e) == sizeofLogEntry(entries[i]));
	assert(!memcmp(e, entries[i], sizeofLogEntry(entries[i])));
      } else { 
	assert(i < ENTRY_COUNT * 2);
	assert(sizeofLogEntry(e) == sizeofLogEntry(entries2[i-ENTRY_COUNT]));
	assert(!memcmp(e, entries2[i-ENTRY_COUNT], sizeofLogEntry(entries2[i-ENTRY_COUNT])));
      }
      i++;
    }
  }

  assert(i == (ENTRY_COUNT * 2));  

  lladd_enableAutoTruncation = 1;
  LogDeinit();
}

START_TEST(loggerReopenTest) {
  deleteLogWriter();  
  reopenLogWorkload(0);

} END_TEST

START_TEST(loggerTruncateReopenTest) { 
  deleteLogWriter();
  reopenLogWorkload(1);
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");
  tcase_set_timeout(tc, 0);
  /* Sub tests are added, one per line, here */
  
  tcase_add_test(tc, loggerTest);
  tcase_add_test(tc, logHandleColdReverseIterator);
  tcase_add_test(tc, loggerTruncate);
  tcase_add_test(tc, loggerCheckWorker);
  tcase_add_test(tc, loggerCheckThreaded);
  if(loggerType != LOG_TO_MEMORY) {
    tcase_add_test(tc, loggerReopenTest);
    tcase_add_test(tc, loggerTruncateReopenTest);
  }

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
