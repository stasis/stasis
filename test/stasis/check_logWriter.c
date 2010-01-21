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
#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/logger/logHandle.h>
#include <stasis/logger/logger2.h>
#include <stasis/logger/safeWrites.h>
#include <stasis/logger/inMemoryLog.h>
#include <stasis/truncation.h>
#include <stasis/latches.h>

#include <sched.h>
#include <assert.h>

#define LOG_NAME   "check_logWriter.log"

LogEntry * dupLogEntry(stasis_log_t * log, const LogEntry *e) {
  LogEntry * ret = malloc(sizeofLogEntry(log, e));
  memcpy(ret,e,sizeofLogEntry(log, e));
  return ret;
}

static stasis_log_t * setup_log() {
  int i;
  lsn_t prevLSN = -1;
  int xid = 42;
  stasis_log_safe_writes_delete(stasis_log_file_name);
  stasis_truncation_automatic = 0;
  Tinit();
  lsn_t firstLSN = -1;
  int  first = 1;
  stasis_log_t * stasis_log_file = stasis_log();
  for(i = 0 ; i < 1000; i++) {
    lsn_t test = stasis_log_file->next_available_lsn(stasis_log_file);

    LogEntry * e = allocCommonLogEntry(stasis_log_file, prevLSN, xid, XBEGIN);
    const LogEntry * f;
    recordid rid;
    byte * args = (byte*)"Test 123.";
    long args_size = 10;  /* Including null */

    rid.page = 0;
    rid.slot = 0;
    rid.size = sizeof(unsigned long);

    stasis_log_file->write_entry(stasis_log_file,e);
    prevLSN = e->LSN;

    LogEntry * tmp = dupLogEntry(stasis_log_file,e);
    stasis_log_file->write_entry_done(stasis_log_file, e);
    e = tmp;

    assert(test <= e->LSN);

    if(first) {
      first = 0;
      firstLSN = prevLSN;
    }

    f = stasis_log_file->read_entry(stasis_log_file, prevLSN);

    fail_unless(sizeofLogEntry(0, e) == sizeofLogEntry(0, f), "Log entry changed size!!");
    fail_unless(0 == memcmp(e,f,sizeofLogEntry(0, e)), "Log entries did not agree!!");
    free(e);
//    stasis_log_file->write_entry_done(stasis_log_file, e);
    stasis_log_file->read_entry_done(stasis_log_file, f);

    e = allocUpdateLogEntry(stasis_log_file, prevLSN, xid, 1, rid.page, args_size);
    memcpy(stasis_log_entry_update_args_ptr(e), args, args_size);
    stasis_log_file->write_entry(stasis_log_file,e);
    prevLSN = e->prevLSN;

    tmp = dupLogEntry(stasis_log_file,e);
    stasis_log_file->write_entry_done(stasis_log_file, e);
    e = tmp;

    //    LogEntry * g = allocCLRLogEntry(100, 1, 200, rid, 0); //prevLSN);
    LogEntry * g = allocCLRLogEntry(stasis_log_file, e); // XXX will probably break
    g->prevLSN = firstLSN;
    stasis_log_file->write_entry(stasis_log_file,g);
    assert (g->type == CLRLOG);
    prevLSN = g->LSN;

    free(e);
    stasis_log_file->write_entry_done(stasis_log_file, g);
  }
  return stasis_log_file;
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
   log->read_entry, which tests the logWriter's ability to succesfully
   manipulate LSN's.

   @todo Test logHandle more thoroughly. (Still need to test the guard mechanism.)

*/
static void loggerTest(int logType) {

  stasis_log_type = logType;
  const LogEntry * e;
  LogHandle* h;
  int i = 0;
  stasis_log_t * stasis_log_file = setup_log();
  h = getLogHandle(stasis_log_file);

  while((e = nextInLog(h))) {
    i++;
    assert(i < 4000);
  }

  freeLogHandle(h);

  assert(i == 3000);

  stasis_log_safe_writes_delete(stasis_log_file_name);
  Tdeinit();
}
START_TEST(loggerFileTest) {
	loggerTest(LOG_TO_FILE);
} END_TEST
START_TEST(loggerMemTest) {
	loggerTest(LOG_TO_MEMORY);
} END_TEST
/**
    @test
    Checks for a bug ecountered during devlopment.  What happens when
    previousInTransaction is called immediately after the handle is
    allocated? */

static void logHandleColdReverseIterator(int logType) {
  const LogEntry * e;
  stasis_log_type = logType;
  stasis_log_t * stasis_log_file = setup_log();
  LogHandle* lh = getLogHandle(stasis_log_file);
  int i = 0;


  while(((e = nextInLog(lh)) && (i < 100)) ) {
    i++;
  }

  lsn_t lsn = e->LSN;

  freeLogHandle(lh);

  i = 0;
  lh = getLSNHandle(stasis_log_file, lsn);
  while((e = previousInTransaction(lh))) {
    i++;
  }
  freeLogHandle(lh);
  assert(i <= 4); /* We should almost immediately hit a clr that goes to the beginning of the log... */
  Tdeinit();
}
START_TEST(logHandleFileColdReverseIterator) {
	logHandleColdReverseIterator(LOG_TO_FILE);
} END_TEST
START_TEST(logHandleMemColdReverseIterator) {
	logHandleColdReverseIterator(LOG_TO_MEMORY);
} END_TEST

/**
    @test

    Build a simple log, truncate it, and then test the logWriter routines against it.
*/
static void loggerTruncate(int logType) {
  stasis_log_type = logType;
  const LogEntry * le;
  const LogEntry * le2;
  const LogEntry * le3 = NULL;
  const LogEntry * tmp;

  stasis_log_t * stasis_log_file = setup_log();

  LogHandle* lh = getLogHandle(stasis_log_file);
  int i = 0;

  while(i < 234) {
    i++;
    le = nextInLog(lh);
  }

  LogEntry * copy = malloc(sizeofLogEntry(stasis_log_file, le));
  memcpy(copy, le, sizeofLogEntry(stasis_log_file, le));
  le = copy;

  le2 = nextInLog(lh);

  copy = malloc(sizeofLogEntry(stasis_log_file, le2));
  memcpy(copy, le2, sizeofLogEntry(stasis_log_file, le2));
  le2 = copy;

  i = 0;
  while(i < 23) {
    i++;
    le3 = nextInLog(lh);
  }

  copy = malloc(sizeofLogEntry(stasis_log_file, le3));
  memcpy(copy, le3, sizeofLogEntry(stasis_log_file, le3));
  le3 = copy;

  stasis_log_file->truncate(stasis_log_file, le->LSN);

  tmp = stasis_log_file->read_entry(stasis_log_file, le->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le->LSN, NULL);

  stasis_log_file->read_entry_done(stasis_log_file, tmp);
  tmp = stasis_log_file->read_entry(stasis_log_file, le2->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le2->LSN, NULL);

  stasis_log_file->read_entry_done(stasis_log_file, tmp);
  tmp = stasis_log_file->read_entry(stasis_log_file, le3->LSN);

  fail_unless(NULL != tmp, NULL);
  fail_unless(tmp->LSN == le3->LSN, NULL);

  stasis_log_file->read_entry_done(stasis_log_file, tmp);
  freeLogHandle(lh);
  lh = getLogHandle(stasis_log_file);

  i = 0;

  free((void*)le);
  free((void*)le2);
  free((void*)le3);

  while((le = nextInLog(lh))) {
    if(le->type != INTERNALLOG) {
      i++;
    }
  }
  assert(i == (3000 - 234 + 1));
  freeLogHandle(lh);
  Tdeinit();
}
START_TEST(loggerFileTruncate) {
	loggerTruncate(LOG_TO_FILE);
} END_TEST
START_TEST(loggerMemTruncate) {
	loggerTruncate(LOG_TO_MEMORY);
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
  stasis_log_t * stasis_log_file = stasis_log();

  while(i < ENTRIES_PER_THREAD) {
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
      stasis_log_file->truncate(stasis_log_file, myTruncVal);
#ifdef NO_CONCURRENCY
      pthread_mutex_unlock(&big);
#endif
      assert(stasis_log_file->truncation_point(stasis_log_file) >= myTruncVal);
    }

    if(threshold < 3) {
    } else {
      LogEntry * le = allocCommonLogEntry(stasis_log_file, -1, -1, XBEGIN);
      le->xid = i+key;
#ifdef NO_CONCURRENCY
      pthread_mutex_lock(&big);
#endif
      stasis_log_file->write_entry(stasis_log_file,le);
#ifdef NO_CONCURRENCY
      pthread_mutex_unlock(&big);
#endif
      lsns[i] = le->LSN;
      i++;
      stasis_log_file->write_entry_done(stasis_log_file,le);
    }
    pthread_mutex_lock(&random_mutex);
#ifdef NO_CONCURRENCY
    pthread_mutex_lock(&big);
#endif
    if(lsns[entry] > truncated_to && entry < i) {
      lsn_t lsn = lsns[entry];
      pthread_mutex_unlock(&random_mutex);

      const LogEntry * e = stasis_log_file->read_entry(stasis_log_file, lsn);
      if(e == NULL) {
        pthread_mutex_lock(&random_mutex);
        assert(lsn < truncated_to);
        pthread_mutex_unlock(&random_mutex);
      } else {
        assert(e->xid == entry+key);
        stasis_log_file->read_entry_done(stasis_log_file, e);
      }
    } else {
      pthread_mutex_unlock(&random_mutex);
    }
#ifdef NO_CONCURRENCY
    pthread_mutex_unlock(&big);
#endif

    /* Try to interleave requests as much as possible */
    sched_yield();
//    stasis_log_file->write_entry_done(stasis_log_file, le);
  }



  return 0;
}
static void loggerCheckWorker(int logType) {
  stasis_log_type = logType;
  int four = 4;

  pthread_mutex_init(&random_mutex, NULL);

  Tinit();
  worker_thread(&four);
  Tdeinit();

}
START_TEST(loggerFileCheckWorker) {
  loggerCheckWorker(LOG_TO_FILE);
} END_TEST
START_TEST(loggerMemCheckWorker) {
  loggerCheckWorker(LOG_TO_MEMORY);
} END_TEST

static void loggerCheckThreaded(int logType) {
  stasis_log_type = logType;

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

}

START_TEST(loggerFileCheckThreaded) {
  loggerCheckThreaded(LOG_TO_FILE);
} END_TEST
START_TEST(loggerMemCheckThreaded) {
  loggerCheckThreaded(LOG_TO_MEMORY);
} END_TEST

void reopenLogWorkload(int truncating) {
  stasis_operation_table_init();
  stasis_truncation_automatic = 0;

  const int ENTRY_COUNT = 1000;
  const int SYNC_POINT = 900;
  stasis_log_t * stasis_log_file = 0;

  if(LOG_TO_FILE == stasis_log_type) {
    stasis_log_file = stasis_log_safe_writes_open(stasis_log_file_name,
                                                  stasis_log_file_mode,
                                                  stasis_log_file_permissions,
                                                  stasis_log_softcommit);
  } else if(LOG_TO_MEMORY == stasis_log_type) {
    stasis_log_file = stasis_log_impl_in_memory_open();
  } else {
    assert(stasis_log_file != NULL);
  }

  int xid = 1;
  stasis_transaction_table_entry_t l;
//  pthread_mutex_init(&l.mut,0);
  stasis_log_begin_transaction(stasis_log_file, xid, &l);
  lsn_t startLSN = 0;

  LogEntry * entries[ENTRY_COUNT];

  for(int i = 0; i < ENTRY_COUNT; i++) {

    entries[i] = stasis_log_write_update(stasis_log_file,
                           &l, 0, OPERATION_NOOP, NULL, 0);

    LogEntry * e = dupLogEntry(stasis_log_file, entries[i]);
    stasis_log_file->write_entry_done(stasis_log_file, entries[i]);
    entries[i] = e;

    if(i == SYNC_POINT) {
      if(truncating) {
	stasis_log_file->truncate(stasis_log_file,entries[i]->LSN);
	startLSN = entries[i]->LSN;
      }
    }
  }

  stasis_log_file->close(stasis_log_file);

  if(LOG_TO_FILE == stasis_log_type) {
    stasis_log_file = stasis_log_safe_writes_open(stasis_log_file_name,
                                                  stasis_log_file_mode,
                                                  stasis_log_file_permissions,
                                                  stasis_log_softcommit);
  } else if(LOG_TO_MEMORY == stasis_log_type) {
    stasis_log_file = stasis_log_impl_in_memory_open();
  } else {
    assert(stasis_log_file != NULL);
  }

  LogHandle * h;
  int i;

  if(truncating) {
    h = getLogHandle(stasis_log_file);
    i = SYNC_POINT;
  } else {
    h = getLogHandle(stasis_log_file);
    i = 0;
  }

  const LogEntry * e;
  while((e = nextInLog(h))) {
    if(e->type != INTERNALLOG) {
      assert(sizeofLogEntry(0, e) == sizeofLogEntry(0, entries[i]));
      assert(!memcmp(e, entries[i], sizeofLogEntry(0, entries[i])));
      assert(i < ENTRY_COUNT);
      i++;
    }
  }

  assert(i == (ENTRY_COUNT));

  LogEntry * entries2[ENTRY_COUNT];
  for(int i = 0; i < ENTRY_COUNT; i++) {
    entries2[i] = stasis_log_write_update(stasis_log_file, &l, 0, OPERATION_NOOP,
                            NULL, 0);
    LogEntry * e = dupLogEntry(stasis_log_file, entries2[i]);
    stasis_log_file->write_entry_done(stasis_log_file, entries2[i]);
    entries2[i] = e;

    if(i == SYNC_POINT) {
      stasis_log_file->force_tail(stasis_log_file, LOG_FORCE_COMMIT);
    }
  }

  freeLogHandle(h);

  if(truncating) {
    h = getLSNHandle(stasis_log_file, startLSN);
    i = SYNC_POINT;
  } else {
    h = getLogHandle(stasis_log_file);
    i = 0;
  }

  while((e = nextInLog(h))) {
    if(e->type != INTERNALLOG) {
      if( i < ENTRY_COUNT) {
        assert(sizeofLogEntry(0, e) == sizeofLogEntry(0, entries[i]));
        assert(!memcmp(e, entries[i], sizeofLogEntry(0, entries[i])));
      } else {
        assert(i < ENTRY_COUNT * 2);
        assert(sizeofLogEntry(0, e) == sizeofLogEntry(0, entries2[i-ENTRY_COUNT]));
        assert(!memcmp(e, entries2[i-ENTRY_COUNT], sizeofLogEntry(0, entries2[i-ENTRY_COUNT])));
      }
      i++;
    }
  }

  freeLogHandle(h);
  assert(i == (ENTRY_COUNT * 2));

  for(int i = 0; i < ENTRY_COUNT; i++) {
    free(entries[i]);
    free(entries2[i]);
  }

  stasis_truncation_automatic = 1;
  stasis_log_file->close(stasis_log_file);
}

START_TEST(loggerReopenTest) {
  stasis_log_type = LOG_TO_FILE;
  stasis_log_safe_writes_delete(stasis_log_file_name);
  reopenLogWorkload(0);

} END_TEST

START_TEST(loggerTruncateReopenTest) {
  stasis_log_type = LOG_TO_FILE;
  stasis_log_safe_writes_delete(stasis_log_file_name);
  reopenLogWorkload(1);
} END_TEST

static void loggerEmptyForce_helper() {
  Tinit();
  int xid = Tbegin();
  TsoftCommit(xid);
  TforceCommits();
  Tdeinit();
}
START_TEST(loggerEmptyFileForceTest) {
  stasis_log_type = LOG_TO_FILE;
  loggerEmptyForce_helper();
} END_TEST
START_TEST(loggerEmptyMemForceTest) {
  stasis_log_type = LOG_TO_MEMORY;
  loggerEmptyForce_helper();
} END_TEST
Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");
  tcase_set_timeout(tc, 0);
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, loggerEmptyFileForceTest);
  tcase_add_test(tc, loggerEmptyMemForceTest);
  tcase_add_test(tc, loggerFileTest);
  tcase_add_test(tc, loggerMemTest);
  tcase_add_test(tc, logHandleFileColdReverseIterator);
  tcase_add_test(tc, logHandleMemColdReverseIterator);
  tcase_add_test(tc, loggerFileTruncate);
  tcase_add_test(tc, loggerMemTruncate);
  tcase_add_test(tc, loggerFileCheckWorker);
  tcase_add_test(tc, loggerMemCheckWorker);
  tcase_add_test(tc, loggerFileCheckThreaded);
  tcase_add_test(tc, loggerMemCheckThreaded);
  if(stasis_log_type != LOG_TO_MEMORY) {
    tcase_add_test(tc, loggerReopenTest);
    tcase_add_test(tc, loggerTruncateReopenTest);
  }

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
