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
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
/** For O_DIRECT.  It's unclear that this is the correct thing to #define, but it works under linux. */
#define __USE_GNU
#include <fcntl.h>
#include <unistd.h>


#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include <config.h>
#include <lladd/common.h>

#include <lladd/transactional.h>
#include "logWriter.h"
#include "logHandle.h"
#include "latches.h"
#include "io.h"
#include <assert.h>


#include <lladd/bufferManager.h>

byte * logBuffer = 0;
/** 
    @todo Should the log file be global? 
*/
static FILE * log;

/**
   @see flushedLSN()
*/
static lsn_t flushedLSN_val;

/**
   Invariant: No thread is writing to flushedLSN.  (This lock is not
   needed if doubles are set atomically by the processeor.)  Since
   flushedLSN is monotonically increasing, readers can immmediately
   release their locks after checking the value of flushedLSN.
*/
static rwl * flushedLSN_lock;

/**
   
   Before writeLogEntry is called, this value is 0. Once writeLogEntry
   is called, it is the next available LSN.

   @see writeLogEntry
*/
static lsn_t nextAvailableLSN = 0;
static lsn_t writtenLSN_val  = 0;
static int bufferedSize = 0;
/**
   Invariant: writeLogEntry() must be able to atomicly read
   nextAvailableLSN, and then update it.  (This lock does not have to
   be held while we're waiting for fwrite() to return.)
*/
static rwl * nextAvailableLSN_lock;

/**
   The global offset for the current version of the log file.
 */
static lsn_t global_offset;
/**
   Invariant: Any thread reading from the file must call flockfile()
   if it needs the file position to be preserved across calls.  (For
   example, when using fseek(); myFseek() does this, but only
   internally, so if it is used to position the stream, it should be
   guarded with flockfile().  Unfortunately, it appears as though we
   cannot use flockfile() on some systems, because this sequence does
   not behave correctly:

   flockfile(foo);
   fclose(foo);
   fopen(foo);
   funlockfile(foo);

   Oh well.
*/
static rwl * log_read_lock;

/**
   Invariant: Any thread writing to the file must hold this lock.  The
   log truncation thread hold this lock from the point where it copies
   the tail of the old log to the new log, until after the rename call
   returns.
*/    
pthread_mutex_t log_write_mutex; 

/**
   Invariant:  We only want one thread in truncateLog at a time.
*/
pthread_mutex_t truncateLog_mutex;


static int sought = 1;
int openLogWriter() {
#define BUFSIZE (1024*96)
//#define BUFSIZE (512)
  char * buffer ;/*= malloc(BUFSIZE);*/

  assert(!posix_memalign((void*)&(buffer), PAGE_SIZE, BUFSIZE));

  int logFD = open (LOG_FILE, O_CREAT | O_RDWR | O_APPEND /*| O_SYNC*/, S_IRWXU | S_IRWXG | S_IRWXO);
  if(logFD == -1) {
    perror("Couldn't open log file (A)");
    abort();
  }
  log = fdopen(logFD, "a+");
  //  log = fopen(LOG_FILE, "a+");

  if (log==NULL) {
    perror("Couldn't open log file");
    abort();
    /*there was an error opening this file */
    return FILE_WRITE_OPEN_ERROR;
  }
  
  setbuffer(log, buffer, BUFSIZE);


  /* Initialize locks. */

  flushedLSN_lock = initlock();
  nextAvailableLSN_lock = initlock();
  log_read_lock = initlock();
  pthread_mutex_init(&log_write_mutex, NULL);
  pthread_mutex_init(&truncateLog_mutex, NULL);


  nextAvailableLSN = 0;
  bufferedSize = 0;
  writtenLSN_val= 0;

  /*  maxLSNEncountered = sizeof(lsn_t); 
      writeLogEntryIsReady = 0; */

  /* Note that the position of the file between calls to this library
     does not matter, since none of the functions in logWriter.h
     assume anything about the position of the stream before they are
     called.

     However, we need to do this seek to check if the file is empty.

  */

  if (myFseek(log, 0, SEEK_END)==0) {
    /*if file is empty, write an LSN at the 0th position.  LSN 0 is
      invalid, and this prevents us from using it.  Also, the LSN at
      this position can be used after log truncation to define a
      global offset for the truncated log.  (Not implemented yet)
    */
    lsn_t zero = 0;
    int nmemb = fwrite(&zero, sizeof(lsn_t), 1, log);
    if(nmemb != 1) {
      perror("Couldn't start new log file!");
      assert(0);
      return FILE_WRITE_OPEN_ERROR;
    }
    global_offset = 0;
  } else {
    int count;
    myFseek(log, 0, SEEK_SET);
    count = fread(&global_offset, sizeof(lsn_t), 1, log);
    assert(count == 1);
  }
  sought =1;
  return 0;
}


/** 
    @internal 

    Unfortunately, this function can't just seek to the end of the
    log.  If it did, and a prior instance of LLADD crashed (and wrote
    a partial entry), then the log would be corrupted.  Therefore, we
    need to be a little bit smarter, and track the next LSN value
    manually.  Calculating it the first time would require a scan over
    the entire log, so we use the following optimization:

    Every time readLSN is called, we check to see if it is called with
    the highest LSN that we've seen so far.  (If writeLogEntry has not
    been called yet.)

    The first time writeLogEntry is called, we seek from the highest
    LSN encountered so far to the end of the log.
    
*/

static int flushLog();

int writeLogEntry(LogEntry * e) {

  const long size = sizeofLogEntry(e);

  if(e->type == UPDATELOG) {
    /*     addPendingEvent(e->contents.update.rid.page); */
  }
  if(e->type == CLRLOG) {
    /*    addPendingEvent(e->contents.clr.rid.page); */
  }

  if(e->xid == -1) { /* Don't write log entries for recovery xacts. */
    e->LSN = -1; 
    return 0;
  }

  /* Need to prevent other writers from messing with nextAvailableLSN.
     The log_write_mutex only blocks log truncation and writeLogEntry,
     so it's exactly what we want here. .*/
  pthread_mutex_lock(&log_write_mutex);  
  
  if(!nextAvailableLSN) { 
    /*  if(!writeLogEntryIsReady) { */
    LogHandle lh;
    LogEntry * le;

    nextAvailableLSN = sizeof(lsn_t);
    lh = getLSNHandle(nextAvailableLSN);

    while((le = nextInLog(&lh))) {
      nextAvailableLSN = le->LSN + sizeofLogEntry(le) + sizeof(long);;
      free(le);
    }
  }

  writelock(log_read_lock, 100);

  /* Set the log entry's LSN. */

#ifdef DEBUGGING
  e->LSN = myFseek(log, 0, SEEK_END) + global_offset;
  sought = 1;
  if(nextAvailableLSN != e->LSN) {
    assert(nextAvailableLSN <= e->LSN);
    DEBUG("Detected log truncation:  nextAvailableLSN = %ld, but log length is %ld.\n", (long)nextAvailableLSN, e->LSN);
  }
#endif

  e->LSN = nextAvailableLSN;

  /* We have the write lock, so no one else can call fseek behind our back. */
  /*  flockfile(log); */ /* Prevent other threads from calling fseek... */

  nextAvailableLSN += (size + sizeof(long));
  int oldBufferedSize = bufferedSize;
  bufferedSize     += (size + sizeof(long));

  logBuffer = realloc(logBuffer, size + sizeof(long));
  if(! logBuffer) {
    abort();
  }
  memcpy(logBuffer + oldBufferedSize, &size, sizeof(long));
  memcpy(logBuffer + oldBufferedSize + sizeof(long), e, size);

  flushLog();
  
  pthread_mutex_unlock(&log_write_mutex);  
  writeunlock(log_read_lock);

  /* We're done. */
  return 0;
}
/** 
    Preliminary version of a function that will write multiple log
    entries at once.  It turns out that there are some nasty
    interactions between write() calls and readLSN, and locking, so
    this currently only writes one entry at a time. (If this function
    weren't designed to bundle log entries together, it would not make
    such heavy use of global variables...) */
static int flushLog() {
  if (!logBuffer) { return 0;}

  if(sought) {
    fseek(log, writtenLSN_val /*nextAvailableLSN*/ - global_offset, SEEK_SET); 
    sought = 0;
  }  

  int nmemb = fwrite(logBuffer, bufferedSize, 1, log);
  writtenLSN_val += bufferedSize;
  bufferedSize = 0;
  
  if(nmemb != 1) {
    perror("writeLog couldn't write next log entry!");
    assert(0);
    return FILE_WRITE_ERROR;
  }
  return 0;
  
}

void syncLog() {
  lsn_t newFlushedLSN;
  if(sought) {
    newFlushedLSN = myFseek(log, 0, SEEK_END);
    sought = 1;
  } else {
    newFlushedLSN = ftell(log);
  }
  /* Wait to set the static variable until after the flush returns. */
  
  fflush(log);
  // Since we open the logfile with O_SYNC, fflush suffices.
#ifdef HAVE_FDATASYNC
  /* Should be available in linux >= 2.4 */
  fdatasync(fileno(log));  
#else
  /* Slow - forces fs implementation to sync the file metadata to disk */
  fsync(fileno(log));   
#endif

  writelock(flushedLSN_lock, 0);
  if(newFlushedLSN > flushedLSN_val) {
    flushedLSN_val = newFlushedLSN;
  }
  writeunlock(flushedLSN_lock);
}

lsn_t flushedLSN() {
  lsn_t ret;
  readlock(flushedLSN_lock, 0);
  ret = flushedLSN_val;
  readunlock(flushedLSN_lock);
  return ret; 
}

void closeLogWriter() {
  /* Get the whole thing to the disk before closing it. */
  syncLog();  
  fclose(log);

  /* Free locks. */

  deletelock(flushedLSN_lock);
  deletelock(nextAvailableLSN_lock);
  deletelock(log_read_lock);
  pthread_mutex_destroy(&log_write_mutex);
  pthread_mutex_destroy(&truncateLog_mutex);

}

void deleteLogWriter() {
  remove(LOG_FILE);
}

static LogEntry * readLogEntry() {
  LogEntry * ret = NULL;
  long size;
  //  assert(!posix_memalign((void*)&(size), 512, sizeof(long)));
  long entrySize;
  int nmemb;
  
  if(feof(log)) {
    return NULL; 
  }

  nmemb = fread(&size, sizeof(long), 1, log);
  
  if(nmemb != 1) {
    if(feof(log)) {
      return NULL;
    }
    if(ferror(log)) {
      perror("Error reading log!");
      assert(0);
      return 0;
    }
  }

  //  assert(!posix_memalign(&ret, 512, (*size)));
  ret = malloc(size);

  nmemb = fread(ret, size, 1, log);

  if(nmemb != 1) {
    /* Partial log entry. */
    if(feof(log)) {
      free(ret);
      return NULL;
    }
    if(ferror(log)) {
      perror("Error reading log!");
      free(ret);
      assert(0);
      return 0;
    }
    assert(0);
    free(ret);
    return 0;
  }


  entrySize = sizeofLogEntry(ret);


  /** Sanity check -- Did we get the whole entry? */
  if(size < entrySize) {
    return 0;
  }


  assert(size == entrySize);

  return ret;
}

LogEntry * readLSNEntry(lsn_t LSN) {
  LogEntry * ret;

  /* We would need a lock to support this operation, and that's not worth it.  
     if(!writeLogEntryIsReady) {
     if(LSN > maxLSNEncountered) {
     maxLSNEncountered = LSN;
     }
     } */

  /*  readlock(log_read_lock); */

  /* Irritating overhead; two mutex acquires to do a read. */
  readlock(log_read_lock, 200);

  flockfile(log); 
  fseek(log, LSN - global_offset, SEEK_SET);
  sought = 1;
  ret = readLogEntry();
  funlockfile(log);

  readunlock(log_read_lock); 

  return ret;
  
}


int truncateLog(lsn_t LSN) {
  FILE *tmpLog;


  LogEntry * le;
  LogHandle lh;

  long size;

  /*  int count; */

  pthread_mutex_lock(&truncateLog_mutex);

  if(global_offset + 4 >= LSN) {
    /* Another thread beat us to it...the log is already truncated
       past the point requested, so just return. */
    pthread_mutex_unlock(&truncateLog_mutex);
    return 0;
  }

  tmpLog = fopen(LOG_FILE_SCRATCH, "w+");  /* w+ = truncate, and open for writing. */

  if (tmpLog==NULL) {
    assert(0);
    /*there was an error opening this file */
    perror("logTruncate() couldn't create scratch log file!");
    return FILE_WRITE_OPEN_ERROR;
  }

  /* Need to write LSN - sizeof(lsn_t) to make room for the offset in
     the file.  If we truncate to lsn 10, we'll put lsn 10 in position
     4, so the file offset is 6. */
  LSN -= sizeof(lsn_t);  

  DEBUG("Truncating log to %ld\n", LSN + sizeof(lsn_t));

  myFwrite(&LSN, sizeof(lsn_t), tmpLog);
  
  LSN += sizeof(lsn_t);

  /**
     @todo We block writers too early.  Instead, read until EOF, then
     lock, and then finish the truncate.
  */
  pthread_mutex_lock(&log_write_mutex);


  lh = getLSNHandle(LSN);


  while((le = nextInLog(&lh))) {
    size = sizeofLogEntry(le);
    myFwrite(&size, sizeof(lsn_t), tmpLog);
    myFwrite(le, size, tmpLog);
    free (le);
  } 

  writelock(log_read_lock, 300);
  
  fflush(tmpLog);
#ifdef HAVE_FDATASYNC
  fdatasync(fileno(tmpLog));
#else
  fsync(fileno(tmpLog));
#endif

  /** Time to shut out the readers */

  /*  flockfile(log); --- Don't need this; we hold the writelock. */

  fclose(log);  /* closeLogWriter calls sync, but we don't need to. :) */
  fclose(tmpLog); 
 
  if(rename(LOG_FILE_SCRATCH, LOG_FILE)) {
    perror("Log truncation failed!");
    abort();
  }


  log = fopen(LOG_FILE, "a+");
  if (log==NULL) {
    abort();
    /*there was an error opening this file */
    return FILE_WRITE_OPEN_ERROR;
  }

  /*  myFseek(log, 0, SEEK_SET); */
  global_offset = LSN - sizeof(lsn_t); /*= fread(&global_offset, sizeof(lsn_t), 1, log);*/
  /*assert(count == 1); */

  /*  funlockfile(log);  */
  writeunlock(log_read_lock);
  pthread_mutex_unlock(&log_write_mutex);

  pthread_mutex_unlock(&truncateLog_mutex);

  return 0;

}

lsn_t firstLogEntry() {
  return global_offset + sizeof(lsn_t);
}
