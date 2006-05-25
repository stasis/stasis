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

/** 
    @todo Should the log file be global? 
*/
static FILE * log = 0;
static int roLogFD = 0;

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

/**
   The global offset for the current version of the log file.
 */
static lsn_t global_offset;
/**
   This mutex makes sequences of calls to lseek() and read() atomic.
   It is also used by truncateLog to block read requests while
   rename() is called.
*/
pthread_mutex_t log_read_mutex;

/**
   Invariant: Any thread writing to the file must hold this lock.  The
   log truncation thread hold this lock from the point where it copies
   the tail of the old log to the new log, until after the rename call
   returns.  This mutex also protects nextAvailableLSN.
*/    
pthread_mutex_t log_write_mutex; 

/**
   Invariant:  We only want one thread in truncateLog at a time.
*/
pthread_mutex_t truncateLog_mutex;

static char * buffer;

/** The size of the in-memory log buffer.  When the buffer is full,
    the log is synchronously flushed to disk. */
#define BUFSIZE (1024 * 1024)
//#define BUFSIZE (1024*96)
//#define BUFSIZE (512)

int openLogWriter() {

  buffer = malloc(BUFSIZE);
  
  if(!buffer) { return LLADD_NO_MEM; }

  /* The file is opened twice for a reason.  fseek() seems to call
     fflush() under Linux, which normally would be a minor problem.
     However, we open the log with O_SYNC, so the fflush() results in
     synchronous disk writes.  Therefore, all read accesses (and
     therefore all seeks) run through the second descriptor.  */

  int logFD = open (LOG_FILE, O_CREAT | O_WRONLY | O_APPEND | O_SYNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if(logFD == -1) {
    perror("Couldn't open log file for append.\n");
    abort();
  }
  log = fdopen(logFD, "a");

  if (log==NULL) {
    perror("Couldn't open log file");
    abort();
    return LLADD_IO_ERROR; 
  }
  
  setbuffer(log, buffer, BUFSIZE);

  /* fread() doesn't notice when another handle writes to its file,
     even if fflush() is used to push the changes out to disk.
     Therefore, we use a file descriptor, and read() instead of a FILE
     and fread(). */
  roLogFD = open (LOG_FILE, O_RDONLY, 0);

  if(roLogFD == -1) {
    perror("Couldn't open log file for reads.\n");
  }

  /* Initialize locks. */

  flushedLSN_lock = initlock();
  pthread_mutex_init(&log_read_mutex, NULL);
  pthread_mutex_init(&log_write_mutex, NULL);
  pthread_mutex_init(&truncateLog_mutex, NULL);


  flushedLSN_val = 0;
  nextAvailableLSN = 0;

  /*
    Seek append only log to the end of the file.  This is unnecessary,
    since the file was opened in append only mode, but it returns the
    length of the file.
  */

  if (myFseek(log, 0, SEEK_END)==0) {
    /*if file is empty, write an LSN at the 0th position.  LSN 0 is
      invalid, and this prevents us from using it.  Also, the LSN at
      this position can be used after log truncation to define a
      global offset for the truncated log.  (Not implemented yet)
    */
    lsn_t zero = 0;
    size_t nmemb = fwrite(&zero, sizeof(lsn_t), 1, log);
    if(nmemb != 1) {
      perror("Couldn't start new log file!");
      //      assert(0);
      return LLADD_IO_ERROR;
    }
    global_offset = 0;
  } else {

    off_t newPosition = lseek(roLogFD, 0, SEEK_SET);
    if(newPosition == -1) {
      perror("Could not seek to head of log");
    }
    
    ssize_t bytesRead = read(roLogFD, &global_offset, sizeof(lsn_t));
    
    if(bytesRead != sizeof(lsn_t)) {
      printf("Could not read log header.");
      abort();
    }

  }
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

    @todo writeLogEntry implicitly ignores all log entries with xid = -1.  
    This is probably the wrong thing to do...
    
*/

int writeLogEntry(LogEntry * e) {

  const lsn_t size = sizeofLogEntry(e);

  pthread_mutex_lock(&log_write_mutex);  
  
  if(!nextAvailableLSN) { 

    LogHandle lh;
    const LogEntry * le;

    nextAvailableLSN = sizeof(lsn_t);
    lh = getLSNHandle(nextAvailableLSN);

    while((le = nextInLog(&lh))) {
      nextAvailableLSN = le->LSN + sizeofLogEntry(le) + sizeof(lsn_t);;
      FreeLogEntry(le);
    }
  }

  /* Set the log entry's LSN. */
  e->LSN = nextAvailableLSN;
  //printf ("\nLSN: %ld\n", e->LSN);
  //fflush(stdout);

  nextAvailableLSN += (size + sizeof(lsn_t));
  
  size_t nmemb = fwrite(&size, sizeof(lsn_t), 1, log);
  if(nmemb != 1) {
    if(feof(log))   { abort();  /* feof makes no sense here */  }
    if(ferror(log)) {
      fprintf(stderr, "writeLog couldn't write next log entry: %d\n", ferror(log));
      abort();
    }
    return LLADD_IO_ERROR;
  }

  nmemb = fwrite(e, size, 1, log);
  
  if(nmemb != 1) {
    if(feof(log)) { abort();  /* feof makes no sense here */ }
    if(ferror(log)) {
      fprintf(stderr, "writeLog couldn't write next log entry: %d\n", ferror(log));
      abort();
    }
    return LLADD_IO_ERROR;
  }

  //fflush(log);
  
  pthread_mutex_unlock(&log_write_mutex);  

  return 0;
}

void syncLog() {
  lsn_t newFlushedLSN;

  pthread_mutex_lock(&log_read_mutex);
  newFlushedLSN = ftell(log) + global_offset;
  pthread_mutex_unlock(&log_read_mutex);
  // Wait to set the static variable until after the flush returns. 

  // Since we opened the logfile with O_SYNC, fflush() is sufficient.
  fflush(log);

  writelock(flushedLSN_lock, 0);
  if(newFlushedLSN > flushedLSN_val) {
    flushedLSN_val = newFlushedLSN;
  }
  writeunlock(flushedLSN_lock);
}

lsn_t flushedLSN() {
  readlock(flushedLSN_lock, 0);
  lsn_t ret = flushedLSN_val;
  readunlock(flushedLSN_lock);
  return ret; 
}

void closeLogWriter() {
  /* Get the whole thing to the disk before closing it. */
  syncLog();  

  fclose(log);
  close(roLogFD);
  log = NULL;

  /* Free locks. */

  deletelock(flushedLSN_lock);
  pthread_mutex_destroy(&log_read_mutex);
  pthread_mutex_destroy(&log_write_mutex);
  pthread_mutex_destroy(&truncateLog_mutex);
  free(buffer);  
}

void deleteLogWriter() {
  remove(LOG_FILE);
}
lsn_t debug_lsn = -1;
static LogEntry * readLogEntry() {
  LogEntry * ret = 0;
  lsn_t size;
  lsn_t entrySize;
  
  ssize_t bytesRead = read(roLogFD, &size, sizeof(lsn_t));

  if(bytesRead != sizeof(lsn_t)) { 
    if(bytesRead == 0) {
      //      fprintf(stderr, "eof on log entry size\n");
      //      fflush(stderr);
      return NULL;
    } else if(bytesRead == -1) {
      perror("error reading log");
      abort();
      return (LogEntry*)LLADD_IO_ERROR;
    } else { 
      fprintf(stderr, "short read from log.  Expected %ld bytes, got %ld.\nFIXME: This is 'normal', but currently not handled", sizeof(lsn_t), bytesRead);
      fflush(stderr);
      abort();  // really abort here.  This code should attempt to piece together short log reads...
    }
  }
  ret = malloc(size);

  //printf("Log entry is %ld bytes long.\n", size);
  //fflush(stdout);

  bytesRead = read(roLogFD, ret, size);

  if(bytesRead != size) {
    if(bytesRead == 0) {
      //      fprintf(stderr, "eof reading entry\n");
      //      fflush(stderr);
      return(NULL);
    } else if(bytesRead == -1) {
      perror("error reading log");
      abort();
      return (LogEntry*)LLADD_IO_ERROR;
    } else { 
      printf("short read from log w/ lsn %ld.  Expected %ld bytes, got %ld.\nFIXME: This is 'normal', but currently not handled", debug_lsn, size, bytesRead);
      fflush(stderr);
      lsn_t newSize = size - bytesRead;
      lsn_t newBytesRead = read (roLogFD, ((byte*)ret)+bytesRead, newSize);
      printf("\nattempt to read again produced newBytesRead = %ld, newSize was %ld\n", newBytesRead, newSize);
      fflush(stderr);
      abort();
      return (LogEntry*)LLADD_IO_ERROR;
    } 
  }

  entrySize = sizeofLogEntry(ret);
  assert(size == entrySize);

  return ret;
}

LogEntry * readLSNEntry(lsn_t LSN) {
  LogEntry * ret;

  /** Because we use two file descriptors to access the log, we need
      to flush the log write buffer before concluding we're at EOF. */
  if(flushedLSN() <= LSN && LSN < nextAvailableLSN) {
    //    fprintf(stderr, "Syncing log flushed = %d, requested = %d\n", flushedLSN(), LSN);
    syncLog();
    assert(flushedLSN() >= LSN);
    //    fprintf(stderr, "Synced log flushed = %d, requested = %d\n", flushedLSN(), LSN);
  }

  pthread_mutex_lock(&log_read_mutex);

  assert(global_offset <= LSN);

  debug_lsn = LSN;
  off_t newPosition = lseek(roLogFD, LSN - global_offset, SEEK_SET);
  if(newPosition == -1) {
    perror("Could not seek for log read");
    abort();
  } else {
    //    fprintf(stderr, "sought to %d\n", (int)newPosition);
    //    fflush(stderr);
  }

  ret = readLogEntry();
  assert(ret || LSN >= nextAvailableLSN);

  pthread_mutex_unlock(&log_read_mutex);

  return ret;
  
}

int truncateLog(lsn_t LSN) {
  FILE *tmpLog;

  const LogEntry * le;
  LogHandle lh;

  lsn_t size;

  pthread_mutex_lock(&truncateLog_mutex);

  if(global_offset + sizeof(lsn_t) >= LSN) {
    /* Another thread beat us to it...the log is already truncated
       past the point requested, so just return. */
    pthread_mutex_unlock(&truncateLog_mutex);
    return 0;
  }

  tmpLog = fopen(LOG_FILE_SCRATCH, "w+");  /* w+ = truncate, and open for writing. */

  if (tmpLog==NULL) {
    pthread_mutex_unlock(&truncateLog_mutex);
    perror("logTruncate() couldn't create scratch log file!");
    abort();
    return LLADD_IO_ERROR;
  }

  /* Need to write LSN - sizeof(lsn_t) to make room for the offset in
     the file.  If we truncate to lsn 10, we'll put lsn 10 in position
     4, so the file offset is 6. */
  LSN -= sizeof(lsn_t);  

  DEBUG("Truncating log to %ld\n", LSN + sizeof(lsn_t));

  myFwrite(&LSN, sizeof(lsn_t), tmpLog);
  
  LSN += sizeof(lsn_t);

  /**
     @todo truncateLog blocks writers too early.  Instead, read until EOF, then
     lock, and then finish the truncate.
  */
  pthread_mutex_lock(&log_write_mutex);

  fflush(log);

  lh = getLSNHandle(LSN);
  lsn_t lengthOfCopiedLog = 0;
  while((le = nextInLog(&lh))) {
    size = sizeofLogEntry(le);
    lengthOfCopiedLog += (size + sizeof(lsn_t));
    myFwrite(&size, sizeof(lsn_t), tmpLog);
    myFwrite(le, size, tmpLog);
    FreeLogEntry(le);
  } 
  assert(nextAvailableLSN == LSN + lengthOfCopiedLog);
  fflush(tmpLog);
#ifdef HAVE_FDATASYNC
  fdatasync(fileno(tmpLog));
#else
  fsync(fileno(tmpLog));
#endif

  /** Time to shut out the readers */

  pthread_mutex_lock(&log_read_mutex);

  /* closeLogWriter calls sync, and does some extra stuff that we don't want, so we
     basicly re-implement closeLogWriter and openLogWriter here...
  */
  fclose(log);  
  close(roLogFD);
  fclose(tmpLog); 
 
  if(rename(LOG_FILE_SCRATCH, LOG_FILE)) {
    pthread_mutex_unlock(&log_read_mutex);
    pthread_mutex_unlock(&log_write_mutex);
    pthread_mutex_unlock(&truncateLog_mutex);
  
    perror("Error replacing old log file with new log file");
    return LLADD_IO_ERROR;
  }
  
  int logFD = open (LOG_FILE, O_CREAT | O_WRONLY | O_APPEND | O_SYNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if(logFD == -1) {
    perror("Couldn't open log file for append.\n");
    abort();
  }
  log = fdopen(logFD, "a");
  
  if (log==NULL) {
    perror("Couldn't open log file");
    abort();
    return LLADD_IO_ERROR; 
  }
  
  setbuffer(log, buffer, BUFSIZE);
  
  roLogFD = open (LOG_FILE, O_RDONLY, 0);

  if(roLogFD == -1) {
    perror("Couldn't open log file for reads.\n");
    abort();
    return LLADD_IO_ERROR;
  }

  if (log==NULL) { 
    pthread_mutex_unlock(&log_read_mutex);
    pthread_mutex_unlock(&log_write_mutex);
    pthread_mutex_unlock(&truncateLog_mutex);

    perror("Couldn't reopen log after truncate");
    abort();
    return LLADD_IO_ERROR;
  }

  global_offset = LSN - sizeof(lsn_t);


  pthread_mutex_unlock(&log_read_mutex);
  pthread_mutex_unlock(&log_write_mutex);
  pthread_mutex_unlock(&truncateLog_mutex);

  return 0;

}

lsn_t firstLogEntry() {
  assert(log);
  pthread_mutex_lock(&log_read_mutex); // for global offset...
  lsn_t ret = global_offset + sizeof(lsn_t);
  pthread_mutex_unlock(&log_read_mutex);
  return ret;
}
