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
#include <lladd/logger/logWriter.h>
#include <lladd/logger/logHandle.h>
#include <assert.h>
#include <config.h>
#include <stdlib.h>
#include <malloc.h>
#include <unistd.h>
/** 
    Invariant: This file stream is at EOF, or positioned so that the
    next read will pull in the size of the next log entry.

    @todo Should the log file be global? 
*/
static FILE * log;

/**
   @see flushedLSN()
*/
static lsn_t flushedLSN_val;

/**
   
   Before writeLogEntry is called, this value is the value of the
   highest LSN encountered so far.  Once writeLogEntry is called, it
   is the next available LSN.

   @see writeLogEntry
*/
static lsn_t nextAvailableLSN = 0;
static int writeLogEntryIsReady = 0;
static lsn_t maxLSNEncountered = sizeof(lsn_t);

static lsn_t global_offset;


/** 
    @todo Put myFseek, myFwrite in their own file, and make a header for it... */

void myFwrite(const void * dat, size_t size, FILE * f);
long myFseek(FILE * f, long offset, int whence);
int openLogWriter() {
  log = fopen(LOG_FILE, "a+");
  if (log==NULL) {
    assert(0);
    /*there was an error opening this file */
    return FILE_WRITE_OPEN_ERROR;
  }

  nextAvailableLSN = 0;
  maxLSNEncountered = sizeof(lsn_t);
  writeLogEntryIsReady = 0;

  /* Note that the position of the file between calls to this library
     does not matter, since none of the functions in logWriter.h
     assume anything about the position of the stream before they are
     called.

     However, we need to do this seek to check if the file is empty.

  */
  fseek(log, 0, SEEK_END); 

  if (ftell(log)==0) {
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

    The first time writeLogEntry is called, we seekfrom the highest
    LSN encountered so far to the end of the log.
    
*/
int writeLogEntry(LogEntry * e) {
  int nmemb;
  const size_t size = sizeofLogEntry(e);

  if(e->xid == -1) { /* Don't write log entries for recovery xacts. */
    e->LSN = -1;
    return 0;
  }
  
  if(!writeLogEntryIsReady) {
    LogHandle lh;
    LogEntry * le;
    
    assert(maxLSNEncountered >= sizeof(lsn_t));

    lh = getLSNHandle(maxLSNEncountered);

    nextAvailableLSN = maxLSNEncountered;

    while((le = nextInLog(&lh))) {
      nextAvailableLSN = le->LSN + sizeofLogEntry(le) + sizeof(size_t);;
      free(le);
    }
    writeLogEntryIsReady = 1;
  }



  /* Set the log entry's LSN. */

#ifdef DEBUGGING
  fseek(log, 0, SEEK_END);
  e->LSN = ftell(log);
  if(nextAvailableLSN != e->LSN) {
    assert(nextAvailableLSN <= e->LSN);
    DEBUG("Detected log truncation:  nextAvailableLSN = %ld, but log length is %ld.\n", (long)nextAvailableLSN, e->LSN);
  }
#endif

  e->LSN = nextAvailableLSN;
  fseek(log, nextAvailableLSN - global_offset, SEEK_SET); 
      
  nextAvailableLSN += (size + sizeof(size_t));

  /* Print out the size of this log entry.  (not including this item.) */
  nmemb = fwrite(&size, sizeof(size_t), 1, log);

  if(nmemb != 1) {
    perror("writeLog couldn't write next log entry size!");
    assert(0);
    return FILE_WRITE_ERROR;
  }
  
  nmemb = fwrite(e, size, 1, log);

  if(nmemb != 1) {
    perror("writeLog couldn't write next log entry!");
    assert(0);
    return FILE_WRITE_ERROR;
  }
  
  /* We're done. */
  return 0;
}

void syncLog() {
  lsn_t newFlushedLSN;
  
  fseek(log, 0, SEEK_END);
  /* Wait to set the static variable until after the flush returns.
     (In anticipation of multithreading) */
  newFlushedLSN = ftell(log);  
  
  fflush(log);
#ifdef HAVE_FDATASYNC
  /* Should be available in linux >= 2.4 */
  fdatasync(fileno(log)); 
#else
  /* Slow - forces fs implementation to sync the file metadata to disk */
  fsync(fileno(log));  
#endif

  flushedLSN_val = newFlushedLSN;
  
}

lsn_t flushedLSN() {
  return flushedLSN_val;
}

void closeLogWriter() {
  /* Get the whole thing to the disk before closing it. */
  syncLog();  
  fclose(log);
}

void deleteLogWriter() {
  remove(LOG_FILE);
}

static LogEntry * readLogEntry() {
  LogEntry * ret = NULL;
  size_t size, entrySize;
  int nmemb;
  
  if(feof(log)) return NULL;

  nmemb = fread(&size, sizeof(size_t), 1, log);
  
  if(nmemb != 1) {
    if(feof(log)) return NULL;
    if(ferror(log)) {
      perror("Error reading log!");
      assert(0);
      return 0;
    }
  }

  ret = malloc(size);

  nmemb = fread(ret, size, 1, log);

  if(nmemb != 1) {
    /* Partial log entry. */
    if(feof(log)) return NULL;
    if(ferror(log)) {
      perror("Error reading log!");
      assert(0);
      return 0;
    }
    assert(0);
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

  if(!writeLogEntryIsReady) {
    if(LSN > maxLSNEncountered) {
      maxLSNEncountered = LSN;
    }
  }

  fseek(log, LSN - global_offset, SEEK_SET);
  ret = readLogEntry();

  return ret;
}

int truncateLog(lsn_t LSN) {
  FILE *tmpLog = fopen(LOG_FILE_SCRATCH, "w+");  /* w+ = truncate, and open for writing. */

  LogEntry * le;
  LogHandle lh;

  long size;

  int count;


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

  DEBUG("Truncate(%ld) new file offset = %ld\n", LSN + sizeof(lsn_t), LSN);

  myFwrite(&LSN, sizeof(lsn_t), tmpLog);
  
  LSN += sizeof(lsn_t);
  
  lh = getLSNHandle(LSN);
  
  while((le = nextInLog(&lh))) {
    size = sizeofLogEntry(le);
    myFwrite(&size, sizeof(lsn_t), tmpLog);
    myFwrite(le, size, tmpLog);
    free (le);
  } 
  
  fflush(tmpLog);
#ifdef HAVE_FDATASYNC
  fdatasync(fileno(tmpLog));
#else
  fsync(fileno(tmpLog));
#endif

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

  myFseek(log, 0, SEEK_SET);
  count = fread(&global_offset, sizeof(lsn_t), 1, log);
  assert(count == 1);


  return 0;

}

lsn_t firstLogEntry() {
  return global_offset + sizeof(lsn_t);
}

void myFwrite(const void * dat, size_t size, FILE * f) {
  int nmemb = fwrite(dat, size, 1, f);
  /* test */
  if(nmemb != 1) {
    perror("myFwrite");
    abort();
    /*    return FILE_WRITE_OPEN_ERROR; */
  }
  
}
