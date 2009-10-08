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
#include <stasis/common.h>
#include <stasis/flags.h>

#include <stasis/latches.h>
#include <stasis/crc32.h>

#include <stasis/logger/safeWrites.h>
#include <stasis/logger/logWriterUtils.h>
#include <stasis/logger/logHandle.h>

#include <assert.h>
#include <stdio.h>
#include <fcntl.h>

/**
 * @file
 *
 * A Stasis log implementation that uses safe writes (copy tail, force, rename) to perform truncation.
 *
 * @todo The safeWrites log implementation is not optimized for reading old entries.
 *
 * @ingroup LOGGING_IMPLEMENTATIONS
 */

/**
   Latch order:  truncate_mutex, write_mutex, read_mutex
*/
typedef struct {
  FILE * fp;
  int  ro_fd;
  const char * filename;
  const char * scratch_filename;
  int filemode;
  int fileperm;
  char softcommit;

  /**
     The first unstable LSN for write ahead logging purposes.
     @see flushedLSN_LogWriter()
  */
  lsn_t flushedLSN_wal;
  /**
     The first unstable LSN for commit purposes.  This can be greater than
     flushedLSN_wal.
  */
  lsn_t flushedLSN_commit;
  /**
     The first LSN that hasn't made it into roLogFD.  This can be greater than
     flushedLSN_commit
  */
  lsn_t flushedLSN_internal;
  /**
     The LSN that will be assigned to the next log entry.
   */
  lsn_t nextAvailableLSN;
  /**
     The global offset for the current version of the log file.
  */
  lsn_t global_offset;
  /**
     Invariant:  Hold while in truncateLog()
  */
  pthread_mutex_t truncate_mutex;
  /**
     Invariant: Any thread writing to the file must hold this latch.  The
     log truncation thread holds this latch from the point where it copies
     the tail of the old log to the new log, until after the rename call
     returns.
  */
  pthread_mutex_t write_mutex;
  /**
     This mutex makes sequences of calls to lseek() and read() atomic.
     It is also used by truncateLog to block read requests while
     rename() is called.
  */
  pthread_mutex_t read_mutex;
  /**
     This mutex protects nextAvailableLSN, which has its own mutex
     because the routines for reading and writing log entries both need
     to acquire it, but only for a relatively short time.
  */
  pthread_mutex_t nextAvailableLSN_mutex;
  /**
     Invariant: No thread is writing to flushedLSN.  Since
     flushedLSN is monotonically increasing, readers can immmediately
     release their locks after checking the value of flushedLSN.
  */
  rwl* flushedLSN_latch;
  /**
     In memory log buffer.  Managed by fwrite().
  */
  char * buffer;
  /**
      CRC of the log between last CRC log entry, and the current end of
      the log.  The CRC includes everything except for the CRC log entry
      and the size fields preceeding each log entry.  This value is
      reset to zero each time a CRC entry is generated..
  */
  unsigned int crc;


} stasis_log_safe_writes_state;

static LogEntry * readLogEntry(stasis_log_safe_writes_state * sw) {
  LogEntry * ret = 0;
  lsn_t size;

  lsn_t bytesRead = read(sw->ro_fd, &size, sizeof(lsn_t));

  if(bytesRead != sizeof(lsn_t)) {
    if(bytesRead == 0) {
      return NULL;
    } else if(bytesRead == -1) {
      perror("error reading log");
      abort();
      return (LogEntry*)LLADD_IO_ERROR;
    } else {
      lsn_t newSize = size - bytesRead;
      lsn_t newBytesRead = read(sw->ro_fd, ((byte*)&size)+bytesRead, newSize);

      fprintf(stdout, "Trying to piece together short read.\n"); fflush(stderr);

      if(newBytesRead == 0) {
        return NULL;
      }
      fprintf(stderr, "short read from log.  Expected %lld bytes, got %lld.\n"
                      "FIXME: This is 'normal', but currently not handled",
              (long long) sizeof(lsn_t), (long long) bytesRead);
      fflush(stderr);
      fprintf(stderr, "\nattempt to read again produced newBytesRead = %lld, "
                      "newSize was %lld\n", newBytesRead, newSize);
      fflush(stderr);
      // XXX This code should attempt to piece together short log reads...
      abort();
    }
  }

  if(!size) {
    return NULL;
  }
  ret = malloc(size);

  bytesRead = read(sw->ro_fd, ret, size);

  if(bytesRead != size) {
    if(bytesRead == 0) {
      fprintf(stderr, "eof reading entry\n");
      fflush(stderr);
      free(ret);
      return(NULL);
    } else if(bytesRead == -1) {
      perror("error reading log");
      abort();
      return (LogEntry*)LLADD_IO_ERROR;
    } else {
      lsn_t newSize = size - bytesRead;
      lsn_t newBytesRead = read (sw->ro_fd, ((byte*)ret)+bytesRead, newSize);

      if(newBytesRead == 0) {
        free(ret);
        return NULL;
      }

      fprintf(stdout, "Trying to piece together short log entry.\n");
      fflush(stderr);

      fprintf(stderr, "short read from log.  Expected %lld bytes, got %lld.\n"
                      "FIXME: This is 'normal', but currently not handled",
              size, bytesRead);
      fprintf(stderr, "\nattempt to read again produced newBytesRead = %lld, "
                      "newSize was %lld\n", newBytesRead, newSize);
      fflush(stderr);

      abort();
      return (LogEntry*)LLADD_IO_ERROR;
    }
  }

  // Would like to do this, but we could reading a partial log entry.
  //assert(sizeofLogEntry(ret) == size);

  return ret;
}

static inline int isDurable_LogWriter(stasis_log_t* log) {
  stasis_log_safe_writes_state* sw = log->impl;
  return !sw->softcommit;
}

static inline lsn_t nextEntry_LogWriter(stasis_log_t* log,
                                        const LogEntry* e) {
  return e->LSN + sizeofLogEntry(log, e) + sizeof(lsn_t);
}

// crc handling

static inline void log_crc_reset(stasis_log_safe_writes_state* sw) {
  sw->crc = 0;
}
static inline void log_crc_update(stasis_log_t* log, const LogEntry * e, unsigned int * crc) {
  *crc = stasis_crc32(e, sizeofLogEntry(log, e), *crc);
}
static LogEntry* log_crc_dummy_entry() {
  LogEntry* ret = allocCommonLogEntry(0, -1, INTERNALLOG);
  assert(ret->prevLSN == 0);
  return ret;
}
static LogEntry* log_crc_entry(unsigned int crc) {
  LogEntry* ret = allocCommonLogEntry(crc, -1, INTERNALLOG);
  return ret;
}

/**
   Scan over the log, looking for the end of log or the first
   corrupted entry.

   @param log a partially recovered log.  This function calls readLogEntry()
          (and copes with corrupt entries), and nextEntry_LogWriter().
   @param ret a known-valid LSN (which will be returned if the log is empty)
 */
static inline lsn_t log_crc_next_lsn(stasis_log_t* log, lsn_t ret) {
  stasis_log_safe_writes_state* sw = log->impl;
  // Using readLogEntry() bypasses checks to see if we're past the end
  // of the log.
  LogEntry * le;
  unsigned int crc = 0;

  while((le = readLogEntry(sw))) {
    if(le->type == INTERNALLOG) {
      if (!(le->prevLSN) || (crc == (unsigned int) le->prevLSN)) {
        ret = nextEntry_LogWriter(log, le);
        crc = 0;
      } else {
        printf("Log corruption: %x != %x (lsn = %lld)\n",
               (unsigned int) le->prevLSN, crc, le->LSN);
        // The log wasn't successfully forced to this point; discard
        // everything after the last CRC.
        freeLogEntry(le);
        break;
      }
    } else {
      log_crc_update(log, le, &crc);
    }
    freeLogEntry(le);
  }
  return ret;
}


/**
    Unfortunately, this function can't just seek to the end of the
    log.  If it did, and a prior instance of Stasis crashed (and wrote
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
static int writeLogEntryUnlocked(stasis_log_t* log, LogEntry * e) {

  stasis_log_safe_writes_state* sw = log->impl;

  const lsn_t size = sizeofLogEntry(log, e);

  /* Set the log entry's LSN. */
  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);
  e->LSN = sw->nextAvailableLSN;
  pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);

  log_crc_update(log, e, &sw->crc);

  DEBUG("Writing Log entry type = %d lsn = %ld, size = %ld\n",
        e->type, e->LSN, size);

  size_t nmemb = fwrite(&size, sizeof(lsn_t), 1, sw->fp);

  if(nmemb != 1) {
    if(feof(sw->fp))   { abort();  /* feof makes no sense here */  }
    if(ferror(sw->fp)) {
      fprintf(stderr, "writeLog couldn't write next log entry: %d\n",
              ferror(sw->fp));
      abort();
    }
    abort();
    // XXX nextAvailableLSN not set...
    return LLADD_IO_ERROR;
  }

  nmemb = fwrite(e, size, 1, sw->fp);

  if(nmemb != 1) {
    if(feof(sw->fp)) { abort();  /* feof makes no sense here */ }
    if(ferror(sw->fp)) {
      fprintf(stderr, "writeLog couldn't write next log entry: %d\n",
              ferror(sw->fp));
      abort();
    }
    abort();
    // XXX nextAvailableLSN not set...
    return LLADD_IO_ERROR;
  }

  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);
  assert(sw->nextAvailableLSN == e->LSN);
  sw->nextAvailableLSN = nextEntry_LogWriter(log, e);
  pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);

  return 0;
}

static int writeLogEntry_LogWriter(stasis_log_t* log, LogEntry * e) {
  stasis_log_safe_writes_state* sw = log->impl;
  pthread_mutex_lock(&sw->write_mutex);
  int ret = writeLogEntryUnlocked(log, e);
  pthread_mutex_unlock(&sw->write_mutex);
  return ret;
}

static lsn_t sizeofInternalLogEntry_LogWriter(stasis_log_t * log,
                                              const LogEntry * e) {
  return sizeof(struct __raw_log_entry);
}

static void syncLogInternal(stasis_log_safe_writes_state* sw) {
  lsn_t newFlushedLSN;

  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);
  newFlushedLSN = sw->nextAvailableLSN;
  if(newFlushedLSN > sw->flushedLSN_internal) {
    pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);
    fflush(sw->fp);
    writelock(sw->flushedLSN_latch, 0);
  }
  if(newFlushedLSN > sw->flushedLSN_internal) {
    sw->flushedLSN_internal = newFlushedLSN;
  }
  unlock(sw->flushedLSN_latch);

}

static void syncLog_LogWriter(stasis_log_t * log,
                              stasis_log_force_mode_t mode) {
  stasis_log_safe_writes_state* sw = log->impl;
  lsn_t newFlushedLSN;

  pthread_mutex_lock(&sw->write_mutex);

  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);
  newFlushedLSN = sw->nextAvailableLSN;
  pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);

  LogEntry* crc_entry = log_crc_entry(sw->crc);
  writeLogEntryUnlocked(log, crc_entry);
  free(crc_entry);
  // Reset log_crc to zero each time a crc entry is written.
  log_crc_reset(sw);

  pthread_mutex_unlock(&sw->write_mutex);

  fflush(sw->fp);

  // We can skip the fsync if we opened with O_SYNC, or if we're in softcommit mode, and not forcing for WAL.

  if((sw->softcommit && mode == LOG_FORCE_WAL)  // soft commit mode; syncing for wal
      || !(sw->softcommit || (sw->filemode & O_SYNC)) // neither soft commit nor opened with O_SYNC
  ) {
#ifdef HAVE_FDATASYNC
  fdatasync(fileno(sw->fp));
#else
  fsync(fileno(sw->fp));
#endif
  }

  // update flushedLSN after fflush returns.
  writelock(sw->flushedLSN_latch, 0);
  if((!sw->softcommit) || mode == LOG_FORCE_WAL) {
    if(newFlushedLSN > sw->flushedLSN_wal) {
      sw->flushedLSN_wal = newFlushedLSN;
    }
  }
  if(newFlushedLSN > sw->flushedLSN_commit) {
    sw->flushedLSN_commit = newFlushedLSN;
  }
  if(newFlushedLSN > sw->flushedLSN_internal) {
    sw->flushedLSN_internal = newFlushedLSN;
  }

  writeunlock(sw->flushedLSN_latch);
}

static lsn_t nextAvailableLSN_LogWriter(stasis_log_t * log) {
  stasis_log_safe_writes_state* sw = log->impl;
  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);
  lsn_t ret = sw->nextAvailableLSN;
  pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);
  return ret;
}

static lsn_t flushedLSN_LogWriter(stasis_log_t* log,
                                  stasis_log_force_mode_t mode) {
  stasis_log_safe_writes_state* sw = log->impl;
  readlock(sw->flushedLSN_latch, 0);
  lsn_t ret;
  if(mode == LOG_FORCE_COMMIT) {
    ret = sw->flushedLSN_commit;
  } else if(mode == LOG_FORCE_WAL) {
    ret = sw->flushedLSN_wal;
  } else {
    abort();
  }
  readunlock(sw->flushedLSN_latch);
  return ret;
}
static lsn_t flushedLSNInternal(stasis_log_safe_writes_state* sw) {
  readlock(sw->flushedLSN_latch, 0);
  lsn_t ret = sw->flushedLSN_internal;
  readunlock(sw->flushedLSN_latch);
  return ret;
}

static int close_LogWriter(stasis_log_t* log) {
  stasis_log_safe_writes_state* sw = log->impl;
  /* Get the whole thing to the disk before closing it. */
  syncLog_LogWriter(log, LOG_FORCE_WAL);

  fclose(sw->fp);
  close(sw->ro_fd);

  /* Free locks. */
  deletelock(sw->flushedLSN_latch);
  pthread_mutex_destroy(&sw->read_mutex);
  pthread_mutex_destroy(&sw->write_mutex);
  pthread_mutex_destroy(&sw->nextAvailableLSN_mutex);
  pthread_mutex_destroy(&sw->truncate_mutex);
  free(sw->buffer);

  free((void*)sw->filename);
  free((void*)sw->scratch_filename);
  free(sw);
  free(log);
  return 0;
}

static const LogEntry * readLSNEntry_LogWriter(stasis_log_t * log, const lsn_t LSN) {
  stasis_log_safe_writes_state* sw = log->impl;

  LogEntry * ret;

  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);

  if(LSN >= sw->nextAvailableLSN) {
    pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);
    return 0;
  }
  pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);

  pthread_mutex_lock(&sw->read_mutex);

  /** Because we use two file descriptors to access the log, we need
      to flush the log write buffer before concluding we're at EOF. */
  if(flushedLSNInternal(sw) <= LSN) { // && LSN < nextAvailableLSN) {
    syncLogInternal(sw);
    assert(flushedLSNInternal(sw) > LSN);
  }

  assert(sw->global_offset <= LSN);

  off_t newPosition = LSN - sw->global_offset;
  newPosition = lseek(sw->ro_fd, newPosition, SEEK_SET);

  if(newPosition == -1) {
    perror("Could not seek for log read");
    abort();
  }
  assert(newPosition == LSN-sw->global_offset);

  ret = readLogEntry(sw);

  assert(ret);

  pthread_mutex_unlock(&sw->read_mutex);

  return ret;

}
/**
   Truncates the log file.  In the single-threaded case, this works as
   follows:

   First, the LSN passed to this function, minus sizeof(lsn_t) is
   written to a new file, called logfile.txt~.  (If logfile.txt~
   already exists, then it is truncated.)

   Next, the contents of the log, starting with the LSN passed into
   this function are copied to logfile.txt~

   Finally, logfile.txt~ is moved on top of logfile.txt

   As long as the move system call is atomic, this function should
   maintain the system's durability.

   The multithreaded case is a bit more complicated, as we need
   to deal with latching:

   With no lock, copy the log.  Upon completion, if the log has grown,
   then copy the part that remains.  Next, obtain a read/write latch
   on the logfile, and copy any remaining portions of the log.
   Perform the move, and release the latch.

*/
static int truncateLog_LogWriter(stasis_log_t* log, lsn_t LSN) {
  stasis_log_safe_writes_state* sw = log->impl;

  FILE *tmpLog;

  const LogEntry * le;
  LogHandle* lh;

  lsn_t size;

  pthread_mutex_lock(&sw->truncate_mutex);

  if(sw->global_offset + sizeof(lsn_t) >= LSN) {
    /* Another thread beat us to it...the log is already truncated
       past the point requested, so just return. */
    pthread_mutex_unlock(&sw->truncate_mutex);
    return 0;
  }

  /* w+ = truncate, and open for writing. */
  tmpLog = fopen(sw->scratch_filename, "w+");

  if (tmpLog==NULL) {
    pthread_mutex_unlock(&sw->truncate_mutex);
    perror("logTruncate() couldn't create scratch log file!");
    abort();
    return LLADD_IO_ERROR;
  }

  /* Need to write LSN - sizeof(lsn_t) to make room for the offset in
     the file.  If we truncate to lsn 10, we'll put lsn 10 in position
     4, so the file offset is 6. */
  LSN -= sizeof(lsn_t);

  myFwrite(&LSN, sizeof(lsn_t), tmpLog);

  LSN += sizeof(lsn_t);

  /**
     @todo truncateLog blocks writers too early.  Instead, read until
           EOF, then lock, and then finish the truncate.
  */
  pthread_mutex_lock(&sw->write_mutex);

  fflush(sw->fp);

  lh = getLSNHandle(log, LSN);
  lsn_t lengthOfCopiedLog = 0;
  int firstInternalEntry = 1;
  lsn_t nextLSN = 0;
  while((le = nextInLog(lh))) {
    size = sizeofLogEntry(log, le);
    if(nextLSN) {
      assert(nextLSN == le->LSN);
    }
    nextLSN = nextEntry_LogWriter(log, le);

    // zero out crc of first entry during copy
    if(firstInternalEntry && le->type == INTERNALLOG) {
      LogEntry * firstCRC = malloc(size);
      memcpy(firstCRC, le, size);
      freeLogEntry(le);
      firstCRC->prevLSN = 0;
      le = firstCRC;
    }

    lengthOfCopiedLog += (size + sizeof(lsn_t));

    myFwrite(&size, sizeof(lsn_t), tmpLog);
    myFwrite(le, size, tmpLog);
    if(firstInternalEntry && le->type == INTERNALLOG) {
      free((void*)le); // remove const qualifier + free
      firstInternalEntry = 0;
    } else {
      freeLogEntry(le);
    }
  }
  freeLogHandle(lh);
  LogEntry * crc_entry = log_crc_dummy_entry();

  pthread_mutex_lock(&sw->nextAvailableLSN_mutex);
  crc_entry->LSN = sw->nextAvailableLSN;
  DEBUG("Crc entry: lsn = %ld, crc = %x\n", crc_entry->LSN,
        crc_entry->prevLSN);

  assert(sw->nextAvailableLSN == LSN + lengthOfCopiedLog);

  size = sizeofLogEntry(log, crc_entry);

  sw->nextAvailableLSN = nextEntry_LogWriter(log, crc_entry);

  log_crc_reset(sw);

  pthread_mutex_unlock(&sw->nextAvailableLSN_mutex);

  myFwrite(&size, sizeof(lsn_t), tmpLog);
  myFwrite(crc_entry, size, tmpLog);
  lengthOfCopiedLog += (size + sizeof(lsn_t));

  assert(sw->nextAvailableLSN == (LSN + lengthOfCopiedLog));
  free(crc_entry);

  fflush(tmpLog);
#ifdef HAVE_FDATASYNC
  fdatasync(fileno(tmpLog));
#else
  fsync(fileno(tmpLog));
#endif

  /** Time to shut out the readers */

  pthread_mutex_lock(&sw->read_mutex);

  /* closeLogWriter calls sync, and does some extra stuff that we
     don't want, so we basicly re-implement closeLogWriter and
     openLogWriter here...
  */
  fclose(sw->fp);
  close(sw->ro_fd);
  fclose(tmpLog);

  if(rename(sw->scratch_filename, sw->filename)) {
    pthread_mutex_unlock(&sw->read_mutex);
    pthread_mutex_unlock(&sw->write_mutex);
    pthread_mutex_unlock(&sw->truncate_mutex);

    perror("Error replacing old log file with new log file");
    return LLADD_IO_ERROR;
  } else {
    DEBUG("Truncation complete.\n");
    fflush(stdout);
  }

  int logFD = open(sw->filename, sw->filemode, sw->fileperm);

  if(logFD == -1) {
    perror("Couldn't open log file for append.\n");
    abort();
  }
  sw->fp = fdopen(logFD, "w+");

  if (sw->fp==NULL) {
    pthread_mutex_unlock(&sw->read_mutex);
    pthread_mutex_unlock(&sw->write_mutex);
    pthread_mutex_unlock(&sw->truncate_mutex);

    perror("Couldn't reopen log after truncate");
    abort();
    return LLADD_IO_ERROR;
  }

  setbuffer(sw->fp, sw->buffer, stasis_log_file_write_buffer_size);

  sw->global_offset = LSN - sizeof(lsn_t);

  lsn_t logPos;
  if((logPos = myFseek(sw->fp, 0, SEEK_END))
     != sw->nextAvailableLSN - sw->global_offset) {
    if(logPos == -1) {
      perror("Truncation couldn't seek");
    } else {
      printf("logfile was wrong length after truncation.  "
             "Expected %lld, found %lld\n",
             sw->nextAvailableLSN - sw->global_offset, logPos);
      fflush(stdout);
      abort();
    }
  }

  sw->ro_fd = open(sw->filename, O_RDONLY, 0);

  if(sw->ro_fd == -1) {
    perror("Couldn't open log file for reads.\n");
    abort();
    return LLADD_IO_ERROR;
  }

  pthread_mutex_unlock(&sw->read_mutex);
  pthread_mutex_unlock(&sw->write_mutex);
  pthread_mutex_unlock(&sw->truncate_mutex);

  return 0;

}

static lsn_t firstLogEntry_LogWriter(stasis_log_t* log) {
  stasis_log_safe_writes_state* sw = log->impl;

  assert(sw->fp);
  pthread_mutex_lock(&sw->read_mutex); // for global offset...
  lsn_t ret = sw->global_offset + sizeof(lsn_t);
  pthread_mutex_unlock(&sw->read_mutex);
  return ret;
}

static void setTruncation_LogWriter(stasis_log_t* log, stasis_truncation_t *trunc) {
  // logwriter does not support hard limits on its size, so this is a no-op
}

stasis_log_t* stasis_log_safe_writes_open(const char * filename,
                                          int filemode, int fileperm, int softcommit) {

  stasis_log_t proto = {
    setTruncation_LogWriter,
    sizeofInternalLogEntry_LogWriter, // sizeof_internal_entry
    writeLogEntry_LogWriter,// write_entry
    readLSNEntry_LogWriter, // read_entry
    nextEntry_LogWriter,// next_entry
    flushedLSN_LogWriter, // first_unstable_lsn
    nextAvailableLSN_LogWriter, // newt_available_lsn
    syncLog_LogWriter, // force_tail
    truncateLog_LogWriter, // truncate
    firstLogEntry_LogWriter,// truncation_point
    close_LogWriter, // deinit
    isDurable_LogWriter, // is_durable
  };

  stasis_log_safe_writes_state * sw = malloc(sizeof(*sw));
  sw->filename = strdup(filename);
  {
    char * log_scratch_filename = malloc(strlen(sw->filename) + 2);
    strcpy(log_scratch_filename, sw->filename);
    strcat(log_scratch_filename, "~");
    sw->scratch_filename = log_scratch_filename;
  }
  sw->filemode = filemode;
  sw->fileperm = fileperm;
  sw->softcommit = softcommit;

  stasis_log_t* log = malloc(sizeof(*log));
  memcpy(log,&proto, sizeof(proto));
  log->impl = sw;

  sw->buffer = calloc(stasis_log_file_write_buffer_size, sizeof(char));

  if(!sw->buffer) { return 0; /*LLADD_NO_MEM;*/ }

  /* The file is opened twice for a reason.  fseek() seems to call
     fflush() under Linux, which normally would be a minor problem.
     However, we open the log with O_SYNC, so the fflush() results in
     synchronous disk writes.  Therefore, all read accesses (and
     therefore all seeks) run through the second descriptor.  */

  int logFD;
  logFD = open(sw->filename, sw->filemode, sw->fileperm);
  if(logFD == -1) {
    perror("Couldn't open log file for append.\n");
    abort();
  }
  sw->fp = fdopen(logFD, "w+");

  if (sw->fp==NULL) {
    perror("Couldn't open log file");
    abort();
    return 0; //LLADD_IO_ERROR;
  }

  /* Increase the length of log's buffer, since it's in O_SYNC mode. */
  setbuffer(sw->fp, sw->buffer, stasis_log_file_write_buffer_size);

  /* fread() doesn't notice when another handle writes to its file,
     even if fflush() is used to push the changes out to disk.
     Therefore, we use a file descriptor, and read() instead of a FILE
     and fread(). */
  sw->ro_fd = open(sw->filename, O_RDONLY, 0);

  if(sw->ro_fd == -1) {
    perror("Couldn't open log file for reads.\n");
  }

  /* Initialize locks. */

  sw->flushedLSN_latch = initlock();
  pthread_mutex_init(&sw->read_mutex, NULL);
  pthread_mutex_init(&sw->write_mutex, NULL);
  pthread_mutex_init(&sw->nextAvailableLSN_mutex, NULL);
  pthread_mutex_init(&sw->truncate_mutex, NULL);

  sw->flushedLSN_wal = 0;
  sw->flushedLSN_commit = 0;
  sw->flushedLSN_internal = 0;
  /*
    Seek append only log to the end of the file.  This is unnecessary,
    since the file was opened in append only mode, but it returns the
    length of the file.
  */

  if (myFseek(sw->fp, 0, SEEK_END)==0) {
    /*if file is empty, write an LSN at the 0th position.  LSN 0 is
      invalid, and this prevents us from using it.  Also, the LSN at
      this position is used after log truncation to store the
      global offset for the truncated log.
    */
    sw->global_offset = 0;
    size_t nmemb = fwrite(&sw->global_offset, sizeof(lsn_t), 1, sw->fp);
    if(nmemb != 1) {
      perror("Couldn't start new log file!");
      return 0; //LLADD_IO_ERROR;
    }
  } else {

    off_t newPosition = lseek(sw->ro_fd, 0, SEEK_SET);
    if(newPosition == -1) {
      perror("Could not seek to head of log");
      return 0; //LLADD_IO_ERROR;
    }

    ssize_t bytesRead = read(sw->ro_fd, &sw->global_offset, sizeof(lsn_t));

    if(bytesRead != sizeof(lsn_t)) {
      printf("Could not read log header.");
      return 0;//LLADD_IO_ERROR;
    }

  }

  // Initialize nextAvailableLSN so readLogEntry() will work.
  sw->nextAvailableLSN =  sizeof(lsn_t) + sw->global_offset;

  if(lseek(sw->ro_fd, sizeof(lsn_t), SEEK_SET) != sizeof(lsn_t)) {
    perror("Couldn't seek to first log entry!");
  }

  // find first lsn after last valid crc.
  sw->nextAvailableLSN = log_crc_next_lsn(log, sw->nextAvailableLSN);

  if(ftruncate(fileno(sw->fp), sw->nextAvailableLSN-sw->global_offset) == -1) {
    perror("Couldn't discard junk at end of log");
  }

  // If there was trailing garbage at the end of the log, overwrite it.
  if(myFseek(sw->fp, sw->nextAvailableLSN-sw->global_offset, SEEK_SET)
     != sw->nextAvailableLSN-sw->global_offset) {
    perror("Error repositioning log");
    abort();
  }

  // Reset log_crc to zero (nextAvailableLSN immediately follows a crc entry).
  log_crc_reset(sw);

  sw->flushedLSN_wal      = sw->nextAvailableLSN;
  sw->flushedLSN_commit   = sw->nextAvailableLSN;
  sw->flushedLSN_internal = sw->nextAvailableLSN;

  return log;
}

void stasis_log_safe_writes_delete(const char* log_filename) {
  remove(log_filename);
}
