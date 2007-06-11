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
 * @file
 *
 * New version of logger.  Based on logger.h
 * 
 * $Id$
 * 
 */


#ifndef __LOGGER2_H__
#define __LOGGER2_H__

#include <stasis/operations.h>

/**
   A callback function that allows logHandle's iterator to stop
   returning log entries depending on the context in which it was
   called.
*/
typedef int (guard_fcn_t)(const LogEntry *, void *);

typedef struct { 
  /** The LSN of the log entry that we would return if next is called. */
  lsn_t       next_offset; 
  /** The LSN of the log entry that we would return if previous is called. */
  lsn_t       prev_offset;
  guard_fcn_t * guard;
  void * guard_state;
} LogHandle;

/**
   Contains the state needed by the logging layer to perform
   operations on a transaction.
 */
typedef struct {
  int xid; 
  lsn_t prevLSN;
  lsn_t recLSN;
} TransactionLog;

/** 
    This is the log implementation that is being used.  
    
    Before Stasis is intialized it will be set to a default value.    
    It may be changed before Tinit() is called by assigning to it.
    The default can be overridden at compile time by defining
    USE_LOGGER.

    (eg: gcc ... -DUSE_LOGGER=LOG_TO_FOO)

    @see constants.h for a list of recognized log implementations. 
         (The constants are named LOG_TO_*)

 */
extern int loggerType;

int  LogInit(int logType);
int  LogDeinit();
void LogForce(lsn_t lsn);
/** 
    @param lsn The first lsn that will be available after truncation.
*/ 
void LogTruncate(lsn_t lsn);

/** This function is guaranteed to return the LSN of the most recent
    log entry that has not been flushed to disk.  (If the entire log
    is flushed, this function returns the LSN of the entry that will
    be allocated the next time the log is appended to. */
lsn_t LogFlushedLSN();
/** Returns the LSN of the first entry of the log, or the LSN of the
    next to be allocated if the log is empty) */
lsn_t LogTruncationPoint();
/** Read a log entry, given its LSN.
    @param lsn  The lsn of the log entry to be read.
*/
const LogEntry * LogReadLSN(lsn_t lsn);
/**
   Given a log entry, return the LSN of the next entry.
*/
lsn_t LogNextEntry(const LogEntry * e);

/**
   Inform the logging layer that a new transaction has begun, and
   obtain a handle.
*/
TransactionLog LogTransBegin(int xid);

/**
  Write a transaction COMMIT to the log tail.  Blocks until the commit
  record is stable.

  @return The lsn of the commit log entry.  
*/
lsn_t LogTransCommit(TransactionLog * l);

/**
  Write a transaction ABORT to the log tail.

  @return The lsn of the abort log entry.
*/
lsn_t LogTransAbort(TransactionLog * l);

/**
  LogUpdate writes an UPDATELOG log record to the log tail.  It also interprets
  its operation argument to the extent necessary for allocating and laying out 
  the log entry.  Finally, it updates the state of the parameter l.
*/
LogEntry * LogUpdate(TransactionLog * l, Page * p, recordid rid, int operation,
		     const byte * args);
/**
  LogDeferred writes a DEFERLOG log record to the log tail

  @see LogUpdate is analagous to this function, but wrutes UPDATELOG entries 
       instead.
*/
LogEntry * LogDeferred(TransactionLog * l, Page * p, recordid rid, 
		       int operation, const byte * args);

/**
   Any LogEntry that is returned by a function in logger2.h or
   logHandle.h should be freed using this function.

   @param e The log entry to be freed.  (The "const" here is a hack
            that allows LogReadLSN to return a const *.
*/
void FreeLogEntry(const LogEntry * e);

/**
   Write a compensation log record.  These records are used to allow
   for efficient recovery, and possibly for log truncation.  They
   record the completion of undo operations, amongst other things.

   @return the lsn of the CLR entry that was written to the log.
           (Needed so that the lsn slot of the page in question can be
           updated.)
*/
lsn_t LogCLR(const LogEntry * e);

lsn_t LogDummyCLR(int xid, lsn_t prevLSN);

/**
   Write a end transaction record @see XEND

   @todo Implement LogEnd
*/
void LogEnd (TransactionLog * l);

/**
   Needed by sizeofLogEntry
*/
long LoggerSizeOfInternalLogEntry(const LogEntry * e);

/** 
   For internal use only...  This would be static, but it is called by
   the test cases.
*/
void LogWrite(LogEntry * e);

#endif
