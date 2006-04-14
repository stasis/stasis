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

#include <lladd/operations.h>

/**
   A callback function that allows logHandle's iterator to stop
   returning log entries depending on the context in which it was
   called.
*/
typedef int (guard_fcn_t)(const LogEntry *, void *);

typedef struct { 
  /** The LSN of the last log entry returned.*/
  /*  lsn_t       file_offset; */ /* Unneeded? */
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
  LogHandle lh;
} TransactionLog;

#define LOG_TO_FILE   0
#define LOG_TO_MEMORY 1

extern int loggerType;

int  LogInit(int logType);

int  LogDeinit();


void LogForce(lsn_t lsn);
void LogTruncate(lsn_t lsn);
lsn_t LogFlushedLSN();


lsn_t LogTruncationPoint();

const LogEntry * LogReadLSN(lsn_t lsn);

/**
   Inform the logging layer that a new transaction has begun.
   Currently a no-op.
*/
TransactionLog LogTransBegin(int xid);

/**
  Write a transaction COMMIT to the log tail, then flush the log tail immediately to disk

  @return The lsn of the commit log entry.  
*/
lsn_t LogTransCommit(TransactionLog * l);

/**
  Write a transaction ABORT to the log tail

  @return The lsn of the abort log entry.
*/
lsn_t LogTransAbort(TransactionLog * l);

/**
  LogUpdate writes an UPDATE log record to the log tail
*/
LogEntry * LogUpdate(TransactionLog * l, Page * p, recordid rid, int operation, const byte * args);

/**
   Whenever a LogEntry is returned by a function that is defined by
   logger2.h or logHandle.h, the caller should eventually call this
   function to release any resources held by that entry.
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
//lsn_t LogCLR (LogEntry * undone);
lsn_t LogCLR(int xid, lsn_t LSN, recordid rid, lsn_t prevLSN);

/**
   Write a end transaction record @see XEND

   @todo Implement LogEnd
*/
void LogEnd (TransactionLog * l);

/** 
  (For internal use only..)
*/
void genericLogWrite(LogEntry * e);

#endif
