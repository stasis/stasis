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

#include "logEntry.h"
#include "logWriter.h"

#ifndef __LOGHANDLE_H
#define __LOGHANDLE_H

BEGIN_C_DECLS

/**
   @file 
   A simple data structure that allows forward iterations over
   the log, and also allows reverse iterations.  Forward iterations
   are used for redo, and return every log entry, in its original
   order.  Reverse iterations are used for undo, and are transaction
   specific.  They follow the prevLSN. (or the next entry to be
   undone stored in any CLR's that are encountered.)

   logHandle is useful for read only access to the log.

   @see logWriter.h For write access to the log.
*/

typedef int (guard_fcn_t)(LogEntry *, void *);

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

/** Returns a logHandle pointing at the first log entry in the log.  */
LogHandle getLogHandle();
/** Returns a logHandle pointing at lsn. */
LogHandle getLSNHandle(lsn_t lsn);
/** Returns a 'guarded log handle'.  This handle executes a callback
    function on each entry it encounters.  If the guard returns 0,
    then it's iterator terminates.  Otherwise, it behaves normally. */
LogHandle getGuardedHandle(lsn_t lsn, guard_fcn_t * f, void * guard_state);

/** 
    @return a pointer to the next log entry in the log, or NULL if at
    EOF.

 */
LogEntry * nextInLog(LogHandle * h);
/** 
    Returns a pointer to the previous log entry in this
    transaction. This is used to undo transactions.  If the logHandle
    is a guarded handle, the handle returns null.  

    The guard is useful for Tprepare, partial rollback, and probably
    any reasonable lock manager implementations.

    If we encounter a CLR, that means that everything after the clr's
    undoNextLSN has already been undone.  In that case, we can skip
    all intervening log entries (including CLR's), since they contain
    'stale' data.  

    @return NULL if there is no previous LogEntry for this
    transaction, or if the guard indicates that we're done by returning 0.
 */
LogEntry * previousInTransaction(LogHandle * h);
/*
void closeHandle(LogHandle h);
*/
END_C_DECLS

#endif
