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

#include <stasis/logger/logger2.h>

#ifndef __LOGHANDLE_H
#define __LOGHANDLE_H

BEGIN_C_DECLS

typedef struct LogHandle LogHandle;

/**
   @file

   Iterator for forward and reverse iterations over the log.  Forward
   iterations are used for redo and return each log entry in log
   order.  Reverse iterations are used for undo, and return each entry
   in a particular transaction.  They follow the prevLSN field,
   skipping any undo entries that have been marked complete.

   @see logWriter.h For write access to the log.
   @see logger.h For the api provided by log implementations.
*/

/**
   Allocate a logHandle pointing at the first log entry in the log.
*/
LogHandle* getLogHandle(stasis_log_t* log);
/**
   Allocate a logHandle pointing at a particular lsn.
*/
LogHandle* getLSNHandle(stasis_log_t* log, lsn_t lsn);
/**
   Free any resources associated with a LogHandle object.
*/
void freeLogHandle(LogHandle* lh);

/**
    @return a pointer to the next log entry in the log, or NULL if at
    EOF.
 */
const LogEntry * nextInLog(LogHandle * h);
/**
    Returns a pointer to the previous log entry in this transaction.

    If we encounter a CLR, that means that everything after the clr's
    prevLSN has already been undone.  Therefore, we skip all
    intervening log entries (including CLR's), since they contain
    obsolete data.

    @return NULL if there is no previous LogEntry for this transaction
 */
const LogEntry * previousInTransaction(LogHandle * h);

END_C_DECLS

#endif
