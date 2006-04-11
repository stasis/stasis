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
 * Prepare a transaction to commit so that it will persist
 * across system crashes.  After recovery, the transaction will be in 
 * the same state it was in when Tprepare() was called.
 *
 * Tprepare() uses the operation interface to abstract away log handling.
 * It would be nice if the logger API could be simplified by having
 * more of its functionality handled this way.
 *
 Implementation notes:
 
 - Just a Tupdate, with a log flush as its operationsTable[]
 function.
 
 - After recovery, all of the xacts pages will have been 'stolen',
 (if recovery flushes dirty pages)
 
 - Recovery function needs to distinguish between actions before and
 after the last Tprepare log entry.  This is handle by a guard on
 the logHandle's iterator, but could be generalized in the future
 (to support savepoints, for example) Right now, recovery uses a
 guarded iterator, transUndo() does not.
 
 @todo Test Tprepare()

 *
 * @ingroup OPERATIONS
 *
 * $Id$
 * 
 *
 */

#ifndef __PREPARE_H__
#define __PREPARE_H__

//#include <lladd/logger/logEntry.h>

extern recordid prepare_bogus_rec;
/** 
    Prepare transaction for commit.  Currently, a transaction may be
    prepared multiple times.  Once Tprepare() returns, the caller is
    guaranteed that the current transaction will resume exactly where
    it was the last time Tprepare() was called.

    @todo Tprepare() shouldn't take a record or buffer as arguments... 

    @param xid Transaction id.
    @param rec must be a valid record id.  any valid recordid will do.  This parameter will be removed eventually.

*/
#define Tprepare(xid, rec) Tupdate(xid, rec, 0, OPERATION_PREPARE)

Operation getPrepare();

/**
   Recovery's undo phase uses this logHandle iterator guard to implement Tprepare().
*/
int prepareGuard(const LogEntry * e, void * state);
void * getPrepareGuardState();
int prepareAction(void * state);
#endif
