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
 * @defgroup LLADD_CORE  Core API
 *
 * The minimal subset of LLADD necessary to implement transactional consistency.
 *
 * This module includes the standard API (excluding operations), the
 * logger, the buffer mananger, and recovery code.
 *
 * In theory, the other .h files that are installed in /usr/include
 * aren't needed for application developers.
 *
 * @todo Move as much of the stuff in lladd/ to src/lladd/ as possible.
 *
 */
/**
 * @defgroup OPERATIONS  Logical Operations
 *
 * Implementations of logical operations, and the interfaces that allow new operations to be added.
 *
 * @todo Write a brief howto to explain the implementation of new operations.
 *
 */

/**
 * @file 
 *
 * Defines LLADD's primary interface.
 *
 *
 *
 * @todo error handling 
 *
 * @ingroup LLADD_CORE
 * $Id$
 */




#ifndef __TRANSACTIONAL_H__
#define __TRANSACTIONAL_H__

#include "common.h"

BEGIN_C_DECLS

/**
 * represents how to look up a record on a page
 */
typedef struct {
  int page;
  int slot;
  long size;
} recordid;

extern const recordid ZERO_RID;


/**
   If a recordid's slot field is set to this, then the recordid
   represents an array of fixed-length records starting at slot zero
   of the recordid's page.

   @todo Support read-only arrays of variable length records, and then
   someday read / write / insert / delete arrays...
*/
#define RECORD_ARRAY (-1)


#include "operations.h"

/**
 * Currently, LLADD has a fixed number of transactions that may be
 * active at one time.
 */
#define EXCEED_MAX_TRANSACTIONS 1

/**
 * @param xid transaction ID
 * @param LSN last log that this transaction used
 */
/*  @param status @ todo Undocumented.  (Unused?)
 */
typedef struct {
	int xid;
	long LSN;
  /*	int status; */
} Transaction;



/**
 * initialize the transactional system, including running recover (if
 * necessary), building the operations_table, and opening the logs
 * @return 0 on success
 * @throws error code on error
 */
int Tinit();

/**
 * @return positive transaction ID on success, negative return value on error
 */
int Tbegin();

/**
 * Used when extending LLADD.
 * Operation implementors should wrap around this function to provide more mnuemonic names.
 *
 * @param xid          The current transaction.
 * @param rid          The record the operation pertains to.  For some logical operations, this will be a dummy record.
 * @param dat          Application specific data to be recorded in the log (for undo/redo), and to be passed to the implementation of op.
 * @param op           The operation's offset in operationsTable
 *
 * @see operations.h set.h
 */
void Tupdate(int xid, recordid rid, const void *dat, int op);

/**
 * @param xid transaction ID
 * @param rid reference to page/slot
 * @param dat buffer into which data goes
 */
void Tread(int xid, recordid rid, void *dat);

/**
 * @param xid transaction ID
 * @return 0 on success
 * @throws error vallue on error
 */
int Tcommit(int xid);

/**
 * @param xid The current transaction
 * @param size The size, in bytes of the new record you wish to allocate
 * @returns A new recordid.  On success, this recordid's size will be 
 *          the requested size.  On failure, its size will be zero.
 */
recordid Talloc(int xid, long size);

/* @function Tabort
 * @param xid transaction ID
 * @return 0 on success, -1 on error.
 */
int Tabort(int xid);

/**
 * flushes all pages, cleans up log
 * @return 0 on success
 * @throws error value on error
 */
int Tdeinit();

/**
 *  Used by the recovery process.
 *  Revives Tprepare'ed transactions.
 *
 * @param xid  The xid that is to be revived. 
 * @param lsn  The lsn of that xid's most recent PREPARE entry in the log.
 */
void Trevive(int xid, long lsn);

/**
 *  Used by the recovery process. 
 *
 *  Sets the number of active transactions. 
 *  Should not be used elsewhere.
 *
 * @param xid  The new active transaction count. 
 */
void TsetXIDCount(int xid);

END_C_DECLS

#endif
