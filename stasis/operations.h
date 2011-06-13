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
 * Interface for defining new logical operations.
 *
 * @ingroup OPERATIONS
 * @todo The functions in operations.h don't belong in the API, but it defines some constants and typedefs that should be there.
 * $Id$
 */
/**
   @defgroup COLLECTIONS Collections

   Stasis provides a number of general-purpose data structures for use
   by applications.

   Stasis provides a number of general-purpose data structures for use
   by applications.  This section documents these data structures and
   associated interfaces, such as iterators.
*/


#ifndef __OPERATIONS_H__
#define __OPERATIONS_H__

#include <stasis/common.h>
//#include <stasis/transactional.h>
#include <stasis/logger/logEntry.h>
//#include <stasis/bufferManager.h>
//#include <stasis/iterator.h>
//#include <stasis/arrayCollection.h>
BEGIN_C_DECLS


/**
 * function pointer that the operation will run
 */
typedef int (*Function)(const LogEntry* e, Page * p);

typedef struct {
  /**
   * ID of operation, also index into operations table
   */
  int id;
  /**
   * The type of the page this operation modifies.  If UNKNWON_TYPE_PAGE,
   * then the page's header will be trusted.  If UNINITIALIZED_PAGE, then
   * the contents of the in-memory version of the page will be undefined
   * before the operation is applied.  (This is used for allocation).
   */
  pagetype_t page_type;
  /**
   * ID of redo operation; logical operations typically
   * set this to OPERATION_NOOP.
   */
  int redo;
  /**
     Implementing operations that may span records is subtle.
     Recovery assumes that page writes (and therefore logical
     operations) are atomic.  This isn't the case for operations that
     span records.  Instead, there are two (and probably other) choices:

      - Periodically checkpoint, syncing the data store to disk, and
        writing a checkpoint operation.  No writes can be serviced
        during the sync, and this implies 'no steal'.  See:

        @@inproceedings{ woo97accommodating,
	author = "Seung-Kyoon Woo and Myoung-Ho Kim and Yoon-Joon Lee",
	title = "Accommodating Logical Logging under Fuzzy Checkpointing in Main Memory Databases",
	booktitle = "International Database Engineering and Application Symposium",
	pages = "53-62",
	year = "1997",
	url = "citeseer.ist.psu.edu/135200.html" }

	for a more complex scheme involving a hybrid logical/physical
	logging system that does not implement steal.

	The other option:

      - Get rid of operations that span records entirely by
        splitting complex logical operations into simpler ones.

	We chose the second option for now.  This implies that the
	entries must be written to the log in an order, that if
	repeated, guarantees that the structure will be in a logically
	consistent state after the REDO phase, regardless of what
	prefix of the log actually makes it to disk.  Note that
	pinning pages before the log entry hits disk is inadequate, in
	general, since other transactions could read dirty information
	from the pinned pages, producsing nonsensical log entries that
	preceed the current transaction's log entry.
   */
  /**
     index into operations table of undo function
  */
  int undo;
  Function run;
} stasis_operation_impl;

/* These need to be installed, since they are required by applications that use LLADD. */

#include "operations/increment.h"
#include "operations/decrement.h"
#include "operations/set.h"
#include "operations/prepare.h"
#include "operations/lladdhash.h"
#include "operations/alloc.h"
#include "operations/pageOperations.h"
#include "operations/noop.h"
#include "operations/arrayList.h"
#include "operations/naiveLinearHash.h"
#include "operations/linkedListNTA.h"
#include "operations/pageOrientedListNTA.h"
#include "operations/linearHashNTA.h"
#include "operations/bTree.h"
#include "operations/regions.h"
#include "operations/lsnFreeSet.h"
#include "operations/segmentFile.h"

/**
   Initialize stasis' operation table.
 */
void stasis_operation_table_init();

/**
   Register a new logical or physical operation (redo/undo functions)
   with Stasis.  This function must be called before Tinit().

   If you register custom operations, then you must call
   stasis_operation_table_init() before calling this function.  Otherwise,
   there is no need to manutally call stasis_operations_table_init().
 */
void stasis_operation_impl_register(stasis_operation_impl o);

/**
    Performs an operation during normal execution.

    Does not write to the log, and assumes that the operation's
    results are not already in the buffer manager.

    @param e the UPDATELOG entry to play forward, regardless of lsn's

    @param p the page the update should be applied to (no support for
             logical redo).  p->rwlatch should be writelock()'ed
 */
void stasis_operation_do(const LogEntry * e, Page * p);
/**
    Undo the update under normal operation, and during recovery.

    For logical undo, this unconditionally executes the requested operation.

    For physical undo, this compares the page LSN to clr_lsn, and runs
        it if the page is out of date.

    @param e The UPDATELOG entry containing the operation to be undone.
    @param clr_lsn The lsn of the clr that records this undo operation.
    @param p Like doUpdate(), this function is called during forward operation,
             so p->rwlatch must be writelock()'ed
*/
void stasis_operation_undo(const LogEntry * e, lsn_t clr_lsn,
					Page * p);
/**
    Redoes an operation during recovery.  This is different than
    doUpdate because it checks to see if the operation needs to be redone
    before redoing it. (if(e->lsn > e->rid.lsn) { doUpdate(e); } return)

    It also invokes the log entry's REDO method instead of the DO method.

    Does not write to the log.  No need for a page parameter; Stasis'
    recovery is single-threaded, so redoUpdate can latch the page itself.

    @param e The UPDATELOG entry containing the operation to be redone.
    @param p The page the redo should be applied to.  Must be writelock()'ed.
*/
void stasis_operation_redo(const LogEntry * e, Page * p);

/**
 * Decides whether or not the entry e should be applied to p, assuming that
 * e's operation is of type MULTI_PAGE.  One such entry applies to many pages, so the
 * operation implementation's callback must perform the test.
 *
 * If segments are enabled, this function always returns true (under the assumption
 * that the multi page operation is a allocation routine).  Otherwise, it compares
 * the page header's LSN with the entry's LSN.
 *
 */
int stasis_operation_multi_should_apply(const LogEntry *e, Page * p);

pagetype_t stasis_operation_type(int op);

END_C_DECLS

#endif
