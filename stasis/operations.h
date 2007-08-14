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
 * @ingroup LLADD_CORE OPERATIONS
 * @todo The functions in operations.h don't belong in the API, but it defines some constants and typedefs that should be there.
 * $Id$
 */

#ifndef __OPERATIONS_H__
#define __OPERATIONS_H__

#include <stasis/constants.h>
#include <stasis/transactional.h>
#include <stasis/logger/logEntry.h> 
#include <stasis/bufferManager.h>
#include <stasis/iterator.h>
#include <stasis/arrayCollection.h>
BEGIN_C_DECLS


/** 
 * function pointer that the operation will run
 */
typedef int (*Function)(int xid, Page * p, lsn_t lsn, recordid r, const void *d);

/**

*/

/**
   If the Operation struct's sizeofData is set to this value, then the
   size field of the recordid is used to determine the size of the
   argument passed into the operation.
 */
#define SIZEOF_RECORD -1
/** 
    Logical log entries (such as those used by nested top actions
    have a null recordid, as they are not assoicated with a specific page
    
    If a log entry is not associated with a specific page, the page id can 
    be overloaded to hold the size of the associated log entry.  Contrast
    this with the description of SIZEOF_RECORD, which is used when the
    operation uses a variable length argument, but is associated with 
    a specfic page.
*/
#define SIZEIS_PAGEID -2
/** If the Operation struct's undo field is set to this value, then
    physical logging is used in lieu of logical logging.
 */

#define NO_INVERSE -1
typedef struct {
  /**
   * ID of operation, also index into operations table
   */
	int id;
  /** 
      This value is the size of the arguments that this operation
      takes.  If set to SIZEOF_RECORD, then the size of the record
      that the operation affects will be used instead.
  */
	long sizeofData;
  /**
     Implementing operations that may span records is subtle.
     Recovery assumes that page writes (and therefore logical
     operations) are atomic.  This isn't the case for operations that
     span records.  Instead, there are two (and probably other) choices:

      - Periodically checkpoint, syncing the data store to disk, and
        writing a checkpoint operation.  No writes can be serviced
        during the sync, and this implies 'no steal'.  See: 

        @inproceedings{ woo97accommodating,
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
} Operation;

/* These need to be installed, since they are required by applications that use LLADD. */

#include "operations/increment.h"
#include "operations/decrement.h"
#include "operations/set.h"
#include "operations/prepare.h"
#include "operations/lladdhash.h"
#include "operations/alloc.h"
#include "operations/pageOperations.h"
#include "operations/noop.h"
#include "operations/instantSet.h"
#include "operations/arrayList.h"
#include "operations/linearHash.h"
#include "operations/bTree.h"
#include "operations/naiveLinearHash.h"
#include "operations/nestedTopActions.h"
#include "operations/linkedListNTA.h"
#include "operations/pageOrientedListNTA.h"
#include "operations/linearHashNTA.h"
#include "operations/regions.h"
#include "operations/lsmTree.h"

extern Operation operationsTable[]; /* [MAX_OPERATIONS];  memset somewhere */

/** Performs an operation during normal execution. 

    Does not write to the log, and assumes that the operation's
    results are not already in the buffer manager.
*/
void doUpdate(const LogEntry * e, Page * p);
/** Undo the update under normal operation, and during recovery. 

    Checks to see if the operation's results are reflected in the
    contents of the buffer manager.  If they are, then it performs the
    undo.  

    Does not write to the log.

    This function does not generate CLR because this would result in
    extra CLRs being generated during recovery.

    @param e The log entry containing the operation to be undone.  
    @param p A pointer to the memory resident copy of the page that is being managed by bufferManager.
    @param clr_lsn The lsn of the clr that corresponds to this undo operation.
*/
void undoUpdate(const LogEntry * e, Page * p, lsn_t clr_lsn);
/** 
    Redoes an operation during recovery.  This is different than
    doUpdate because it checks to see if the operation needs to be redone
    before redoing it. (if(e->lsn > e->rid.lsn) { doUpdate(e); } return)

    Also, this is the only function in operations.h that can take
    either CLR or UPDATE log entries.  The other functions can     handle update entries.

    Does not write to the log.
*/
void redoUpdate(const LogEntry * e);

END_C_DECLS

#endif
