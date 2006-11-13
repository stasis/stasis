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
 * The minimal subset of Stasis necessary to implement transactional consistency.
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
   @mainpage Introduction to Stasis
   
   @section compiling Compiling and installation
   
   Prerequisites:
   
   - automake 1.8+: needed to build from CVS 
   - <a href="http://check.sourceforge.net">check</a>: A unit testing 
     framework (needed to run the self-tests)

   Optional:

   - libconfuse: Used by older networking code to parse configuration options.
   - BerkeleyDB: Used by the benchmarking code for purposes of comparison. 
   
   Development is currently performed under Debian's Testing branch.
   
   To compile Stasis, first check out a copy with CVS:

   @code

   $  cvs -z3 -d:pserver:anonymous@lladd.cvs.sourceforge.net:/cvsroot/lladd co -P lladd

   @endcode

   then:
   
   @code
   
   $ ./reconf
   $ ./configure --quiet
   $ make -j4 > /dev/null
   $ cd test/lladd
   $ make check
   
   @endcode
   
   This will fail if your system defaults to an old (pre-1.7) version
   of autotools.  Fortunately, multiple versions of autotools may
   exist on the same system.  Execute the following commands to
   compile with version 1.8 of autotools:

   @code

   $ ./reconf-1.8
   $ ./configure --quiet
   $ make -j4 > /dev/null
   $ cd test/lladd
   $ make check

   @endcode

   Of course, you can omit "--quiet" and "> /dev/null", but configure
   and make both produce quite a bit of output that may obscure useful
   warning messages.

   'make install' is currently unsupported.  Look in utilities/ for an example of a 
   simple program that uses Stasis.  Currently, most generally useful programs 
   written on top of Stasis belong in lladd/src/apps, while utilities/ contains 
   programs useful for debugging the library.
   
   @section usage Using Stasis in your software
   
   Synopsis:
   
   @code
   
   #include <lladd/transaction.h>
   
   ...
   
   Tinit();
   
   int i = 42;
   
   int xid = Tbegin();
   recordid rid = Talloc(xid, sizeof(int)); 
   Tset(xid, rid, &i);   // the application is responsible for memory management.
                         // Here, stack-allocated integers are used, although memory
			 // from malloc() works as well.
   Tcommit(xid); 

   int j;
  
   xid = Tbegin();
   Tread(xid, rid, &j);  // j is now 42.
   Tdealloc(xid, rid); 
   Tabort(xid); 
   Tdeinit();
   
   @endcode
   
   Hopefully, Tbegin(), Talloc(), Tset(), Tcommit(), Tabort() and Tdealloc() are 
   self explanatory.  If not, they are covered in detail elsewhere.  Tinit() and 
   Tdeinit() initialize the library, and clean up when the program is finished.
   
   Other partiularly useful functions are ThashCreate(), ThashDelete(),
   ThashInsert(), ThashRemove(), and ThashLookup() which provide a
   re-entrant linear hash implementation.  ThashIterator() and
   ThashNext() provide an iterator over the hashtable's values.
   
   @subsection bootstrap Reopening a closed data store
   
   Stasis imposes as little structure upon the application's data structures as 
   possible.  Therefore, it does not maintain any information about the contents
   or naming of objects within the page file.  This means that the application 
   must maintain such information manually.
   
   In order to facilitate this, Stasis provides the function TgetRecordType() and
   guarantees that the first recordid returned by any allocation will point to 
   the same page and slot as the constant ROOT_RECORD.  TgetRecordType 
   will return NULLRID if the record passed to it does not exist.  
   
   Therefore, the following code will safely initialize or reopen a data 
   store:
   
   @code
   Tinit();

   recordid rootEntry;

   int xid = Tbegin();
   if(TrecordType(xid, ROOT_RECORD) == UNINITIALIZED_RECORD) {
     // ThashAlloc() will work here as well.
     rootEntry = Talloc(xid, sizeof(something)); 
   
     assert(ROOT_RECORD.page == rootEntry.page);
     assert(ROOT_RECORD.slot == rootEntry.slot);
     // newRoot.size will be sizeof(something) from above.
     
     // Continue initialization procedures...
  
   } else {
     
     // The store already is initialized.  
     
     rootEntry = ROOT_RECORD;
     rootEntry.size = sizeof(something);  // Same as sizeof(something) above.
     
     // Perform any application initialization based upon its contents...
   }
   
   @endcode

   @todo Explain how to determine the correct value of rootEntry.size in the case
         of a hashtable.
   
   
   @see OPERATIONS for more operations that may be useful for your software.
   
   @subsection consistency  Using Stasis in multithreaded applications.
   
   Unless otherwise noted, Stasis' operations are re-entrant.  This
   means that an application may call them concurrently without
   corrupting Stasis' internal data structures.  However, this does
   not mean that Stasis provides full transactional consistency or
   serializable schedules.  Therefore, an application must manipulate
   data in a way that ensures logical consistency.  In other words, if
   two threads attempt to write to the same data value simultaneously,
   the result is undefined.  In database terms, you could say that
   Stasis only provides latches.  

   This is different than saying all read and write locks are 'short';
   short write locks would guarantee that once two concurrent writes
   complete, one of the values have been stored.  Stasis does not
   guarantee this although some of its data structures do have this
   property.
   
   From the point of view of conventional multithreaded software
   development, Stasis closely matches the semantics provided by
   typical operating system thread implementations.  However, it
   allows transactions to abort and rollback independently of each
   other.  This means that transactions may observe the effects of
   transactions that will eventually abort.

   Finally, Stasis asumes that each thread has its own transaction;
   concurrent calls within the same transaction are not supported.
   This restriction may be removed in the future.
   
   @section selfTest The self-test suite
   
   Stasis includes an extensive self test suite which may be invoked
   by running 'make check' in Stasis' root directory.  Some of the
   tests are for older, unmaintained code that was built on top of
   Stasis.  Running 'make check' in test/lladd runs all of the Stasis
   tests.
   
   @section archictecture Stasis' architecture 
   
   @todo Provide a brief summary of Stasis' architecture.
   
   @section extending Implementing you own operations
   
   @todo Provide a tutorial that explains howto extend Stasis with new operations.
   
   @see increment.h for an example of a very simple logical operation. 
   @see linearHashNTA.h for a more sophisticated example that makes use of Nested Top Actions.
   
   @section roadmap Roadmap
  
   @todo Fill out the roadmap section.

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
 * Defines Stasis' primary interface.
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
 * @todo recordid.page should be 64bit.
 * @todo int64_t (for recordid.size) is a stopgap fix.
 */
typedef struct {
  int page;  // XXX needs to be pageid_t, but that breaks unit tests.
  int slot;
  int64_t size; //signed long long size;
} recordid;

typedef struct {
  size_t offset;
  size_t size;
  // unsigned fd : 1;
} blob_record_t;



extern const recordid ROOT_RECORD;
extern const recordid NULLRID;

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
 * Currently, Stasis has a fixed number of transactions that may be
 * active at one time.
 */
#define EXCEED_MAX_TRANSACTIONS 1

/**
 * @param xid transaction ID
 * @param LSN last log that this transaction used
 */
typedef struct {
	int xid;
	long LSN;
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
 * Used when extending Stasis.
 * Operation implementors should wrap around this function to provide more mnuemonic names.
 *
 * @param xid          The current transaction.
 * @param rid          The record the operation pertains to.  For some logical operations, this will be a dummy record.
 * @param dat          Application specific data to be recorded in the log (for undo/redo), and to be passed to the implementation of op.
 * @param op           The operation's offset in operationsTable
 *
 * @see operations.h set.h
 */
compensated_function void Tupdate(int xid, recordid rid, const void *dat, int op);

/**
 * @param xid transaction ID
 * @param rid reference to page/slot
 * @param dat buffer into which data goes
 */
compensated_function void Tread(int xid, recordid rid, void *dat);
void TreadUnlocked(int xid, recordid rid, void *dat);

/**
 * Commit an active transaction.  Each transaction should be completed 
 * with exactly one call to Tcommit() or Tabort().
 * 
 * @param xid transaction ID
 * @return 0 on success
 */
int Tcommit(int xid);

/**
 * Abort (rollback) an active transaction.  Each transaction should be
 * completed with exactly one call to Tcommit() or Tabort().
 * 
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

/**
 * Checks to see if a transaction is still active.
 *
 * @param xid The transaction id to be tested.
 * @return true if the transacation is still running, false otherwise.
 */
int TisActiveTransaction(int xid);

/** 
    This is used by log truncation.
*/
lsn_t transactions_minRecLSN();

END_C_DECLS

#endif
