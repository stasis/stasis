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
   @mainpage Introduction to LLADD
   
   @section compiling Compiling and installation
   
   Prerequisites:
   
   - automake 1.8+: needed to build from CVS
   - libconfuse: configuration file parser_range
   - libcheck: A unit testing framework (optional, needed to run most of the self-tests)
   - BerkeleyDB: Used by the benchmarking code for purposes of comparison (Should 
     eventually be made optional)  The benchmarks have been tested with BerkeleyDB 4,2
   
   Development is currently performed under Debian's Testing branch.  Unless 
   noted above, the most recent version is the one used for development.
   
   To compile LLADD, first check out a copy with CVS, then:
   
   @code
   
   $ ./reconf
   $ ./configure
   $ make
   $ make check
   
   @endcode
   
   'make install' is currently broken.  Look in utilities/ for an example of a 
   simple program that uses LLADD.  Currently, most generally useful programs 
   written on top of LLADD belong in lladd/src/apps, while utilities/ contains 
   programs useful for debugging the library.
   
   @section usage Using LLADD in your software
   
   Synopsis:
   
   @code
   
   #include <lladd/transaction.h>
   
   ...
   
   Tinit();
   
   int i = 42;
   
   int xid = Tbegin();
   recordid rid = Talloc(xid, sizeof(int)); 
   Tset(xid, rid, i);    // the application is responsible for memory management.
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
   
   Other partiularly useful functions are ThashAlloc(), ThashDelete(), 
   ThashInsert(), ThashRemove(), and ThashLookup() which provide a re-entrant 
   linear hash implementation.  Currently, the hashtable only supports fixed 
   length keys and values (the lengths are set when the hashtable is created).  
   ThashIterator() and ThashNext() provide an iterator over the hashtable's 
   values.  Also of general use is the Tprepare() function which guarantees that 
   a transaction's current state will survive a system crash, but does not cause
   the transaction to commit.
   
   @subsection bootstrap Reopening a closed data store
   
   LLADD imposes as little structure upon the application's data structures as 
   possible.  Therefore, it does not maintain any information about the contents
   or naming of objects within the page file.  This means that the application 
   must maintain such information manually.
   
   In order to facilitate this, LLADD provides the function TgetRecordType() and
   guarantess that the first recordid returned by any allocation will point to 
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
   
   @subsection consistency  Using LLADD in multithreaded applications.
   
   Unless otherwise noted, LLADD's operations are re-entrant.  This means that 
   an application may call them concurrently without corrupting LLADD's internal
   data structures.  However, this does not mean that LLADD provides full
   transactional consistency or serializable schedules.  Therefore, an application 
   must manipulate data in a way that ensures logical consistency.  In other words, 
   if two threads attempt to write to the same data value simultaneously, the result 
   is undefined.  In database terms, you could say that LLADD only provides latches, 
   or that all read and write locks are 'short'.  
   
   If you are unfamiliar with the terms above, but are familiar with multithreaded
   software development, don't worry.  These semantics closely match those 
   provided by typical operating system thread implementations, and we recommend
   the use of pthread's mutexes, or a similar synchronization mechanism to 
   protect the logical consistency of application data.
   
   Finally, LLADD asumes that each thread has its own transaction; concurrent calls
   within the same transaction are not supported.  This restriction may be lifted in 
   the future.
   
   @section selfTest The self-test suite
   
   LLADD includes an extensive self test suite which may be invoked by running 
   'make check' in LLADD's root directory.
   
   @section archictecture LLADD's architecture 
   
   @todo Provide a brief summary of LLADD's architecture.
   
   @section extending Implementing you own operations
   
   @todo Provide a tutorial that explains howto extend LLADD with new operations.
   
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
 * @todo size should be 64bit.  Unfortunately, 'long' is 32 bit on ia32...
 * @todo signed long long is a stopgap fix.. should do this in a prinicpled way.
 */
typedef struct {
  int page;
  int slot;
  signed long long size;
} recordid;

typedef struct {
  size_t offset;
  size_t size;
  unsigned fd : 1;
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
 * Currently, LLADD has a fixed number of transactions that may be
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
compensated_function void Tupdate(int xid, recordid rid, const void *dat, int op);

/**
 * @param xid transaction ID
 * @param rid reference to page/slot
 * @param dat buffer into which data goes
 */
compensated_function void Tread(int xid, recordid rid, void *dat);
void TreadUnlocked(int xid, recordid rid, void *dat);

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

/** 
    This is used by log truncation.
*/
lsn_t transactions_minRecLSN();

END_C_DECLS

#endif
