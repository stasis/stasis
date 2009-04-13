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
 * This module includes page manipulation API's, the logger, the
 * buffer manager, recovery code, and other primitives can be used to
 * implement custom transactional data structures.
 *
 * This section does not include @ref OPERATIONS, which include
 * implementations of higher level transactional data structures, such
 * as hashtables, and record / blob manipulation.
 */
/**
   @mainpage Introduction to Stasis

   Stasis is a <i>flexible</i> transactional storage library.  Unlike
   existing systems, it provides application and server developers
   with much freedom, but little guidance regarding page file layouts,
   data models, and concurrency schemes.  This often means that Stasis
   can outperform general purpose storage solutions by orders of
   magnitude, but it does require more effort on the part of its end
   users.  We are in the process of implementing a library of commonly
   used, general-purpose transactional data structures on top of
   Stasis.

   @section The Stasis data model

   Stasis does not really have a data model.  While designing and
   implementing Stasis, we focused on providing end users with
   <i>mechanisms, not policy</i>.  As much as possible, we have
   avoiding hardcoding application-specific implmentation details such
   as data formats and concurrency models.  Instead, Stasis provides a
   number of reusable lower-level mechanisms that make it easy for
   applications to implement custom transactional storage systems.

   At the lowest level, Stasis provides <i>transactional pages</i>;
   the buffer manager manages a set of pages and regions on disk, and
   coordinates with the log and other Stasis mechanisms to provide
   recovery and concurrent transactions.  On top of this low level
   API, we have developed record oriented interfaces that facilitate
   development and interchangability of new page formats.  Conformance
   to these APIs is recommended, but not required.  Stasis records are
   simply arrays of bytes (not tuples).  A Stasis recordid is simply a
   record that contains a pointer to another record.

   Stasis's @ref OPERATIONS provide a set of methods that manipulate
   records in a transactional, durable fashion.  They are implemented
   on top of the record (and sometimes page) API's, and range from low
   level access methods like record reads and writes to higher level
   data structure implementations, such as hash tables and log-oriented
   tree indexes.  Stasis' library of operations makes use of a number of
   different @ref LOGGING_DISCIPLINES.  Most new operations will want to
   choose one of these disciplines, as many subtlties arise during the
   development of new concurrent, high-performance recovery
   algorithms.

   @image html "StasisDBMS.png" "Stasis' place in a conventional DBMS architecture"

   @section Tutorial

   @ref gettingStarted explains how to download and compile Stasis,
   and includes a number of sample programs.

   The "Modules" section in the navigation pane contains detailed
   documentation of Stasis' major components.

   @see <a href="modules.html">Modules</a>

*/

/**
   @page gettingStarted Getting Started
   @section compiling Compiling and installation

   Prerequisites:

   - automake 1.8+: needed to build from CVS
   - <a href="http://check.sourceforge.net">check</a>: A unit testing
     framework (needed to run the self-tests)

   Optional:

   - libconfuse: Used by older networking code to parse configuration options.
   - BerkeleyDB: Used by the benchmarking code for purposes of comparison.

   Development is currently performed under Debian's Testing branch.

   To compile Stasis, first check out a copy with SVN.  If you have commit access:

   @verbatim
   svn co --username username https://stasis.googlecode.com/svn/trunk stasis
   @endverbatim

   For anonymous checkout:

   @verbatim
   svn co http://stasis.googlecode.com/svn/trunk stasis
   @endverbatim

   then:

   @code

   $ ./reconf
   $ ./configure --quiet
   $ make -j4 > /dev/null
   $ cd test/stasis
   $ make check

   @endcode

   This will fail if your system defaults to an old (pre-1.7) version
   of autotools.  Fortunately, multiple versions of autotools may
   exist on the same system.  Execute the following commands to
   compile with version 1.8 (or 1.9) of autotools:

   @code

   $ ./reconf-1.8  # or ./reconf-1.9
   $ ./configure --quiet
   $ make -j4 > /dev/null
   $ cd test/stasis
   $ make check

   @endcode

   Of course, you can omit "--quiet" and "> /dev/null", but configure
   and make both produce quite a bit of output that may obscure useful
   warning messages.

   'make install' installs the Stasis library and python SWIG
   bindings, but none of the extra programs that come with Stasis.
   utilities/ contains a number of utility programs that are useful
   for debugging Stasis.  The examples/ directory contains a number of
   simple C examples.

   @section usage Using Stasis in your software

   Synopsis (examples/ex1.c):

   @include examples/ex1.c

   Hopefully, Tbegin(), Talloc(), Tset(), Tcommit(), Tabort() and Tdealloc() are
   self explanatory.  If not, they are covered in detail elsewhere.  Tinit() and
   Tdeinit() initialize the library, and clean up when the program is finished.

   Other particularly useful functions are ThashCreate(), ThashDelete(),
   ThashInsert(), ThashRemove(), and ThashLookup() which provide a
   re-entrant linear hash implementation.  ThashIterator() and
   ThashNext() provide an iterator over the hashtable's values.

   @subsection bootstrap Reopening a closed data store

   Stasis imposes as little structure upon the application's data structures as
   possible.  Therefore, it does not maintain any information about the contents
   or naming of objects within the page file.  This means that the application
   must maintain such information manually.

   In order to facilitate this, Stasis provides the function
   TrecordType() and guarantees that the first recordid returned by
   any allocation will point to the same page and slot as the constant
   ROOT_RECORD.  TrecordType() will return UNINITIALIZED_RECORD if the
   record passed to it does not exist.  A second function,
   TrecordSize() returns the size of a record in bytes, or -1 if the
   record does not exist.

   Therefore, the following code (found in examples/ex2.c) will safely
   initialize or reopen a data store:

   @include examples/ex2.c

   @todo Explain how to determine the correct value of rootEntry.size in the case
         of a hashtable.


   @see OPERATIONS for other transactional primitives that may be
        useful for your software.

   @subsection consistency  Using Stasis in multithreaded applications

   Unless otherwise noted, Stasis' operations are re-entrant.  This
   means that an application may call them concurrently without
   corrupting Stasis' internal data structures.  However, if two
   threads attempt to write the same data value simultaneously, the
   result is undefined.

   In database terms, Stasis uses latches to protect its own data
   structures' consistency (including those on disk), but does not
   obtain short term read or write locks to protect data as it is
   being written.  This is less consistency than SQL's Level 0 (Dirty
   Reads) provides.  Some of Stasis' data structures do obtain short
   read and write locks automatically.  Refer to individual data
   structures for more information.

   Stasis' allocation functions, such as Talloc(), do not reuse space
   that was freed by an ongoing transaction.  This means that you may
   safely overwrite freshly allocated space without writing undo
   entries, and allows concurrent transactions to safely allocate
   space.

   From the point of view of conventional multithreaded software
   development, Stasis closely matches the semantics provided by
   typical operating system thread implementations.  However, it
   allows transactions to abort and rollback independently of each
   other.  This means that transactions may observe the effects of
   transactions that will eventually abort.

   Finally, Stasis assumes that each thread has its own transaction;
   concurrent calls within the same transaction are not supported.
   This restriction may be removed in the future.

   @section selfTest The test suite

   Stasis includes an extensive unit test suite which may be invoked
   by running 'make check' in Stasis' root directory.  Some of the
   tests are for older, unmaintained code that was built on top of
   Stasis.  Running 'make check' in test/stasis runs all of the Stasis
   tests without running the obsolete tests.

   @section architecture Stasis' structure

   This section is geared toward people that would like to extend
   Stasis.  The OSDI paper provides a higher level description and
   motivation for the architecture.  This section describes naming
   conventions used to distinguish between different portions of
   Stasis, and provides an overview of memory management and mutex
   acquisition conventions.

   This section does not describe recovery, transaction initiation,
   etc.  Those methods change less frequently.  Instead of focusing on
   them, this text focuses on the issues faced by transactional data
   structures.

   Stasis components can be classified as follows:

   - I/O utilities (file handles, OS compatibility wrappers)
   - Log interfaces (logger/logger2.c  logger/logEntry.c logger/logHandle.c) and implementations (logger/logWriter.c logger/inMemoryLog.c)
   - Buffer management
   - Recovery
   - Page formats and associated operations (page/slotted.c page/fixed.c)
   - Application visible methods (Talloc, Tset, ThashInsert, etc)

   @subsection layoutNaming Directory layout

   The Stasis repository contains the following "interesting" directories:

   @par $STASIS/stasis/

   Contains the header directory structure.

   In theory, this contains all of the .h files that need to be
   installed for a fully functional Stasis development environment.
   In practice, .h files in src/ are also  needed in some cases.  The
   separation of .h files between src/ and stasis/ continues for
   various obscure reasons, including CVS's lack of a "move" command.
   For now, .h files should be placed with similar .h files, or in
   stasis/ if no such files exist.

   The directory structure of stasis/ mirrors that of src/

   @par $STASIS/src/

   Contains the .c files

   @par $STASIS/src/stasis

   Contains Stasis and the implementations of its standard modules.
   The subdirectories group files by the type of module they
   implement.

   @note By convention, when the rest of this document says
   <tt>foo/</tt>, it is referring to two directories:
   <tt>stasis/foo/</tt> and <tt>src/stasis/foo/</tt>.  Unless it's clear
   from context, a file without an explicit directory name is in
   <tt>stasis/</tt> or <tt>src/stasis/</tt>.  In order to refer to files
   and directories outside of these two locations, but still in the
   repository, this document will use the notation
   <tt>$STASIS/dir</tt>.

   @note This is done for brevity, and to avoid coupling documentation
   to the (deprecated) placement of .h files under src/.

   @note <b>Example:</b> The transactional data structure
   implementations in <tt>operations/</tt> can be found in
   <tt>$STASIS/src/stasis/operations/</tt> and
   <tt>$STASIS/stasis/operations/</tt>.

   @subsection Modules

   Stasis is implemented in C, but is structured in a somewhat object
   oriented style.  There are a number of different "modules", for
   lack of a better term.  Each implementation in the module lives in
   the module's subdirectory.  Code that is common to many
   implementation, and headers that define per-module functions live
   in files named after the module.

   <b>Example:</b> The <tt>io</tt> module contains the following files:

   @code
      io.h
      io.c
      io/handle.h
      io/debug.c
      io/file.c
      io/memory.c
      io/non_blocking.c
      io/rangeTracker.h
      io/rangeTracker.c
   @endcode

   In this case, rangeTracker.c and io.c are the only files containing
   more than one non-static method, so they are the only ones that
   have corresponding .h files.  rangeTracker.c is implementing a data
   structure that is being used by the other files.  debug.c, file.c,
   memory.c and non_blocking.c each implements a different type of
   handle.

   Some modules are simply groups of files that perform similar tasks,
   or make use of the same set of interfaces (eg: <tt>page/</tt> and
   <tt>operations/</tt>).  Files in these directories may make use of the same
   utility functions, but aren't implementing the same interface.

   Other modules provide multiple implementations of the same
   interface (eg: <tt>io/</tt> and <tt>logger/</tt>).  C doesn't have
   inheritance, so Stasis "fakes it" using one of two methods.  In
   both cases, a struct is defined to contain a void pointer, which
   the implementation manually casts to the appropriate type:

   @par Dispatch functions

   The dispatch functions contain a switch statement or conditional
   that decides which implementation to call. Calling convention:

   @code bird_carry(african_swallow, coconut); @endcode

   @par struct of function pointers

   These functions use the following calling convention:

   @code african_swallow->carry(african_swallow,coconut) @endcode

   @subsection ioutil I/O utilities

   The I/O utilities live in <tt>io/</tt>.  They provide reentrant
   interfaces.  This was written to insulate Stasis from Linux's
   ever-evolving I/O system calls, for portability, and to allow (for
   example) in-memory operation.

   @subsection walin WAL Modules

   None of these modules understand page formats; at this level
   everything is either

   - a page with an LSN (a version number),or

   - a log entry with an associated operation (redo / undo
   functions).

   Interesting files in this part of Stasis include logger2.c,
   bufferManager.c, and recovery2.c.

   @subsection page Custom page formats

   Stasis provides a default @ref PAGE_RECORD_INTERFACE to custom page
   implementations.  Methods that define their own log disciplines, or
   otherwise need to bypass Stasis' default recovery mechanisms should
   call into this API.

   By defining these methods and registering appropriate callbacks,
   page implementations allow callers to access their data through
   standard Stasis methods such as Tread() and Tset().  This module
   also includes a set of utility methods to simplify the pointer
   arithmetic associated with manipulating the buffer manager's copy
   of pages.

   @par A note on storage allocation

   Stasis currently provides a few different mechanisms that allocate
   entire pages and page ranges at once.  There are examples of three
   approaches in the current code base:

   - Implement a full-featured, general purpose allocator, like the
     one in alloc.h.  This is more difficult than it sounds.

   - Allocate entire regions at a time, and manually initialize pages
     within them.  arrayList.h does this.  This is the most flexible
     and efficient approach, but requires extra management code if
     region allocation is not a natural approach.

   - Allocate a single page at a time using TallocPage(), then call
     page initialization methods on each page.  Currently,
     TallocPage() is poorly implemented and wastes one page for every
     page it allocates.

   Note that before you initialize a new page you need to call
   stasis_page_cleanup() to notify the page's old format that it should
   free resources associated with the old version of the page.
   Stasis' allocation routines guarantee that the pages they return
   were freed by committed transactions (and therefore, that their
   contents can be discarded).  Therefore, you do not need to log the
   preimage of pages returned by the allocator.

   @todo Should we change the API so that allocation routines (TpageAlloc(), TregionAlloc()) call stasis_page_cleanup() on behalf of their callers?

   @todo Optimize page, region allocation to call page initializers automatically during forward operation and redo?

   @see page.h, fixed.h, and slotted.h for more information on the
   page API's, and the implementations of two common page formats.

   @subsection appfunc Application visible methods

   These methods start with "T".  Look at the examples above.  These
   are the "wrapper functions" from the OSDI paper.  They are
   supported by @ref OPERATIONS.

   @section extending Implementing you own operations

   @todo Provide a tutorial that explains how to extend Stasis with new operations.

   @see increment.h for an example of a very simple logical operation.
   @see linearHashNTA.h for a more sophisticated example that makes use of Nested Top Actions.

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
 * @defgroup LOGGING_DISCIPLINES Logging Disciplines
 *
 * Stasis' log API provides a number of methods that directly
 * manipulate the log.
 *
 * @section SNF STEAL/NO-FORCE recovery
 * Stasis includes a function, Tupdate(), that
 * provides traditional STEAL/NO-FORCE logging for recovery.  The
 * STEAL/NO-FORCE strategy allows dirty, uncommitted pages to be
 * written back to disk (STEAL), which prevents long running
 * transactions from exhausting RAM.  It does not force write pages to
 * disk at commit (NO-FORCE), and instead only forces the log.  This
 * prevents the hard drive head from performing unnecessary seeks
 * during commit.  Recovery works by "repeating history"; all actions
 * are redone up to some point in time after the last successful
 * transaction committed, but before the crash.  Conceptually, any
 * partially commited transactions are then rolled back using
 * Tabort(), as they would be during normal operation.  For more
 * information about STEAL/NO-FORCE recovery strategies, see the ARIES
 * paper (XXX cite aries properly)
 *
 *
 * @section SF STEAL/FORCE and bulk-logged recovery
 *
 * Stasis supports other logging disciplines as well.  In particular,
 * the buffer manager allows REGIONS (XXX document region allocator)
 * to be synchronously written to disk, allowing operations to make
 * use of a STEAL/FORCE recovery strategy.  This is attractive when a
 * transaction is writing a large, contiguous region of disk, as
 * STEAL/FORCE operations do not write redo information to the log.
 * If the STEAL/FORCE transaction is overwriting newly allocated
 * pages, it can also avoid writing undo information to the log, as
 * the newly allocated pages do not contain useful data.  This allows
 * large objects to be written with no tangible logging overhead, and
 * has been implemented by a number of commercial systems.  It is used
 * by the Stasis' Rose indexes. (XXX cite LSM trees, etc.)
 *
 * @section LSNFREE LSN-Free pages
 *
 * Stasis' third (and most exotic) logging strategy makes use of
 * LSN-free pages.  By constraining the behavior of redo and undo log
 * entries, we can entirely avoid storing Stasis metadata on pages.
 * This logging discipline is under development.
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
#include "flags.h"
BEGIN_C_DECLS

/**
 * Initialize Stasis.  This opens the pagefile and log, initializes
 * subcomponents, and runs recovery.
 *
 * @return 0 on success
 */
int Tinit();

/**
 * Start a new transaction, and return a new transaction id (xid).
 *
 * @return positive transaction ID (xid) on success, negative return value on error
 */
int Tbegin();

/**
 * Used when extending Stasis.
 * Operation implementers should wrap around this function to provide more mnemonic names.
 *
 * @param xid          The current transaction.
 * @param page         The id of the page that the operation should be run against.
 * @param dat          Application specific data to be recorded in the log (for undo/redo), and to be passed to the implementation of op.
 * @param datlen       The length of dat, in bytes.
 * @param op           The operation's offset in operationsTable
 *
 * @see operations.h For an overview of the operations API
 */
compensated_function void Tupdate(int xid, pageid_t page,
				  const void *dat, size_t datlen, int op);
/**
   @deprecated Only exists to work around swig/python limitations.
 */
compensated_function void TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op);

void TreorderableUpdate(int xid, void * h, pageid_t page,
                        const void * dat, size_t datlen, int op);
/** Note; it is *your* responsibility to set the lsn on the page; this
    function returns a plausible value */
lsn_t TwritebackUpdate(int xid, pageid_t page,
                       const void * dat, size_t datlen, int op);


/** DANGER: you need to set the LSN's on the pages that you want to write back,
    this method doesn't help you do that, so the only option is to pin until
    commit, then set a conservative (too high) lsn */
void TreorderableWritebackUpdate(int xid, void* h,
                                 pageid_t page, const void * dat,
                                 size_t datlen, int op);

/**
 * Read the value of a record.
 *
 * @param xid transaction ID
 * @param rid reference to page/slot
 * @param dat buffer into which data goes
 */
compensated_function void Tread(int xid, recordid rid, void *dat);
/**
 * Read a value of a record without first dereferencing the record.
 * Use Tread() unless you're implementing code that provides
 * dereferencible records.
 *
 * @see arrayList for a data structure that uses recordid
 *      dereferencing to transparently provide records to its callers.
 */
compensated_function void TreadRaw(int xid, recordid rid, void *dat);
compensated_function void TreadStr(int xid, recordid rid, char *dat);

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
 * Cleanly shutdown Stasis.  After this function is called, you should
 * call Tinit() before attempting to access data stored in Stasis.
 * This function flushes all pages, cleans up log, and frees any
 * resources that Stasis is holding.
 *
 * @return 0 on success
 */
int Tdeinit();
/**
 * Uncleanly shutdown Stasis.  This function frees any resources that
 * Stasis is holding, and flushes the log, but it does not flush dirty
 * pages to disk.  This is used by testing to exercise the recovery
 * logic.
 *
 * @return 0 on success
*/
int TuncleanShutdown();
/**
 *  Used by the recovery process.
 *  Revives Tprepare'ed transactions.
 *
 * @param xid  The xid that is to be revived.
 * @param prevlsn  The lsn of that xid's most recent PREPARE entry in the log.
 * @param reclsn The lsn of the transaction's BEGIN record.
 */
void Trevive(int xid, lsn_t prevlsn, lsn_t reclsn);
/**
    Prepare transaction for commit.  Currently, a transaction may be
    prepared multiple times.  Once Tprepare() returns, the caller is
    guaranteed that the current transaction will resume exactly where
    it was the last time Tprepare() was called.

    @todo move prepare to prepare.[ch]

    @param xid Transaction id.
*/
int Tprepare(int xid);

/**
 * Begin a nested top action
 *
 * Nested Top Actions allow you to register logical undo operations
 * for data structure manipulation.  This is generally a prerequisite
 * to concurrent transaction systems.
 *
 * @see ex3.c for an example of nested top actions.
 */
int TnestedTopAction(int xid, int op, const byte * arg, size_t arg_len);

/**
 * Begin a nested top action
 *
 * Nested top actions provide atomic updates to multiple pages within
 * a single transaction.  Stasis's nested top actions may be nested
 * within each other.
 *
 * @see TnestedTopAction() is less expressive, but much more convenient.
 */
void * TbeginNestedTopAction(int xid, int op, const byte* log_arguments,
                             int log_arguments_length);
/**
 * Complete a nested top action, atomically switching from physical to
 * logical undo.
 *
 * @see TnestedTopAction() is less expressive, but much more convenient.
 */
lsn_t TendNestedTopAction(int xid, void * handle);

/**
   List all active transactions.

   @return an array of transaction ids.
 */
int* TlistActiveTransactions();

/**
 * Checks to see if a transaction is still active.
 *
 * @param xid The transaction id to be tested.
 * @return true if the transaction is still running, false otherwise.
 */
int TisActiveTransaction(int xid);
/*
 * @return the number of currently active transactions.
 */
int TactiveTransactionCount();

/**
   Initialize Stasis' transaction table.  Called by Tinit() and unit
   tests that wish to test portions of Stasis in isolation.
 */
void stasis_transaction_table_init();

/**
 *  Used by recovery to prevent reuse of old transaction ids.
 *
 *  Should not be used elsewhere.
 *
 * @param xid  The highest transaction id issued so far.
 */
void stasis_transaction_table_max_transaction_id_set(int xid);
/**
 *  Used by test cases to mess with internal transaction table state.
 *
 * @param xid  The new active transaction count.
 */
void stasis_transaction_table_active_transaction_count_set(int xid);

int stasis_transaction_table_roll_forward(int xid, lsn_t lsn, lsn_t prevLSN);
/**
   @todo update Tprepare() to not write reclsn to log, then remove
         this function.
 */
int stasis_transaction_table_roll_forward_with_reclsn(int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN);
int stasis_transaction_table_forget(int xid);

/**
    This is used by log truncation.
*/
lsn_t stasis_transaction_table_minRecLSN();
/**
 * Called at the end of transactions aborted by recovery, after the transaction
 * has been completely rolled back (ie: all rollback entries are in the log's
 * in-memory write buffer).
 *
 * Writes an XEND log entry.
 */
int Tforget(int xid);
/**
   Report Stasis' current durability guarantees.

   @return VOLATILE if the data will be lost after Tdeinit(), or a
   crash, PERSISTENT if the data will be written back to disk after
   Tdeinit(), but may be corrupted at crash, or DURABLE if Stasis will
   apply committed transactions, and roll back active transactions
   after a crash.
*/
int TdurabilityLevel();

#include "operations.h"

END_C_DECLS

#endif
