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
 * Transaction-level log manipulation routines (commit, update, etc...) that maintain consistency with the transaction tables.
 *
 * @ingroup LOGGING_INTERFACES
 */

#ifndef __LOGGER2_H__
#define __LOGGER2_H__

#include <stasis/common.h>

typedef struct stasis_log_t stasis_log_t;

typedef struct stasis_log_group_force_t stasis_log_group_force_t;

typedef enum {
  LOG_FORCE_COMMIT, LOG_FORCE_WAL
} stasis_log_force_mode_t;

#include <stasis/logger/groupForce.h>
#include <stasis/logger/logEntry.h>
#include <stasis/truncation.h>
#include <stasis/constants.h>
#include <stasis/transactionTable.h>

/**
   A callback function that allows logHandle's iterator to stop
   returning log entries depending on the context in which it was
   called.
*/
typedef int (guard_fcn_t)(const LogEntry *, void *);

/**
 * Interface provided by Stasis log implementations.
 *
 * This struct defines the interface provided by Stasis log
 * implementations.  New log implementations should provide a
 * method that populates a stasis_log_t with appropriate function
 * pointers and runtime state.
 *
 * @see safeWrites.c, inMemoryLog.c for example implementations.
 *
 * @ingroup LOGGING_IMPLEMENTATIONS
 */
struct stasis_log_t {
  /**
     Register a truncation policy with the log.

     If this method is not called, and the log's length is
     constrained by a hard limit, then truncation might not occur
     promptly (or at all) when the log runs out of space.

   */
  void (*set_truncation)(struct stasis_log_t *log, stasis_truncation_t *trunc);
  /**
     Return the size of an implementation-specific log entry.

     Log implementations may store extra information in "internal entries".
     These entries will be ignored by higher-level code.  In order to
     facilitate memory management, Stasis' sizeofLogEntry() method supports
     internal entries by calling this method.

     @param log "this" log object
     @param e A log entry with type INTERNALLOG
     @return the length of e, in bytes.
  */
  lsn_t (*sizeof_internal_entry)(struct stasis_log_t* log, const LogEntry * e);

  /**
     Append a log entry to the end of the log.

     Append a log entry to the end of the log.

     @param log "this" log object
     @param e The entry to be written to log.  After the call returns, e->LSN will be the new entry's offset.
     @return 0 on success
  */
  int (*write_entry)(struct stasis_log_t* log, LogEntry * e);

  LogEntry* (*reserve_entry)(struct stasis_log_t* log, size_t sz);

  int (*write_entry_done)(struct stasis_log_t* log, LogEntry* e);

  /**
     Read a log entry, given its LSN.

     Read a log entry, given its LSN.

     @param log "this" log object
     @param lsn  The LSN of the log entry to be read.  This must be the LSN of a valid log entry.
     @return The LogEntry of interest.  Should be freed with freeLogEntry().  A NULL return value means the log was truncated past the requested entry.
  */
  const LogEntry* (*read_entry)(struct stasis_log_t* log, lsn_t lsn);

  /**
   * Free any resources associated with reading a log entry.  This should be called once for each call to read_entry.
   */
  void (*read_entry_done)(struct stasis_log_t *log, const LogEntry *e);
  /**
     Given a log entry, return the LSN of the next entry.

     This method returns the LSN of the log entry that will succeed the
     given entry.  Since the meaning of the LSN field is defined by the
     underlying log implementation, this could return the offset into some
     underlying file, or simply e->LSN + 1.

     @param log "this" log object
     @param e A LogEntry that has already been stored in this log.
     @return the LSN of the next entry.  Since LSN's must define an order
             over the log, this must be greater than e->LSN.
  */
  lsn_t (*next_entry)(struct stasis_log_t* log, const LogEntry * e);

  /**
     Return the LSN of the earliest log entry that may not survive a crash.

     Return the LSN of the earliest log entry that may not survive a crash.
     If the entire log is stable, or the log does not support durability,
     this function returns the LSN of the entry that will be allocated
     the next time the log is appended to.

     @param log "this" log object, which may or may not support durability.
     @param mode The mode in which the log entries must have been forced.
  */
  lsn_t (*first_unstable_lsn)(struct stasis_log_t* log,
                              stasis_log_force_mode_t mode);
  /**
     Return the LSN that will be assigned to the next entry written to this log.

     This function returns the LSN that will be assigned to the next entry
     written to log.  Because multiple threads may be accessing the same
     stasis_log_t object, this method should not be used to determine which
     LSN will actually be assigned; rather it is used to compute a valid
     lower bound of the LSN of newly-loaded LSN-free pages.

     @param log "this" log object
  */
  lsn_t (*next_available_lsn)(struct stasis_log_t* log);
  /**
     Force any enqueued, unwritten entries to disk.

     Once this method returns, any log entries written before the call began
     should survive subsequent crashes.  If the underlying log implementation
     is not durable, then this method has no effect.

     This method should not attempt to amortize the cost of multiple
     concurrent calls; stasis_log_t::group_force provides takes care of this.
     If group_force is non-null, callers should invoke methods on it rather
     than call this method directly.

     @param log "this" log object, which may or may not support durability.
     @param mode The reason the log tail should be forced; in certain
     environments, force writes that maintain the write-ahead invariant are
     treated differently than those for transaction commit.
  */
  void (*force_tail)(struct stasis_log_t* log, stasis_log_force_mode_t mode);

  /**
     Delete a prefix of the log.

     This method allows the log to "forget" about old log entries.  Its
     behavior is implementation defined.  A call to truncate amounts to a
     promise that subsequent calls to stasis_log_t::read_entry will not
     request entries before the truncation point.

     @param log "this" log object.
     @param lsn The truncation point; the first lsn that will be available after truncation.
     @return 0 on success
  */
  int (*truncate)(struct stasis_log_t* log, lsn_t lsn);

  /**
     Return the LSN of the first entry of the log.

     This function returns the LSN of the earliest entry in the log, which
     must be less than or equal to the highest value ever passed into
     stasis_log_t::truncate().  If the log is empty, this function returns
     the same value as stasis_log_t::next_available_lsn().

     @param log "this" log object
     @return A valid LSN that may be passed into stasis_log_t::read_entry().
  */
  lsn_t (*truncation_point)(struct stasis_log_t* log);
  /**
     Ensure that the tail of the log is durable, and free any associated resources.

     Ensure that the tail of the log is durable, and free any associated resources.

     @return 0 on success
  */
  int (*close)(struct stasis_log_t* log);

  /**
   * Determine whether or not this log provides durability.
   *
   * @return true if this log implementation is durable, zero otherwise.
   */
  int (*is_durable)(struct stasis_log_t* log);
  /**
   * @see groupForce.c
   */
  stasis_log_group_force_t * group_force;
  /**
   *  Implementation-specific state.
   */
  void* impl;
};

/**
   Synchronously make a prefix of the log durable.

   This method uses group commit to reduce the number of calls to
   force_tail().

   Durability is guaranteed in an implementation-specific way.

   @param log A log that already contains the entries to be forced to disk.
   @param lsn Log entries up to and including the one that overlaps lsn will
              be durable after this call returns.  If INVALID_LSN is passed in,
              the log will be immediately forced up to the current tail, bypassing
              group commit.
   @param mode The durability mode associated with this call.

   @see stasis_log_force_mode_t
   @see logger2.h for information about force_tail().
 */
void stasis_log_force(stasis_log_t* log, lsn_t lsn, stasis_log_force_mode_t mode);

/**
   Inform the logging layer that a new transaction has begun, and
   obtain a handle.
*/
void stasis_log_begin_transaction(stasis_log_t* log, int xid, stasis_transaction_table_entry_t* l);

/**
   Write a transaction PREPARE to the log tail.  Blocks until the
   prepare record is stable.

   @return the lsn of the prepare log entry
*/
lsn_t stasis_log_prepare_transaction(stasis_log_t* log, stasis_transaction_table_entry_t * l);
/**
   Write a transaction COMMIT to the log tail.  Blocks until the commit
   record is stable.

   @return the lsn of the commit log entry.
*/
lsn_t stasis_log_commit_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l, int force);

/**
   Write a transaction ABORT to the log tail.  Does not force the log.

   @return the lsn of the abort log entry.
*/
lsn_t stasis_log_abort_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l);

/**
   Write a end transaction record.  This entry tells recovery's undo
   phase that it may safely ignore the transaction.
*/
lsn_t stasis_log_end_aborted_transaction (stasis_log_t* log, stasis_transaction_table_t *tbl, stasis_transaction_table_entry_t * l);

/**
   stasis_log_write_update writes an UPDATELOG log record to the log tail.  It
   also interprets its operation argument to the extent necessary for
   allocating and laying out the log entry.  Finally, it updates the
   state of the parameter l.
*/
LogEntry * stasis_log_write_update(stasis_log_t* log,
                     stasis_transaction_table_entry_t * l, pageid_t page, unsigned int operation,
                     const byte * arg, size_t arg_size);

/**
   Write a compensation log record.  These records are used to allow
   for efficient recovery, and possibly for log truncation.  They
   record the completion of undo operations, amongst other things.

   @return the lsn of the CLR entry that was written to the log.
   (Needed so that the lsn slot of the page in question can be
   updated.)
*/
lsn_t stasis_log_write_clr(stasis_log_t* log, const LogEntry * e);

lsn_t stasis_log_write_dummy_clr(stasis_log_t* log, int xid, lsn_t prev_lsn);

LogEntry * stasis_log_begin_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, unsigned int op,
                                const byte * arg, size_t arg_size);
lsn_t stasis_log_end_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, LogEntry * e);
#endif
