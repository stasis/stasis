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
 * Interface to Stasis' log file.
 *
 * @ingroup LOGGING_DISCIPLINE
 *
 * $Id$
 *
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
#include <stasis/constants.h>
/**
   Contains the state needed by the logging layer to perform
   operations on a transaction.
 */
typedef struct TransactionLog {
  int xid;
  lsn_t prevLSN;
  lsn_t recLSN;
  pthread_mutex_t mut;
} TransactionLog;


#include <stasis/operations.h>

/**
   A callback function that allows logHandle's iterator to stop
   returning log entries depending on the context in which it was
   called.
*/
typedef int (guard_fcn_t)(const LogEntry *, void *);



/**
   XXX TransactionTable should be private to transactional2.c!
*/
extern TransactionLog stasis_transaction_table[MAX_TRANSACTIONS];



struct stasis_log_t {
  /**
     Needed by sizeofLogEntry
  */
  lsn_t (*sizeof_internal_entry)(struct stasis_log_t* log, const LogEntry * e);

  /**
     Append a log entry to the end of the log.

     @param e This call sets e->LSN to entry's offset.
     @return 0 on success
  */
  int (*write_entry)(struct stasis_log_t* log, LogEntry * e);

  /**
     Read a log entry, given its LSN.
     @param lsn  The lsn of the log entry to be read.
  */
  const LogEntry* (*read_entry)(struct stasis_log_t* log, lsn_t lsn);

  /**
     Given a log entry, return the LSN of the next entry.
  */
  lsn_t (*next_entry)(struct stasis_log_t* log, const LogEntry * e);

  /**
     This function returns the LSN of the most recent
     log entry that has not been flushed to disk.  If the entire log
     is flushed, this function returns the LSN of the entry that will
     be allocated the next time the log is appended to.

     @param log The log file, which may or may not support durability.
     @param mode The mode in which the log entries must have been forced.
  */
  lsn_t (*first_unstable_lsn)(struct stasis_log_t* log,
                              stasis_log_force_mode_t mode);
  /**
     This function returns the LSN of the next log entry passed to
     write_entry.  This shouldn't be used to determine which entry a
     particular call will assign; rather it is used to provide a lower
     bound on the LSN of newly-loaded LSN-free pages.
  */
  lsn_t (*next_available_lsn)(struct stasis_log_t* log);
  /**
     Force any enqueued, unwritten entries to disk
  */
  void (*force_tail)(struct stasis_log_t* log, stasis_log_force_mode_t mode);

  /**
      @param lsn The first lsn that will be available after truncation.
      @return 0 on success
  */
  int (*truncate)(struct stasis_log_t* log, lsn_t lsn);

  /**
     Returns the LSN of the first entry of the log.  If the log is
     empty, return the LSN that will be assigned to the next log
     entry that is appended to the log.
  */
  lsn_t (*truncation_point)(struct stasis_log_t* log);
  /**
     @return 0 on success
  */
  int (*close)(struct stasis_log_t* log);

  int (*is_durable)(struct stasis_log_t* log);

  stasis_log_group_force_t * group_force;

  void* impl;
};

/**
   Synchronously make a prefix of the log durable.

   This method uses group commit to reduce the number of calls to
   force_tail().

   Durability is guaranteed in an implementation-specific way.

   @param log A log that already contains the entries to be forced to disk.
   @param lsn Log entries up to and including the one that overlaps lsn will
              be durable after this call returns.
   @param mode The durability mode associated with this call.

   @see stasis_log_force_mode_t
   @see logger2.h for information about force_tail().
 */
void stasis_log_force(stasis_log_t* log, lsn_t lsn, stasis_log_force_mode_t mode);

/**
   Inform the logging layer that a new transaction has begun, and
   obtain a handle.
*/
void stasis_log_begin_transaction(stasis_log_t* log, int xid, TransactionLog* l);

/**
   Write a transaction PREPARE to the log tail.  Blocks until the
   prepare record is stable.

   @return the lsn of the prepare log entry
*/
lsn_t stasis_log_prepare_transaction(stasis_log_t* log, TransactionLog * l);
/**
   Write a transaction COMMIT to the log tail.  Blocks until the commit
   record is stable.

   @return the lsn of the commit log entry.
*/
lsn_t stasis_log_commit_transaction(stasis_log_t* log, TransactionLog * l);

/**
   Write a transaction ABORT to the log tail.  Does not force the log.

   @return the lsn of the abort log entry.
*/
lsn_t stasis_log_abort_transaction(stasis_log_t* log, TransactionLog * l);

/**
   Write a end transaction record.  This entry tells recovery's undo
   phase that it may safely ignore the transaction.
*/
lsn_t stasis_log_end_aborted_transaction (stasis_log_t* log, TransactionLog * l);

/**
   stasis_log_write_update writes an UPDATELOG log record to the log tail.  It
   also interprets its operation argument to the extent necessary for
   allocating and laying out the log entry.  Finally, it updates the
   state of the parameter l.
*/
LogEntry * stasis_log_write_update(stasis_log_t* log,
                     TransactionLog * l, Page * p, unsigned int operation,
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

lsn_t stasis_log_write_dummy_clr(stasis_log_t* log, int xid,
                  lsn_t prev_lsn, lsn_t compensated_lsn);

#endif
