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
 * New version of logstreamer; designed to work with logEntry, and has
 * a simplified API.
 *
 * logstreamer is the implementation of writing the log tail
 * It must be bufferred -- in that when something is written to the log tail it
 * is not immediately written to disk, but rather just to memory. But
 * logstreamer must be able to force flush to disk, which will be done when a
 * commit log entry is written to the log tail
 * 
 * Note: using the stdio FILEs for this, and by default it is "fully" buffered.
 * The log tail may be flushed to disk without an explicit call to fflush (when
 * the program terminates, the file closes), but this is acceptable because it
 * never hurts to have more flushes to disk, as long as it doesn't hurt
 * performance.
 *
 * @todo Everything in this file cores on failure (no error handling yet)
 * @todo logWriter is optimized for forward operation, scanning backwards 
 *       (as in abort()) is very slow.  Perhaps we should maintian a 
 *       'write behind' buffer?
 *
 * $Id$
 * 
 */

#ifndef __LOGWRITER_H__
#define __LOGWRITER_H__

/*#include "logEntry.h"*/
#include <lladd/constants.h>
#include <lladd/common.h>

BEGIN_C_DECLS
/**
  start a new log stream by opening the log file for reading

  returns 0 on success, or an error code define above

*/
int openLogWriter();

/**
   
  @param e Pointer to a log entry.  After the call, e->LSN will be set appropriately.  If e's xid is set to -1, then this call has no effect (and e's LSN will be set to -1.)

  returns 0 on success, or an error code defined above
*/
int writeLogEntry(LogEntry * e);

/**
  flush the entire log (tail) that is currently in memory to disk
*/
void syncLog();

/** 
   Return the highest LSN that is known to be on disk.  (Currently, we
   only know if an LSN is on disk if we've written that LSN before a
   call to syncLog().

   Note: This function might not return an LSN corresponding to a real
   log entry, but it will definitely return one that is greater than
   or equal to the LSN of a log entry that has been forced to disk,
   and is less than the LSN of all log entries that might not have
   been forced to disk.
*/
lsn_t flushedLSN();

/**
   Truncates the log file.  In the single-threaded case, this works as
   follows:

   First, the LSN passed to this function, minus sizeof(lsn_t) is
   written to a new file, called logfile.txt~.  (If logfile.txt~
   already exists, then it is truncated.)

   Next, the contents of the log, starting with the LSN passed into
   this function are copied to logfile.txt~

   Finally, logfile.txt~ is moved on top of logfile.txt

   As long as the move system call is atomic, this function should
   maintain the system's durability.

   The multithreaded case is a bit more complicated, as we need
   to deal with latching:

   With no lock, copy the log.  Upon completion, if the log has grown,
   then copy the part that remains.  Next, obtain a read/write latch
   on the logfile, and copy any remaining portions of the log.
   Perform the move, and release the latch.

*/

int truncateLog(lsn_t);


/** 
    @return The LSN of the first entry in the log file.  (If the file
    is empty, this returns the LSN of the log entry that would be
    created if writeLogEntry were called.)
*/
lsn_t firstLogEntry();
/**
  Close the log stream
*/
void closeLogWriter();

/**
  Actually deletes the log file that may have been written to disk! Danger!!
  Only use after calling closeLogStream AND you are sure there are no active (or
  future active) transactions!

  @todo This is in here now for completeness, but once we implement
  log truncation, it should leave.
*/
void deleteLogWriter();

/**
   Read a log entry at a particular LSN.

   @param LSN the LSN of the entry that will be read.
*/
LogEntry * readLSNEntry(lsn_t LSN);

END_C_DECLS

#endif /* __LLADD_LOGGER_LOGWRITER_H */

