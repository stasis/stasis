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
 * External interaface to Logging API.  (Only exposes operations used
 * to add entries to the log.)
 *
 * Logger is the front end for logging actions -- whatever must be done (ie
 * write to the log tail; flushing for commits) is handled in these functions
 *
 * @deprecated 
 * @see logger2.h
 *
 * @ingroup LLADD_CORE
 * $Id$
 * 
 * *************/


#ifndef __LOGGER_H__
#define __LOGGER_H__

#include "transactional.h"
#include "operations.h"

/**
  Does NOT write a transaction begin; rather, just returns that the
  LSN of a potential entry is -1 so the next command will have a
  prevLSN of -1.  (Althoug this is currently a no-op, it's possible
  that some other logging scheme would actually write begin records.)
*/
long LogTransBegin(Transaction t);

/**
 * logs the fact that a rid has been allocated for a transaction
 */
long LogTransAlloc(long prevLSN, int xid, recordid rid);

/**
  Write a transaction COMMIT to the log tail, then flush the log tail immediately to disk

  @return the LSN of this entry
*/
long LogTransCommit(long prevLSN, int xid);

/**
  Write a transaction ABORTto the log tail

  @return returns the LSN of this entry
*/
long LogTransAbort(long prevLSN, int xid);

/**
  LogUpdate writes an UPDATE log record to the log tail

  returns the LSN of this entry
*/
long LogUpdate (long prevLSN, int xid, recordid rid, Operation op, const void *args);

/**
   Write a compensation log record.  These records are used to allow
   for efficient recovery, and possibly for log truncation.  They
   record the completion of undo operations.
*/
long LogCLR (long prevLSN, int xid, long ulLSN, recordid ulRID, long ulPrevLSN);

/**
   Write a end transaction record @ todo What does this do exactly?  Indicate completion of aborts?
*/
long LogEnd (long prevLSN, int xid);

/*
  Starts a new log stream, possibly other stuff can go here too?
*/
void logInit();

/*
  Called when ALL transactions are guaranteed to be completed (either
  committed or aborted) and no new ones can be had. So therefore we
  can close the log streamer and delete the log file.  @todo Doesn't
  delete logs right now.  (For debugging)
*/
void logDeinit();

#endif
