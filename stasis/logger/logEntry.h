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

#ifndef __LLADD_LOGGING_LOGENTRY_H
#define __LLADD_LOGGING_LOGENTRY_H

#include <stasis/common.h>

BEGIN_C_DECLS

/**
   @file

   Structs and memory managment routines for log entries

   @todo Other than some typedefs, is there anything in logEntry that belongs in the API?

   @ingroup LLADD_CORE

   $Id$
*/

typedef struct {
  unsigned int funcID : 8;
  pageid_t page;
  int64_t  arg_size;
  /* Implicit members:
     args;     @ ((byte*)ule) + sizeof(UpdateLogEntry)
  */
} UpdateLogEntry;

struct __raw_log_entry {
  lsn_t LSN;
  lsn_t prevLSN;
  int   xid;
  unsigned int type;
};

typedef struct {
  lsn_t LSN;
  lsn_t prevLSN;
  int   xid;
  unsigned int type;
  UpdateLogEntry update;
} LogEntry;

typedef struct __raw_log_entry CLRLogEntry;
/**
   Allocate a log entry that does not contain any extra payload
   information.  (Eg: Tbegin, Tcommit, etc.)

   @return a LogEntry that should be freed with free().
 */
LogEntry * allocCommonLogEntry(lsn_t prevLSN, int xid, unsigned int type);

LogEntry * allocPrepareLogEntry(lsn_t prevLSN, int xid, lsn_t recLSN);
/**
   Allocate a log entry associated with an operation implemention.  This
   is usually called inside of Tupdate().

   @return a LogEntry that should be freed with free().

*/
LogEntry * allocUpdateLogEntry(lsn_t prevLSN, int xid,
			       unsigned int op, pageid_t page,
			       const byte * arg, unsigned int arg_size);

/**
   Allocate a CLR entry.  These are written during recovery as log
   entries are undone.  This moves undo operations into the redo
   phase, by recording the inverse of the original operation, and sets
   prevLSN to the prevLSN of old_e.

   @return a LogEntry that should be freed with free().
*/
LogEntry * allocCLRLogEntry(const LogEntry * e);
/**
   @param e a log entry returned from one of the alloc???LogEntry functions.
 */
void freeLogEntry(const LogEntry * e);
/**
   @return the length, in bytes, of e.
*/
lsn_t sizeofLogEntry(const LogEntry * e);
/**
   @todo Remove explicit casts from getUpdateArgs calls (so we don't accidentally strip the const).
   @return the operation's arguments.
*/
const void * getUpdateArgs(const LogEntry * e);

/**
	@return a copy of the log entry that this CLR compensated.
 */
static inline const LogEntry * getCLRCompensated(const LogEntry * e) {
	return (const LogEntry*)(
			((const struct __raw_log_entry*)e)+1
			);
}

lsn_t getPrepareRecLSN(const LogEntry *e);

END_C_DECLS

#endif /* __LOGENTRY_H */
