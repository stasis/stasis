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

#include <lladd/common.h>

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
  recordid rid;
  unsigned int argSize;
  /* Implicit members:
     args;     @ ((byte*)ule) + sizeof(UpdateLogEntry)
     preImage; @ ((byte*)ule) + sizeof(UpdateLogEntry) + ule.argSize */
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

/**
   Allocate a log entry that does not contain any extra payload
   information.  (Eg: Tbegin, Tcommit, etc.)

   @return a LogEntry that should be freed with free().
 */
LogEntry * allocCommonLogEntry(lsn_t prevLSN, int xid, unsigned int type);
/** 
   Allocate a log entry associated with an operation implemention.  This
   is usually called inside of Tupdate().

   @return a LogEntry that should be freed with free().

*/
LogEntry * allocUpdateLogEntry(lsn_t prevLSN, int xid, 
			       unsigned int operation, recordid rid, 
			       const byte * args, unsigned int argSize, 
			       const byte * preImage);
/**
   Alloc a deferred log entry.  This is just like allocUpdateLogEntry(), except 
   the log entry's type will be DEFERLOG instead UPDATELOG.  This is usually
   called inside of Tdefer().

   @return a LogEntry that should be freed with free().

*/
LogEntry * allocDeferredLogEntry(lsn_t prevLSN, int xid, 
				 unsigned int operation, recordid rid, 
				 const byte * args, unsigned int argSize, 
				 const byte * preImage);
/**
   Allocate a CLR entry.  These are written during recovery as log
   entries are undone.  This moves undo operations into the redo
   phase, by recording the inverse of the original operation, and sets
   prevLSN to the prevLSN of old_e.

   @return a LogEntry that should be freed with free().
*/
LogEntry * allocCLRLogEntry(const LogEntry * old_e);
/** 
   @return the length, in bytes, of e.
*/
long sizeofLogEntry(const LogEntry * e);
/**
   @return the operation's arguments.
*/
const byte * getUpdateArgs(const LogEntry * e);
/**
   @return the undo information for operations that use record-based
   phsysical undo.
*/
const byte * getUpdatePreImage(const LogEntry * e);

END_C_DECLS

#endif /* __LOGENTRY_H */
