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

#include <config.h>
#include <stasis/common.h>

#include <assert.h>

#include "../page.h" // For stasis_record_type_to_size()
#include <stasis/logger/logger2.h> // needed for LoggerSizeOfInternalLogEntry()
#include <stasis/logger/logEntry.h>
#include <stasis/crc32.h>
LogEntry * allocCommonLogEntry(lsn_t prevLSN, int xid, unsigned int type) {
  LogEntry * ret = calloc(1,sizeof(struct __raw_log_entry));
  ret->LSN     = -1;
  ret->prevLSN = prevLSN;
  ret->xid     = xid;
  ret->type    = type;
  return ret;
}

const byte * getUpdateArgs(const LogEntry * ret) {
  assert(ret->type == UPDATELOG || 
	 ret->type == DEFERLOG  || 
	 ret->type == CLRLOG);
  if(ret->update.argSize == 0) {
    return NULL;
  } else {
    return ((byte*)ret) + 
      sizeof(struct __raw_log_entry) + 
      sizeof(UpdateLogEntry);
  }
}

const byte * getUpdatePreImage(const LogEntry * ret) {
  assert(ret->type == UPDATELOG || 
	 ret->type == DEFERLOG  || 
	 ret->type == CLRLOG);
  if(operationsTable[ret->update.funcID].undo != NO_INVERSE && 
     operationsTable[ret->update.funcID].undo != NO_INVERSE_WHOLE_PAGE) {
    return NULL;
  } else {
    return ((byte*)ret) + 
      sizeof(struct __raw_log_entry) + 
      sizeof(UpdateLogEntry) + 
      ret->update.argSize;
  }
}

LogEntry * allocUpdateLogEntry(lsn_t prevLSN, int xid, 
			       unsigned int funcID, recordid rid, 
			       const byte * args, unsigned int argSize, 
			       const byte * preImage) {
  int invertible = operationsTable[funcID].undo != NO_INVERSE;
  int whole_page_phys = operationsTable[funcID].undo == NO_INVERSE_WHOLE_PAGE;
  
  /** Use calloc since the struct might not be packed in memory;
      otherwise, we'd leak uninitialized bytes to the log. */

  size_t logentrysize =  
    sizeof(struct __raw_log_entry) + 
    sizeof(UpdateLogEntry) + argSize +
    ((!invertible) ? stasis_record_type_to_size(rid.size) 
     : 0) + 
    (whole_page_phys ? PAGE_SIZE 
     : 0);
  LogEntry * ret = calloc(1,logentrysize);
  ret->LSN = -1;
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = UPDATELOG;
  ret->update.funcID = funcID;
  ret->update.rid    = rid;
  ret->update.argSize = argSize;
  
  if(argSize) {
    memcpy((void*)getUpdateArgs(ret), args, argSize);
  } 
  if(!invertible) {
    memcpy((void*)getUpdatePreImage(ret), preImage, 
	   stasis_record_type_to_size(rid.size));
  }
  if(whole_page_phys) {
    memcpy((void*)getUpdatePreImage(ret), preImage, 
	   PAGE_SIZE);
  }
  //assert(logentrysize == sizeofLogEntry(ret));
  // XXX checks for uninitialized values in valgrind
  //  stasis_crc32(ret, sizeofLogEntry(ret), 0);
  return ret;
}

LogEntry * allocDeferredLogEntry(lsn_t prevLSN, int xid, 
				 unsigned int funcID, recordid rid, 
				 const byte * args, unsigned int argSize, 
				 const byte * preImage) {
  LogEntry * ret = allocUpdateLogEntry(prevLSN, xid, funcID, rid, 
				       args, argSize, preImage);
  ret->type = DEFERLOG;
  return ret;
}
LogEntry * allocCLRLogEntry(const LogEntry * old_e) { 

  // Could handle other types, but we should never encounter them here.
  assert(old_e->type == UPDATELOG); 

  LogEntry * ret = calloc(1, sizeofLogEntry(old_e));
  memcpy(ret, old_e, sizeofLogEntry(old_e));
  ret->LSN = -1;
  // prevLSN is OK already
  // xid is OK already
  ret->type = CLRLOG;
  // update is also OK
  
  return ret;
}



long sizeofLogEntry(const LogEntry * log) {
  switch (log->type) {
  case CLRLOG:
  case UPDATELOG:
  case DEFERLOG: { 
    int undoType = operationsTable[log->update.funcID].undo;
    return sizeof(struct __raw_log_entry) + 
      sizeof(UpdateLogEntry) + log->update.argSize + 
      ((undoType == NO_INVERSE) ? stasis_record_type_to_size(log->update.rid.size) 
                                : 0) +
      ((undoType == NO_INVERSE_WHOLE_PAGE) ? PAGE_SIZE : 0);
  }
  case INTERNALLOG:
    return LoggerSizeOfInternalLogEntry(log);
  default:
    return sizeof(struct __raw_log_entry);
  }
}
