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
#include <lladd/common.h>

#include <lladd/transactional.h>

#include <assert.h>
#include <lladd/operations.h>

LogEntry * allocCommonLogEntry(lsn_t prevLSN, int xid, unsigned int type) {
  LogEntry * ret = malloc(sizeof(struct __raw_log_entry));
  ret->LSN     = -1;
  ret->prevLSN = prevLSN;
  ret->xid     = xid;
  ret->type    = type;
  return ret;
}

const byte * getUpdateArgs(const LogEntry * ret) {
  assert(ret->type == UPDATELOG);
  if(ret->contents.update.argSize == 0) {
    return NULL;
  } else {
    return ((byte*)ret) + sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry);
  }
}

const byte * getUpdatePreImage(const LogEntry * ret) {
  assert(ret->type == UPDATELOG);
  if(operationsTable[ret->contents.update.funcID].undo != NO_INVERSE) {
    return NULL;
  } else {
    return ((byte*)ret) + sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + ret->contents.update.argSize;
  }
}

LogEntry * allocUpdateLogEntry(lsn_t prevLSN, int xid, 
			       unsigned int funcID, recordid rid, 
			       const byte * args, unsigned int argSize, const byte * preImage) {
  int invertible = operationsTable[funcID].undo != NO_INVERSE;

  LogEntry * ret = malloc(sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + argSize + ((!invertible) ? rid.size : 0));
  ret->LSN = -1;
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = UPDATELOG;
  ret->contents.update.funcID = funcID;
  ret->contents.update.rid    = rid;
  ret->contents.update.argSize = argSize;
  
  if(argSize) {
    memcpy((void*)getUpdateArgs(ret), args, argSize);
  } 
  if(!invertible) {
    memcpy((void*)getUpdatePreImage(ret), preImage, rid.size);
  }

  return ret;

}

LogEntry * allocCLRLogEntry   (lsn_t prevLSN, int xid, 
			       lsn_t thisUpdateLSN, recordid rid, lsn_t undoNextLSN) {
  LogEntry * ret = malloc(sizeof(struct __raw_log_entry) + sizeof(CLRLogEntry));
  ret->LSN = -1;
  ret->prevLSN = prevLSN;
  ret->xid = xid;
  ret->type = CLRLOG;

  ret->contents.clr.thisUpdateLSN = thisUpdateLSN;
  ret->contents.clr.rid           = rid;
  ret->contents.clr.undoNextLSN   = undoNextLSN;
  
  return ret;
}



long sizeofLogEntry(const LogEntry * log) {
  switch (log->type) {
  case CLRLOG:
    return sizeof(struct __raw_log_entry) + sizeof(CLRLogEntry);
  case UPDATELOG: 
    return sizeof(struct __raw_log_entry) + sizeof(UpdateLogEntry) + log->contents.update.argSize + 
      ((operationsTable[log->contents.update.funcID].undo == NO_INVERSE) ? log->contents.update.rid.size : 0);
  default:
    return sizeof(struct __raw_log_entry);
  }
}

