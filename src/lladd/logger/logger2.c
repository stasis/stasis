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

#include <lladd/bufferManager.h>
#include <lladd/logger/logger2.h>
#include <malloc.h>

TransactionLog LogTransBegin(int xid) {
  TransactionLog tl;
  tl.xid = xid;
  
  DEBUG("Log Begin %d\n", xid);
  tl.prevLSN = -1;
  return tl;
}

static void LogTransCommon(TransactionLog * l, int type) {
  LogEntry * e = allocCommonLogEntry(l->prevLSN, l->xid, type);
  writeLogEntry(e);
  l->prevLSN = e->LSN;
  DEBUG("Log Common %d, LSN: %ld type: %ld (prevLSN %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);
  free(e);
}

void LogTransCommit(TransactionLog * l) {
  LogTransCommon(l, XCOMMIT);
}

void LogTransAbort(TransactionLog * l) {
  LogTransCommon(l, XABORT);
}

LogEntry * LogUpdate(TransactionLog * l, recordid rid, int operation, const byte * args) {
  void * preImage = NULL;
  size_t argSize  = 0;
  LogEntry * e;

  if(operationsTable[operation].sizeofData == SIZEOF_RECORD) {
    argSize = rid.size;
  } else {
    argSize = operationsTable[operation].sizeofData;
  }

  if(operationsTable[operation].undo == NO_INVERSE) {
    preImage = malloc(rid.size);
    readRecord(l->xid, rid, preImage);
  }

  e = allocUpdateLogEntry(l->prevLSN, l->xid, operation, rid, args, argSize, preImage);
  writeLogEntry(e);
  DEBUG("Log Common %d, LSN: %ld type: %ld (prevLSN %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  if(preImage) {
    free(preImage);
  }

  l->prevLSN = e->LSN;
  return e;
}

lsn_t LogCLR(/*TransactionLog * l,*/ LogEntry * undone) {
  lsn_t ret;
  LogEntry * e = allocCLRLogEntry(-1, undone->xid, undone->LSN, undone->contents.update.rid, undone->prevLSN);
  writeLogEntry(e);
  DEBUG("Log CLR %d, LSN: %ld type: %ld (undoing: %ld, next to undo: %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)undone->LSN, (long int)undone->prevLSN);

  ret = e->LSN;
  free(e);
  return ret;
}
