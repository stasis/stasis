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
#include <lladd/operations.h>

#include "logger/logWriter.h"
#include <lladd/bufferManager.h>
#include <assert.h>
#include <string.h>
/** @todo questionable include */
#include "page.h"

#include <stdio.h>

Operation operationsTable[MAX_OPERATIONS];

void doUpdate(const LogEntry * e, Page * p) {


  DEBUG("OPERATION update arg length %d, lsn = %ld\n", e->contents.update.argSize, e->LSN);

  operationsTable[e->contents.update.funcID].run(e->xid, p, e->LSN, e->contents.update.rid, getUpdateArgs(e));

}

void redoUpdate(const LogEntry * e) {
  if(e->type == UPDATELOG) {
    /*    lsn_t pageLSN = readLSN(e->contents.update.rid.page); */
    recordid rid = e->contents.update.rid;
    Page * p = loadPage(rid.page);
    lsn_t pageLSN = pageReadLSN(p);

    if(e->LSN > pageLSN) {
      DEBUG("OPERATION Redo, %ld > %ld {%d %d %ld}\n", e->LSN, pageLSN, rid.page, rid.slot, rid.size);
      doUpdate(e, p);
    } else {
      DEBUG("OPERATION Skipping redo, %ld <= %ld {%d %d %ld}\n", e->LSN, pageLSN, rid.page, rid.slot, rid.size);
    }

    releasePage(p);
  } else if(e->type == CLRLOG) {
    LogEntry * f = readLSNEntry(e->contents.clr.thisUpdateLSN);
    recordid rid = f->contents.update.rid;
    Page * p = loadPage(rid.page);

    assert(rid.page == e->contents.update.rid.page); /* @todo Should this always hold? */

    /* See if the page contains the result of the undo that this CLR is supposed to perform. If it
       doesn't, then undo the original operation. */
    /*    if(f->LSN > pageReadLSN(e->contents.update.rid.page)) { */
    if(f->LSN > pageReadLSN(p)) {

      DEBUG("OPERATION Undoing for clr, %ld {%d %d %ld}\n", f->LSN, rid.page, rid.slot, rid.size);
      undoUpdate(f, p, e->LSN);
    } else {
      DEBUG("OPERATION Skiping undo for clr, %ld {%d %d %ld}\n", f->LSN, rid.page, rid.slot, rid.size);
    }

    releasePage(p);
  } else {
    assert(0);
  }

}


void undoUpdate(const LogEntry * e, Page * p, lsn_t clr_lsn) {

  int undo = operationsTable[e->contents.update.funcID].undo;
  DEBUG("OPERATION FuncID %d Undo op %d LSN %ld\n",e->contents.update.funcID, undo, clr_lsn);

#ifdef DEBUGGING
  recordid rid = e->contents.update.rid;
#endif
  /*  lsn_t page_lsn = readLSN(e->contents.update.rid.page); */
  lsn_t page_lsn = -1;
  if(p) {
    page_lsn = pageReadLSN(p);
  }
  if(e->LSN <= page_lsn || !p) {

    /* Actually execute the undo */
    if(undo == NO_INVERSE) {
      /* Physical undo */
      assert(p);  // Must be provided wiht a page in order to perform a physical undo!
      DEBUG("OPERATION %d Physical undo, %ld {%d %d %ld}\n", undo, e->LSN, e->contents.update.rid.page, e->contents.update.rid.slot, e->contents.update.rid.size);
      writeRecord(e->xid, p, clr_lsn, e->contents.update.rid, getUpdatePreImage(e));
    } else if(undo == NO_INVERSE_WHOLE_PAGE) {
      assert(p);
      DEBUG("OPERATION %d Whole page physical undo, %ld {%d}\n", undo, e->LSN, e->contents.update.rid.page);
      memcpy(p->memAddr, getUpdatePreImage(e), PAGE_SIZE);
      pageWriteLSN(p, clr_lsn);

    } else {
      /* @see doUpdate() */
      /*      printf("Logical undo"); fflush(NULL); */
      DEBUG("OPERATION %d Logical undo, %ld {%d %d %ld}\n", undo, e->LSN, e->contents.update.rid.page, e->contents.update.rid.slot, e->contents.update.rid.size);
      operationsTable[undo].run(e->xid, p, clr_lsn, e->contents.update.rid, getUpdateArgs(e));
    }
  } else {
    DEBUG("OPERATION %d Skipping undo, %ld {%d %d %ld}\n", undo, e->LSN, e->contents.update.rid.page, e->contents.update.rid.slot, e->contents.update.rid.size);
  }

  /*  printf("Undo done."); fflush(NULL); */

}
