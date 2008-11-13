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
#include <string.h>
#include <stdio.h>

#include <stasis/operations.h>
#include <stasis/logger/logger2.h>
#include <stasis/bufferManager.h>

#include <stasis/page.h>

Operation operationsTable[MAX_OPERATIONS];

void doUpdate(const LogEntry * e, Page * p) {
  assert(p);
  assertlocked(p->rwlatch);

  operationsTable[e->update.funcID].run(e, p);

  DEBUG("OPERATION xid %d Do, %lld {%lld:%lld}\n", e->xid,
         e->LSN, e->update.page, stasis_page_lsn_read(p));

  stasis_page_lsn_write(e->xid, p, e->LSN);

}

void redoUpdate(const LogEntry * e) {
  // Only handle update log entries
  assert(e->type == UPDATELOG);
  // If this is a logical operation, something is broken
  assert(e->update.page != INVALID_PAGE);

  if(operationsTable[operationsTable[e->update.funcID].id].run == noop) 
    return;

  Page * p = loadPage(e->xid, e->update.page);
  writelock(p->rwlatch,0);
  if(stasis_page_lsn_read(p) < e->LSN) {
    DEBUG("OPERATION xid %d Redo, %lld {%lld:%lld}\n", e->xid,
           e->LSN, e->update.page, stasis_page_lsn_read(p));
    // Need to check the id field to find out what the REDO_action
    // is for this log type.

    // contrast with doUpdate(), which doesn't check the .id field.
    operationsTable[operationsTable[e->update.funcID].id]
      .run(e,p);
    stasis_page_lsn_write(e->xid, p, e->LSN);
  } else {
    DEBUG("OPERATION xid %d skip redo, %lld {%lld:%lld}\n", e->xid,
           e->LSN, e->update.page, stasis_page_lsn_read(p));
  }
  unlock(p->rwlatch);
  releasePage(p);
}

void undoUpdate(const LogEntry * e, lsn_t effective_lsn, Page * p) {
  // Only handle update entries
  assert(e->type == UPDATELOG);

  int undo = operationsTable[e->update.funcID].undo;

  if(e->update.page == INVALID_PAGE) {
    // logical undos are excuted unconditionally.

    DEBUG("OPERATION xid %d FuncID %d Undo, %d LSN %lld {logical}\n", e->xid,
          e->update.funcID, undo, e->LSN);

    operationsTable[undo].run(e,0);
  } else {
    assert(p->id == e->update.page);

    if(stasis_page_lsn_read(p) < effective_lsn) {
      DEBUG("OPERATION xid %d Undo, %lld {%lld:%lld}\n", e->xid,
             e->LSN, e->update.page, stasis_page_lsn_read(p));
      operationsTable[undo].run(e,p);
      stasis_page_lsn_write(e->xid, p, effective_lsn);
    } else {
      DEBUG("OPERATION xid %d skip undo, %lld {%lld:%lld}\n", e->xid,
             e->LSN, e->update.page, stasis_page_lsn_read(p));
    }
  }
}
