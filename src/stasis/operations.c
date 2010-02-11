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
#include <stasis/operations.h>
#include <stasis/logger/logEntry.h>
#include <stasis/page.h>

#include <assert.h>

static stasis_operation_impl stasis_operation_table[MAX_OPERATIONS];

void stasis_operation_impl_register(stasis_operation_impl o) {
  if(stasis_operation_table[o.id].id != OPERATION_INVALID) {
    stasis_operation_impl old = stasis_operation_table[o.id];
    assert(o.id == old.id);
    assert(o.redo==old.redo && o.undo==old.undo && o.run==old.run);
  }
  stasis_operation_table[o.id] = o;
}
static int stasis_operations_initted = 0;
void stasis_operation_table_init() {
  if(!stasis_operations_initted) {
    stasis_operations_initted = 1;
    for(int i = 0; i < MAX_OPERATIONS; i++) {
      stasis_operation_table[i].id = OPERATION_INVALID;
      stasis_operation_table[i].redo = OPERATION_INVALID;
      stasis_operation_table[i].undo = OPERATION_INVALID;
      stasis_operation_table[i].run = NULL;
    }
  }

  stasis_operation_impl_register(stasis_op_impl_set());
  stasis_operation_impl_register(stasis_op_impl_set_inverse());

  stasis_operation_impl_register(stasis_op_impl_increment());
  stasis_operation_impl_register(stasis_op_impl_decrement());

  stasis_operation_impl_register(stasis_op_impl_alloc());

  stasis_operation_impl_register(stasis_op_impl_prepare());

  stasis_operation_impl_register(stasis_op_impl_lsn_free_set());
  stasis_operation_impl_register(stasis_op_impl_lsn_free_set_inverse());
  // placeholder
  // placeholder
  stasis_operation_impl_register(stasis_op_impl_dealloc());
  stasis_operation_impl_register(stasis_op_impl_realloc());

  stasis_operation_impl_register(stasis_op_impl_page_set_range());
  stasis_operation_impl_register(stasis_op_impl_page_set_range_inverse());

  stasis_operation_impl_register(stasis_op_impl_noop());

  stasis_operation_impl_register(stasis_op_impl_array_list_header_init());
  stasis_operation_impl_register(stasis_op_impl_page_initialize());

  stasis_operation_impl_register(stasis_op_impl_set_range());
  stasis_operation_impl_register(stasis_op_impl_set_range_inverse());

  stasis_operation_impl_register(stasis_op_impl_linked_list_insert());
  stasis_operation_impl_register(stasis_op_impl_linked_list_remove());

  stasis_operation_impl_register(stasis_op_impl_linear_hash_insert());
  stasis_operation_impl_register(stasis_op_impl_linear_hash_remove());

  stasis_operation_impl_register(stasis_op_impl_boundary_tag_alloc());

  // place holder

  stasis_operation_impl_register(stasis_op_impl_region_alloc());
  stasis_operation_impl_register(stasis_op_impl_region_alloc_inverse());

  stasis_operation_impl_register(stasis_op_impl_region_dealloc());
  stasis_operation_impl_register(stasis_op_impl_region_dealloc_inverse());

  stasis_operation_impl_register(stasis_op_impl_segment_file_pwrite());
  stasis_operation_impl_register(stasis_op_impl_segment_file_pwrite_inverse());
}



void stasis_operation_do(const LogEntry * e, Page * p) {

  if(p) assertlocked(p->rwlatch);
  assert(e->update.funcID != OPERATION_INVALID);
  stasis_operation_table[e->update.funcID].run(e, p);

  DEBUG("OPERATION xid %d Do, %lld {%lld:%lld}\n", e->xid,
      e->LSN, e->update.page, p ? stasis_page_lsn_read(p) : -1);

  if(p) stasis_page_lsn_write(e->xid, p, e->LSN);

}

void stasis_operation_redo(const LogEntry * e, Page * p) {
  // Only handle update log entries
  assert(e->type == UPDATELOG);
  // If this is a logical operation, something is broken
  assert(e->update.page != INVALID_PAGE);
  assert(e->update.funcID != OPERATION_INVALID);
  assert(stasis_operation_table[e->update.funcID].redo != OPERATION_INVALID);

  if(stasis_operation_table[e->update.funcID].redo == OPERATION_NOOP) {
    return;
  }
  if((!p) || stasis_page_lsn_read(p) < e->LSN ||
     e->update.funcID == OPERATION_SET_LSN_FREE ||
     e->update.funcID == OPERATION_SET_LSN_FREE_INVERSE) {
    DEBUG("OPERATION xid %d Redo, %lld {%lld:%lld}\n", e->xid,
           e->LSN, e->update.page, stasis_page_lsn_read(p));
    // Need to check the id field to find out what the REDO_action
    // is for this log type.

    // contrast with stasis_operation_do(), which doesn't check the .redo field
    stasis_operation_table[stasis_operation_table[e->update.funcID].redo]
      .run(e,p);
    if(p) stasis_page_lsn_write(e->xid, p, e->LSN);
  } else {
    DEBUG("OPERATION xid %d skip redo, %lld {%lld:%lld}\n", e->xid,
           e->LSN, e->update.page, stasis_page_lsn_read(p));
  }
}

void stasis_operation_undo(const LogEntry * e, lsn_t effective_lsn, Page * p) {
  // Only handle update entries
  assert(e->type == UPDATELOG);

  assert(e->update.funcID != OPERATION_INVALID);
  int undo = stasis_operation_table[e->update.funcID].undo;
  assert(undo != OPERATION_INVALID);

  if(e->update.page == INVALID_PAGE || e->update.page == SEGMENT_PAGEID) {
    // logical undos are executed unconditionally, as are segment-based undos

    DEBUG("OPERATION xid %d FuncID %d Undo, %d LSN %lld {logical}\n", e->xid,
          e->update.funcID, undo, e->LSN);

    stasis_operation_table[undo].run(e,0);
  } else {
    assert(p->id == e->update.page);

    if(stasis_page_lsn_read(p) < effective_lsn) {
      DEBUG("OPERATION xid %d Undo, %lld {%lld:%lld}\n", e->xid,
             e->LSN, e->update.page, stasis_page_lsn_read(p));
      stasis_operation_table[undo].run(e,p);
      stasis_page_lsn_write(e->xid, p, effective_lsn);
    } else {
      DEBUG("OPERATION xid %d skip undo, %lld {%lld:%lld}\n", e->xid,
             e->LSN, e->update.page, stasis_page_lsn_read(p));
    }
  }
}
pagetype_t stasis_operation_type(int op) {
  return stasis_operation_table[op].page_type;
}
