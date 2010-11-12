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
/**********************************************
 * $Id$
 *
 * sets the given reference to dat
 **********************************************/

#include <stasis/operations.h>
#include <stasis/blobManager.h>
#include <stasis/bufferManager.h>
#include <stasis/page.h>
#include <string.h>
#include <assert.h>
static int op_set(const LogEntry *e, Page *p) {
  assert(e->update.arg_size >= sizeof(slotid_t) + sizeof(int64_t));
  const byte * b = stasis_log_entry_update_args_cptr(e);
  recordid rid;

  rid.page = p->id;
  rid.slot = *(slotid_t*)b;    b+=sizeof(slotid_t);
  rid.size = *(int64_t*)b;     b+=sizeof(int64_t);

  assert(e->update.arg_size == sizeof(slotid_t) + sizeof(int64_t) + 2 * rid.size);
  assert(stasis_record_type_to_size(rid.size) == rid.size);
  assert(stasis_record_length_read(e->xid,p,rid) == rid.size);

  stasis_record_write(e->xid, p, rid, b);

  return 0;
}
static int op_set_inverse(const LogEntry *e, Page *p) {
  assert(e->update.arg_size >= sizeof(slotid_t) + sizeof(int64_t));
  const byte * b = stasis_log_entry_update_args_cptr(e);
  recordid rid;

  rid.page = p->id;
  rid.slot = *(slotid_t*)b;    b+=sizeof(slotid_t);
  rid.size = *(int64_t*)b;     b+=sizeof(int64_t);

  assert(e->update.arg_size == sizeof(slotid_t) + sizeof(int64_t) + 2 * rid.size);
  assert(stasis_record_type_to_size(rid.size) == rid.size);

  stasis_record_write(e->xid, p, rid, b+rid.size);

  return 0;
}

typedef struct {
  int offset;
  slotid_t slot;
} set_range_t;

Page * TsetWithPage(int xid, recordid rid, Page *p, const void * dat) {
  readlock(p->rwlatch,0);
  rid = stasis_record_dereference(xid,p,rid);
  if(rid.page != p->id) {
    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid, rid.page);
    readlock(p->rwlatch,0);
  }
  short type = stasis_record_type_read(xid,p,rid);

  if(type == BLOB_SLOT) {
    stasis_blob_write(xid,p,rid,dat);
    unlock(p->rwlatch);
  } else {
    rid.size = stasis_record_type_to_size(rid.size);
    if(rid.page == p->id) {
      // failing early avoids unrecoverable logs...
      assert(rid.size == stasis_record_length_read(xid, p, rid));
    }
    unlock(p->rwlatch);

    size_t sz = sizeof(slotid_t) + sizeof(int64_t) + 2 * rid.size;
    byte * const buf = malloc(sz);

    byte * b = buf;
    *(slotid_t*) b = rid.slot;    b += sizeof(slotid_t);
    *(int64_t*)  b = rid.size;    b += sizeof(int64_t);
    memcpy(b, dat, rid.size);
    b += rid.size;
    TreadWithPage(xid, rid, p, b);

    TupdateWithPage(xid,rid.page,p,buf,sz,OPERATION_SET);
    free(buf);
  }
  return p;
}

int Tset(int xid, recordid rid, const void * dat) {
  Page * p = loadPage(xid, rid.page);
  releasePage( TsetWithPage(xid, rid, p, dat) );
  return 0;
}

int TsetRaw(int xid, recordid rid, const void * dat) {
  rid.size = stasis_record_type_to_size(rid.size);
  size_t sz = sizeof(slotid_t) + sizeof(int64_t) + 2 * rid.size;
  byte * const buf = malloc(sz);

  byte * b = buf;
  *(slotid_t*) b = rid.slot;    b += sizeof(slotid_t);
  *(int64_t*)  b = rid.size;    b += sizeof(int64_t);
  memcpy(b, dat, rid.size);
  b += rid.size;
  TreadRaw(xid, rid, b);
  Tupdate(xid,rid.page,buf,sz,OPERATION_SET);
  free(buf);
  return 0;
}

static int op_set_range(const LogEntry* e, Page* p) {
  int diffLength = e->update.arg_size - sizeof(set_range_t);
  assert(! (diffLength % 2));
  diffLength >>= 1;
  const set_range_t * range = stasis_log_entry_update_args_cptr(e);
  recordid rid;
  rid.page = p->id;
  rid.slot = range->slot;
  rid.size = stasis_record_length_read(e->xid,p,rid);

  byte * data = (byte*)(range + 1);
  byte * tmp = malloc(rid.size);

  stasis_record_read(e->xid, p, rid, tmp);

  memcpy(tmp+range->offset, data, diffLength);
  stasis_record_write(e->xid, p, rid, tmp);

  free(tmp);
  return 0;
}

static int op_set_range_inverse(const LogEntry* e, Page* p) {
  int diffLength = e->update.arg_size - sizeof(set_range_t);
  assert(! (diffLength % 2));
  diffLength >>= 1;
  const set_range_t * range = stasis_log_entry_update_args_cptr(e);
  recordid rid;
  rid.page = p->id;
  rid.slot = range->slot;
  rid.size = stasis_record_length_read(e->xid,p,rid);

  byte * data = (byte*)(range + 1) + diffLength;
  byte * tmp = malloc(rid.size);

  stasis_record_read(e->xid, p, rid, tmp);
  memcpy(tmp+range->offset, data, diffLength);
  stasis_record_write(e->xid, p, rid, tmp);

  free(tmp);
  return 0;
}
compensated_function void TsetRange(int xid, recordid rid, int offset, int length, const void * dat) {
  Page * p;

  try {
    p = loadPage(xid, rid.page);
  } end;

  ///  XXX rewrite without malloc (use read_begin, read_done)
  set_range_t * range = malloc(sizeof(set_range_t) + 2 * length);

  range->offset = offset;
  range->slot = rid.slot;

  // Copy new value into log structure
  memcpy(range + 1, dat, length);

  // No further locking is necessary here; readRecord protects the
  // page layout, but attempts at concurrent modification have undefined
  // results.  (See page.c)
  readlock(p->rwlatch,0);

  const byte* record = stasis_record_read_begin(xid,p,rid);
  // Copy old value into log structure
  memcpy((byte*)(range + 1) + length, record+offset, length);
  stasis_record_read_done(xid,p,rid,record);

  unlock(p->rwlatch);

  Tupdate(xid, rid.page, range, sizeof(set_range_t) + 2 * length, OPERATION_SET_RANGE);
  free(range);

  releasePage(p);
}

stasis_operation_impl stasis_op_impl_set() {
  stasis_operation_impl o = {
    OPERATION_SET,
    UNKNOWN_TYPE_PAGE,
    OPERATION_SET,
    OPERATION_SET_INVERSE,
    op_set
  };
  return o;
}

stasis_operation_impl stasis_op_impl_set_inverse() {
  stasis_operation_impl o = {
    OPERATION_SET_INVERSE,
    UNKNOWN_TYPE_PAGE,
    OPERATION_SET_INVERSE,
    OPERATION_SET,
    op_set_inverse
  };
  return o;
}

stasis_operation_impl stasis_op_impl_set_range() {
  stasis_operation_impl o = {
    OPERATION_SET_RANGE,
    UNKNOWN_TYPE_PAGE,
    OPERATION_SET_RANGE,
    OPERATION_SET_RANGE_INVERSE,
    op_set_range
  };
  return o;
}

stasis_operation_impl stasis_op_impl_set_range_inverse() {
  stasis_operation_impl o = {
    OPERATION_SET_RANGE_INVERSE,
    UNKNOWN_TYPE_PAGE,
    OPERATION_SET_RANGE_INVERSE,
    OPERATION_SET_RANGE,
    op_set_range_inverse
  };
  return o;
}
