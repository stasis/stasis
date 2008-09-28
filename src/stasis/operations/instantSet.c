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
#include <stasis/page.h>
#include <string.h>

// XXX do not use

static int op_instant_set(const LogEntry *e, Page* p) {
  assert(e->update.arg_size >= sizeof(slotid_t) + sizeof(int64_t));
  const byte * b = getUpdateArgs(e);
  recordid rid;

  rid.page = p->id;
  rid.slot = *(slotid_t*)b;    b+=sizeof(slotid_t);
  rid.size = *(int64_t*)b;     b+=sizeof(int64_t);

  assert(e->update.arg_size == sizeof(slotid_t) + sizeof(int64_t) + rid.size);
  assert(stasis_record_type_to_size(rid.size) == rid.size);

  stasis_record_write(e->xid, p, e->LSN, rid, b);
  return 0;
}
int TinstantSet(int xid, recordid rid, const void * dat) {
  Page * p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  rid = stasis_record_dereference(xid,p,rid);
  unlock(p->rwlatch);
  releasePage(p);
  rid.size = stasis_record_type_to_size(rid.size);
  size_t sz = sizeof(slotid_t) + sizeof(int64_t) + rid.size;
  byte * const buf = malloc(sz);

  byte * b = buf;
  *(slotid_t*) b = rid.slot;    b += sizeof(slotid_t);
  *(int64_t*)  b = rid.size;    b += sizeof(int64_t);
  memcpy(b, dat, rid.size);

  Tupdate(xid,rid,buf,sz,OPERATION_INSTANT_SET);
  free(buf);
  return 0;
}

Operation getInstantSet() { 
	Operation o = {
		OPERATION_INSTANT_SET, /* id */
		OPERATION_NOOP,
		op_instant_set
	};
	return o;
}
