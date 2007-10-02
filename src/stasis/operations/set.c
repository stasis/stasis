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
#include "../page.h"
#include <string.h>
#include <assert.h>
static int operate(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
	stasis_record_write(xid, p, lsn, rid, dat);
	return 0;
}
typedef struct {
  int offset;
  int realRecordLength;
} set_range_t;

static int operateRange(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat)  {
  int diffLength = rid.size - sizeof(set_range_t);
  assert(! (diffLength % 2));
  diffLength /= 2;
  const set_range_t * range = dat;
  rid.size = range->realRecordLength;

  byte * data = (byte*)(range + 1);
  byte * tmp = malloc(rid.size);

  stasis_record_read(xid, p, rid, tmp);
  memcpy(tmp+range->offset, data, diffLength);
  stasis_record_write(xid, p, lsn, rid, tmp);

  free(tmp);
  return 0;
}

static int deOperateRange(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat)  {
  int diffLength = rid.size - sizeof(set_range_t);
  assert(! (diffLength % 2));
  diffLength /= 2;
  
  const set_range_t * range = dat;
  rid.size = range->realRecordLength;

  byte * data = (byte*)(range + 1);
  data += diffLength;
  byte * tmp = malloc(rid.size);

  stasis_record_read(xid, p, rid, tmp);
  memcpy(tmp+range->offset, data, diffLength);
  stasis_record_write(xid, p, lsn, rid, tmp);

  free(tmp);
  return 0;
}
compensated_function void TsetRange(int xid, recordid rid, int offset, int length, const void * dat) {
  Page * p;

  try {
    p = loadPage(xid, rid.page);
  } end;

  set_range_t * range = malloc(sizeof(set_range_t) + 2 * length);
  byte * record = malloc(rid.size);
  
  range->offset = offset;
  range->realRecordLength = rid.size;

  // Copy new value into log structure
  memcpy(range + 1, dat, length);
  
  // No further locking is necessary here; readRecord protects the 
  // page layout, but attempts at concurrent modification have undefined 
  // results.  (See page.c)
  stasis_record_read(xid, p, rid, record);

  // Copy old value into log structure 
  memcpy((byte*)(range + 1) + length, record+offset, length);
  
  // Pass size of range into Tupdate via the recordid.
  rid.size = sizeof(set_range_t) + 2 * length;

  free(record);
  /** @todo will leak 'range' if interrupted with pthread_cancel */
  begin_action(releasePage, p) {
    Tupdate(xid, rid, range, OPERATION_SET_RANGE);
    free(range);
  } compensate;
  
}

Operation getSet() { 
	Operation o = {
		OPERATION_SET, /* id */
		SIZEOF_RECORD, /* use the size of the record as size of arg */
		NO_INVERSE, 
		&operate /* Function */
	};
	return o;
}

Operation getSetRange() {
	Operation o = {
		OPERATION_SET_RANGE,
		SIZEOF_RECORD,
		OPERATION_SET_RANGE_INVERSE,
		&operateRange
	};
	return o;
}

Operation getSetRangeInverse() {
	Operation o = {
		OPERATION_SET_RANGE_INVERSE,
		SIZEOF_RECORD,
		OPERATION_SET_RANGE,
		&deOperateRange
	};
	return o;
}
