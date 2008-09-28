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
 * Decrements the given reference by one
 *********************************************/
#include <stasis/transactional.h>
#include <stasis/common.h>
#include <stasis/operations/nestedTopActions.h>
#include <stasis/logger/logger2.h>
#include <string.h>
#include <stdlib.h>
#include <stasis/latches.h>
#include <assert.h>
/** @todo Remove extern declaration of transactional_2_mutex from nestedTopActions.c */
extern pthread_mutex_t transactional_2_mutex;

extern TransactionLog XactionTable[];

void initNestedTopActions() {
}
void deinitNestedTopActions() {
}

typedef struct {
  lsn_t prev_lsn;
  lsn_t compensated_lsn;
} stasis_nta_handle;

/** @todo TbeginNestedTopAction's API might not be quite right.
    Are there cases where we need to pass a recordid in?

    @return a handle that must be passed into TendNestedTopAction
*/
void * TbeginNestedTopAction(int xid, int op, const byte * dat, int datSize) {
  assert(xid >= 0);
  LogEntry * e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], NULL, op, dat, datSize);
  DEBUG("Begin Nested Top Action e->LSN: %ld\n", e->LSN);
  stasis_nta_handle * h = malloc(sizeof(stasis_nta_handle));

  h->prev_lsn = e->prevLSN;
  h->compensated_lsn = e->LSN;

  FreeLogEntry(e);
  return h;
}

/**
    Call this function at the end of a nested top action.
    @return the lsn of the CLR.  Most users (everyone?) will ignore this.
*/
lsn_t TendNestedTopAction(int xid, void * handle) {
  stasis_nta_handle * h = handle;
  assert(xid >= 0);

  // Write a CLR.
  lsn_t clrLSN = LogDummyCLR(xid, h->prev_lsn, h->compensated_lsn);

  // Ensure that the next action in this transaction points to the CLR.
  XactionTable[xid % MAX_TRANSACTIONS].prevLSN = clrLSN;

  DEBUG("NestedTopAction CLR %d, LSN: %ld type: %ld (undoing: %ld, next to undo: %ld)\n", e->xid, 
	 clrLSN, undoneLSN, *prevLSN);

  free(h);

  return clrLSN;
}
