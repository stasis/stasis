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
#include <stasis/logger/logger2.h>

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
recordid prepare_bogus_rec  = { 0, 0, 0};

static int op_prepare(const LogEntry * e, Page * p) {
  //NO-OP.
  return 0;
}

stasis_operation_impl stasis_op_impl_prepare() {
  stasis_operation_impl o = {
    OPERATION_PREPARE, /* id */
    UNKNOWN_TYPE_PAGE,
    OPERATION_PREPARE,
    OPERATION_NOOP,
    &op_prepare/* Function */
  };
  return o;
}

/** PrepareGuardState is 1 if the iterator should continue on the next
    (previous) log entry, 0 otherwise. */
typedef struct{
  int continueIterating;
  int prevLSN;
  int xid;
  int aborted;
} PrepareGuardState;

void * getPrepareGuardState() {
  PrepareGuardState * s = malloc (sizeof(PrepareGuardState));
  s->continueIterating = 1;
  s->prevLSN = -1;
  s->xid = -1;
  s->aborted = 0;
  return s;
}


int prepareGuard(const LogEntry * e, void * state) {
  PrepareGuardState * pgs = state;
  int ret = pgs->continueIterating;
  if(e->type == UPDATELOG && !pgs->aborted) {
    if(e->update.funcID == OPERATION_PREPARE) {
      pgs->continueIterating = 0;
      pgs->prevLSN           = e->prevLSN;
    }
  } else if (e->type == XABORT) {
    printf("xid %d aborted.\n", e->xid);
    pgs->aborted = 1;
  }
  if(pgs->xid == -1) {
    pgs->xid = e->xid;
  } else {
    assert(pgs->xid == e->xid);
  }

  return ret;
}

/** @todo When fleshing out the logHandle's prepareAction interface,
    figure out what the return value should mean... */
int prepareAction(void * state) {
  PrepareGuardState * pgs = state;
  int ret;
  if(!(pgs->continueIterating || pgs->aborted)) {
    //assert(pgs->prevLSN != -1);
    abort();
    //    Trevive(pgs->xid, pgs->prevLSN);
    ret = 1;
  } else {
    ret = 0;
  }
  return ret;
}
