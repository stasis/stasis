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

#include <lladd/logger/logHandle.h>
#include <lladd/logger/logWriter.h>
#include <config.h> 
#include <malloc.h>

/**
   Sets the next and prev field of h, but does not set h.file_offset.
   That should probably be set before calling this function.
*/

static void set_offsets(LogHandle * h, LogEntry * e, lsn_t lastRead);

/*-------------------------------------------------------*/

LogHandle getLogHandle() {

  lsn_t lsn = firstLogEntry();

  return getGuardedHandle(lsn, NULL, NULL);
}

LogHandle getLSNHandle(lsn_t lsn) {
  return getGuardedHandle(lsn, NULL, NULL);
}

LogHandle getGuardedHandle(lsn_t lsn, guard_fcn_t * guard, void * guard_state) {
  LogHandle ret;
  ret.next_offset = lsn;
  ret.prev_offset = lsn;
  ret.guard = guard;
  ret.guard_state = guard_state;
  return ret;
}

LogEntry * nextInLog(LogHandle * h) {
  LogEntry * ret = readLSNEntry(h->next_offset);
  if(ret != NULL) {
    set_offsets(h, ret, h->next_offset);
  }

  if(h->guard) {
    if(!(h->guard(ret, h->guard_state))) {
      free(ret);
      ret = NULL;
    }
  }


  return ret;
}

LogEntry * previousInTransaction(LogHandle * h) {
  LogEntry * ret = NULL;
  if(h->prev_offset > 0) {
    /* printf("A");  fflush(NULL); */
    ret = readLSNEntry(h->prev_offset);
    set_offsets(h, ret, h->prev_offset);
    /*printf("B");  fflush(NULL); */

    if(h->guard) {
      /*printf("C");  fflush(NULL);*/

      if(!h->guard(ret, h->guard_state)) {
	free(ret);
	ret = NULL;
      }
      /*printf("D");  fflush(NULL);*/

    }
  }

  return ret;

}

/**
   @todo The next_offset field is set in a way that assumes a
   particular layout of log entries.  If we want to support other
   loggers, then the lsn of the next entry should be calculated by the
   logging implementation, not here.  (One possibility is to have
   readLSNEntry return it explicitly.)
*/
static void set_offsets(LogHandle * h, LogEntry * e, lsn_t lastRead) {
  h->next_offset = lastRead + sizeofLogEntry(e)+sizeof(lsn_t);
  h->prev_offset = (e->type==CLRLOG) ? e->contents.clr.undoNextLSN : e->prevLSN ;
}

