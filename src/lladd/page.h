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

/**
 * @file
 *
 * interface for dealing with slotted pages
 *
 * @ingroup LLADD_CORE
 * $Id$
 * 
 * @todo update docs in this file.
 **/

#ifndef __PAGE_H__
#define __PAGE_H__

#include <config.h>
#include <lladd/common.h>
#include "latches.h"
/** @todo page.h includes things that it shouldn't!  (Or, page.h shouldn't be an installed header.) */

#include <lladd/transactional.h>


BEGIN_C_DECLS

/** 
    The page type contains in-memory information about pages.  This
    information is used by LLADD to track the page while it is in
    memory, and is never written to disk.

    In particular, our current page replacement policy requires two doubly
    linked lists, 

    @todo In general, we pass around page structs (as opposed to page
    pointers).  This is starting to become cumbersome, as the page
    struct is becoming more complex...)
*/
typedef struct Page_s {
  /** @todo Shouldn't Page.id be a long? */
  int id;
  /** @todo The Page.LSN field seems extraneous.  Why do we need it? */
  long LSN;
  byte *memAddr;
  /** @todo dirty pages currently aren't marked dirty! */
  int dirty;
  /** The next item in the replacement policy's queue */
  struct Page_s *next;
  /** The previous item in the replacement policy's queue. */
  struct Page_s *prev; 
  /** Which queue is the page in? */
  int queue; 
  /** Is the page in the cache at all? */
  int inCache;

  /** Used for page-level latching.
      
      Each page has an associated read/write lock.  This lock only
      protects the internal layout of the page, and the members of the
      page struct.  Here is how it is held in various circumstances:
      
      Record allocation:  Write lock
      Record read:        Read lock
      Read LSN            Read lock
      Record write       *READ LOCK*
      Write LSN           Write lock

      Since the bufferManager re-uses page structs, this lock is also
      held when the page is being read or written to disk:

      Write page to disk    Write lock
      Read page from disk   Write lock
      
      Any circumstance where one these locks are held during an I/O
      operation is a bug.  (Unless the I/O operation is reading or
      writing the locked page to disk)
  */
  
  rwl * rwlatch;

  rwl * loadlatch;

  /** This mutex protects the pending field.  We don't use rwlatch for
      this, since we also need to use a condition variable to update
      this properly, and there are no read-only functions for the
      pending field. */

  pthread_mutex_t pending_mutex;  /* pthread_mutex_t */

  pthread_cond_t  noMorePending;  /* pthread_cond_t */

  int waiting; 
  
  /** 
      In the multi-threaded case, before we steal a page, we need to
      know that all pending actions have been completed.  Here, we
      track that on a per-resident page basis, by incrementing the
      pending field each time we generate a log entry that will result
      in a write to the corresponding page.

      (For a concrete example of why this is needed, imagine two
      threads write to different records on the same page, and get
      LSN's 1 and 2.  If 2 happens to write first, then the page is
      stolen, and then we crash, recovery will not know that the page
      does not reflect LSN 1.)

      "Pending events" are calls to functions that take lsn's.
      Currently, those functions are writeRecord and pageSlotRalloc.

      @todo work out what happens with kickPage() and loadPage() more
      carefully.

  */
  int pending;
} Page;

/**
 * initializes all the important variables needed in all the
 * functions dealing with pages.
 */
void pageInit();

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page.  The Page struct contains the new LSN and the page
 * number to which the new LSN must be written to.
 */
/*void pageWriteLSN(Page page);*/

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t pageReadLSN(const Page * page);

/**
 * assumes that the page is already loaded in memory.  It takes as a
 * parameter a Page, and returns an estimate of the amount of free space on this
 * page.  This is either exact, or an underestimate.
 */
int freespace(Page * page);

/**
 * assumes that the page is already loaded in memory.  It takes as
 * parameters a Page and the size in bytes of the new record.  pageRalloc()
 * returns a recordid representing the newly allocated record.
 *
 * If you call this function, you probably need to be holding lastFreepage_mutex.
 *
 * @see lastFreepage_mutex
 *
 * NOTE: might want to pad records to be multiple of words in length, or, simply
 *       make sure all records start word aligned, but not necessarily having 
 *       a length that is a multiple of words.  (Since Tread(), Twrite() ultimately 
 *       call memcpy(), this shouldn't be an issue)
 *
 * NOTE: pageRalloc() assumes that the caller already made sure that sufficient
 * amount of freespace exists in this page.  (@see freespace())
 *
 * @todo Makes no attempt to reuse old recordid's.
 */
recordid pageRalloc(Page * page, int size);

void pageDeRalloc(Page * page, recordid rid);

void pageWriteRecord(int xid, Page * page, recordid rid, lsn_t lsn, const byte *data);

void pageReadRecord(int xid, Page * page, recordid rid, byte *buff);

void pageCommit(int xid);

void pageAbort(int xid);

void pageReallocNoLock(Page * p, int id);
/** @todo Do we need a locking version of pageRealloc? */
void pageRealloc(Page * p, int id);

Page* pageAlloc(int id);

recordid pageSlotRalloc(Page * page, lsn_t lsn, recordid rid);

/*int pageTest(); */

int pageGetSlotType(Page * p, int slot, int type);
void pageSetSlotType(Page * p, int slot, int type);


END_C_DECLS

#endif
