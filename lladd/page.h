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
 * interface for dealing with pages
 *
 * @ingroup LLADD_CORE
 * $Id$
 * 
 * @todo update docs in this file.
 **/

#ifndef __PAGE_H__
#define __PAGE_H__

#include "common.h"

BEGIN_C_DECLS

#include <stddef.h>
/*#include <pbl/pbl.h> */

/**
 * represents how to look up a record on a page
 */
typedef struct {
  int page;
  int slot;
  size_t size;
} recordid;


typedef struct Page_s {
	int id;
	long LSN;
	byte *memAddr;
	int dirty;
	struct Page_s *next;
        /** for replacement policy */
	struct Page_s *prev; 
        /** this too */
	int queue; 
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
void pageWriteLSN(Page page);

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
long pageReadLSN(Page page);

/**
 * assumes that the page is already loaded in memory.  It takes as a
 * parameter a Page, and returns an estimate of the amount of free space on this
 * page.  This is either exact, or an underestimate.
 */
size_t freespace(Page page);

/**
 * assumes that the page is already loaded in memory.  It takes as
 * parameters a Page and the size in bytes of the new record.  pageRalloc()
 * returns a recordid representing the newly allocated record.
 */
recordid pageRalloc(Page page, size_t size);

void pageWriteRecord(int xid, Page page, recordid rid, const byte *data);

void pageReadRecord(int xid, Page page, recordid rid, byte *buff);

void pageCommit(int xid);

void pageAbort(int xid);

void pageRealloc(Page *p, int id);

Page* pageAlloc(int id);

recordid pageSlotRalloc(Page page, lsn_t lsn, recordid rid);

int pageTest();

int getSlotType(Page p, int slot, int type);
void setSlotType(Page p, int slot, int type);


END_C_DECLS

#endif
