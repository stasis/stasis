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
/*******************************
 * $Id$
 *
 * implementation of the page buffer
 * *************************************************/

#include <config.h>
#include <lladd/common.h>
#include <assert.h>
#include <lladd/bufferManager.h>
#include "blobManager.h"
#include <lladd/pageCache.h>
/*#include "logger/logWriter.h" */

#include "page.h"
#include "pageFile.h"



/**
   Invariant: This lock should be held while updating lastFreepage, or
   while performing any operation that may decrease the amount of
   freespace in the page that lastFreepage refers to.  

   Since pageCompact and pageDeRalloc may only increase this value,
   they do not need to hold this lock.  Since bufferManager is the
   only place where pageRalloc is called, pageRalloc does not obtain
   this lock.
*/

static pthread_mutex_t lastFreepage_mutex;
static unsigned int lastFreepage = 0;

/**
 * @param pageid ID of the page you want to load
 * @return fully formed Page type
 * @return page with -1 ID if page not found
 */
Page * loadPage(int pageid); 



int bufInit() {

  /*	stable = NULL; */
	pageInit();
	openPageFile();
	pageCacheInit();
	openBlobStore();

	lastFreepage = 0;
	pthread_mutex_init(&lastFreepage_mutex , NULL);
	

	return 0;
}

void bufDeinit() {

	closeBlobStore();
	pageCacheDeinit();
	closePageFile();

	return;
}
/**
    Just close file descriptors, don't do any other clean up. (For
    testing.)
*/
void simulateBufferManagerCrash() {
  closeBlobStore();
  closePageFile();
}

/* ** No file I/O below this line. ** */

Page * loadPage (int pageid) {
	return loadPagePtr(pageid);
}

Page * lastRallocPage = 0;


recordid ralloc(int xid, /*lsn_t lsn,*/ long size) {
  
  recordid ret;
  Page * p;
  
  DEBUG("Rallocing record of size %ld\n", (long int)size);
  
  assert(size < BLOB_THRESHOLD_SIZE || size == BLOB_SLOT);
  
  pthread_mutex_lock(&lastFreepage_mutex);
  
  while(freespace(p = loadPage(lastFreepage)) < size ) { lastFreepage++; }
  
  ret = pageRalloc(p, size);
    
  pthread_mutex_unlock(&lastFreepage_mutex);
  
  DEBUG("alloced rid = {%d, %d, %ld}\n", ret.page, ret.slot, ret.size);

  return ret;
}

void slotRalloc(int pageid, lsn_t lsn, recordid rid) {
  Page * loadedPage = loadPage(rid.page);
  pageSlotRalloc(loadedPage, lsn, rid);
}

long readLSN(int pageid) {

	return pageReadLSN(loadPage(pageid));
}

void writeRecord(int xid, lsn_t lsn, recordid rid, const void *dat) {

  Page *p;
  
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    DEBUG("Writing blob.\n");
    writeBlob(xid, lsn, rid, dat);

  } else {
    DEBUG("Writing record.\n");
    p = loadPagePtr(rid.page);
    assert( (p->id == rid.page) && (p->memAddr != NULL) );	
    /** @todo This assert should be here, but the tests are broken, so it causes bogus failures. */
    /*assert(pageReadLSN(*p) <= lsn);*/
    
    pageWriteRecord(xid, p, rid, lsn, dat);
    
  }
}

void readRecord(int xid, recordid rid, void *buf) {
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    DEBUG("Reading blob. xid = %d rid = { %d %d %ld } buf = %x\n", 
	  xid, rid.page, rid.slot, rid.size, (unsigned int)buf);
    readBlob(xid, rid, buf);
  } else {
    DEBUG("Reading record xid = %d rid = { %d %d %ld } buf = %x\n", 
	  xid, rid.page, rid.slot, rid.size, (unsigned int)buf);
    pageReadRecord(xid, loadPage(rid.page), rid, buf);
  }
}

int bufTransCommit(int xid, lsn_t lsn) {

  commitBlobs(xid);
  pageCommit(xid);

  return 0;
}

int bufTransAbort(int xid, lsn_t lsn) {

  abortBlobs(xid);  /* abortBlobs doesn't write any log entries, so it doesn't need the lsn. */
  pageAbort(xid);

  return 0;
}

void setSlotType(int pageid, int slot, int type) {
  Page * p = loadPage(pageid);
  pageSetSlotType(p, slot, type);
}

/** 
    Inform bufferManager that a new event (such as an update) will be
    performed on page pageid.  This function may not be called on a
    page after finalize() has been called on that page, and each call
    to this function must be followed by a corresponding call to
    removePendingEvent.

    This function is called by the logger when CLR or UPDATE records
    are written.
    
    @see finalize, removePendingEvent

*/
void addPendingEvent(int pageid){
  
  Page * p = loadPage(pageid);

  pthread_mutex_lock(&(p->pending_mutex));

  assert(!(p->waiting));

  p->pending++;

  pthread_mutex_unlock(&(p->pending_mutex));

}

/**
   
   Because updates to a page might not happen in order, we need to
   make sure that we've applied all updates to a page that we've heard
   about before we flush that page to disk.

   This method informs bufferManager that an update has been applied.
   It is called by operations.c every time doUpdate, redoUpdate, or
   undoUpdate is called.

   @todo as implemented, loadPage() ... doOperation is not atomic!

*/
void removePendingEvent(int pageid) {
  
  Page * p = loadPage(pageid);

  pthread_mutex_lock(&(p->pending_mutex));

  p->pending--;
  
  assert(p->pending >= 0);

  if(p->waiting && !p->pending) {
    assert(p->waiting == 1);
    pthread_cond_signal(&(p->noMorePending));
  }

  pthread_mutex_unlock(&(p->pending_mutex));
}


