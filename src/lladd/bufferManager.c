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
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <pbl/pbl.h>
#include <lladd/bufferManager.h>
#include <lladd/page.h>
#include <lladd/constants.h>

#include "blobManager.h"

static pblHashTable_t *activePages; /* page lookup */
static unsigned int bufferSize = 1; /* < MAX_BUFFER_SIZE */
static Page *repHead, *repMiddle, *repTail; /* replacement policy */

static int stable = -1;
int blobfd0 = -1;
int blobfd1 = -1;

static void pageMap(Page *ret) {

	int fileSize;
	/* this was lseek(stable, SEEK_SET, pageid*PAGE_SIZE), but changed to
	            lseek(stable, pageid*PAGE_SIZE, SEEK_SET) by jkit (Wed Mar 24 12:59:18 PST 2004)*/
	fileSize = lseek(stable, 0, SEEK_END);

	if ((ret->id)*PAGE_SIZE >= fileSize) {
		lseek(stable, (1 + ret->id)*PAGE_SIZE -1 , SEEK_SET);
		write(stable, "", 1);
	}

	if((ret->memAddr = mmap((void *) 0, PAGE_SIZE, (PROT_READ | PROT_WRITE), MAP_SHARED, stable, (ret->id)*PAGE_SIZE)) == (void*)-1) {
		printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
		exit(errno);
	}
}

int bufInit() {

	Page *first;

	bufferSize = 1;
	stable = -1;
	blobfd0 = -1;
	blobfd1 = -1;


	/* Create STORE_FILE, BLOB0_FILE, BLOB1_FILE if necessary, 
	   then open it read/write 

	   If we're creating it, then put one all-zero record at the beginning of it.  
	   (Need to have at least one record in the PAGE file?)

	   It used to be that there was one file per page, and LSN needed to be set to -1.  

	   Now, zero means uninitialized, so this could probably be replaced 
	   with a call to open(... O_CREAT|O_RW) or something like that...
	*/
	if( (stable = open(STORE_FILE, O_RDWR, 0)) == -1 ) { /* file may not exist */
		void *zero = mmap(0, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0); /* zero = /dev/zero */
		if( (stable = creat(STORE_FILE, 0666)) == -1 ) { /* cannot even create it */
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
		/* kick off a fresh page */
		if( write(stable, zero, PAGE_SIZE) != PAGE_SIZE ) { /* write zeros out */
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
		if( close(stable) || ((stable = open(STORE_FILE, O_RDWR, 0)) == -1) ) { /* need to reopen with read perms */
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
	}

	activePages = pblHtCreate();
	assert(activePages);

	first = pageAlloc(0);
	pblHtInsert(activePages, &first->id, sizeof(int), first);

	first->prev = first->next = NULL;
	pageMap(first);

	repHead = repTail = first;
	repMiddle = NULL;

	openBlobStore();

	/*	if( (blobfd0 = open(BLOB0_FILE, O_RDWR, 0)) == -1 ) { / * file may not exist * /
		if( (blobfd0 = creat(BLOB0_FILE, 0666)) == -1 ) { / * cannot even create it * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
		if( close(blobfd0) || ((blobfd0 = open(BLOB0_FILE, O_RDWR, 0)) == -1) ) { / * need to reopen with read perms * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
	}
	if( (blobfd1 = open(BLOB1_FILE, O_RDWR, 0)) == -1 ) { / * file may not exist * /
		if( (blobfd1 = creat(BLOB1_FILE, 0666)) == -1 ) { / * cannot even create it * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
		if( close(blobfd1) || ((blobfd1 = open(BLOB1_FILE, O_RDWR, 0)) == -1) ) { / * need to reopen with read perms * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
	} */

	return 0;
}

static void headInsert(Page *ret) {

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);

	repHead->prev = ret;
	ret->next = repHead;
	ret->prev = NULL;
	repHead = ret;
}

static void middleInsert(Page *ret) {

	assert( bufferSize == MAX_BUFFER_SIZE );

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);

	ret->prev  = repMiddle->prev;
	ret->next = repMiddle;
	repMiddle->prev = ret;
	ret->prev->next = ret;
	ret->queue = 2;

	repMiddle = ret;
	assert(ret->next != ret && ret->prev != ret);
}

static void qRemove(Page *ret) {

	assert( bufferSize == MAX_BUFFER_SIZE );
	assert(ret->next != ret && ret->prev != ret);

	if( ret->prev )
		ret->prev->next = ret->next;
	else /* is head */
		repHead = ret->next; /* won't have head == tail because of test in loadPage */
	if( ret->next ) {
		ret->next->prev = ret->prev;
		/* TODO: these if can be better organizeed for speed */
		if( ret == repMiddle ) 
			/* select new middle */
			repMiddle = ret->next;
	}
	else /* is tail */
		repTail = ret->prev;

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);
}

static Page *kickPage(int pageid) {
	/* LRU-2S from Markatos "On Caching Searching Engine Results" */
	Page *ret = repTail;

	assert( bufferSize == MAX_BUFFER_SIZE );

	qRemove(ret);
	pblHtRemove(activePages, &ret->id, sizeof(int));
	if( munmap(ret->memAddr, PAGE_SIZE) )
		assert( 0 ); 

	pageRealloc(ret, pageid);

	middleInsert(ret);
	pblHtInsert(activePages, &pageid, sizeof(int), ret);

	return ret;
}


int lastPageId = -1;
Page * lastPage = 0;

static Page *loadPagePtr(int pageid) {
	/* lock activePages, bufferSize */
	Page *ret;

	if(lastPage && lastPageId == pageid) {
	  return lastPage;
	} else {
	  ret = pblHtLookup(activePages, &pageid, sizeof(int));
	}

	if( ret ) {
		if( bufferSize == MAX_BUFFER_SIZE ) { /* we need to worry about page sorting */
			/* move to head */
			if( ret != repHead ) {
				qRemove(ret);
				headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);

				if( ret->queue == 2 ) {
					/* keep first queue same size */
					repMiddle = repMiddle->prev;
					repMiddle->queue = 2;

					ret->queue = 1;
				}
			}
		}

		lastPage = ret;
		lastPageId = pageid;

		return ret;
	} else if( bufferSize == MAX_BUFFER_SIZE ) { /* we need to kick */
		ret = kickPage(pageid);
	} else if( bufferSize == MAX_BUFFER_SIZE-1 ) { /* we need to setup kickPage mechanism */
		int i;
		Page *iter;

		ret = pageAlloc(pageid);
		headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);

		pblHtInsert( activePages, &pageid, sizeof(int), ret );

		bufferSize++;

		/* split up queue:
		 * "in all cases studied ... fixing the primary region to 30% ...
		 * resulted in the best performance"
		 */
		repMiddle = repHead;
		for( i = 0; i < MAX_BUFFER_SIZE / 3; i++ ) {
			repMiddle->queue = 1;
			repMiddle = repMiddle->next;
		}

		for( iter = repMiddle; iter; iter = iter->next ) {
			iter->queue = 2;
		}

	} else { /* we are adding to an nonfull queue */

		bufferSize++;

		ret = pageAlloc(pageid);
		headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);
		assert(ret->next != ret && ret->prev != ret);
		pblHtInsert( activePages, &pageid, sizeof(int), ret );
	}

	/* we now have a page we can dump info into */
	assert( ret->id == pageid );

	pageMap(ret);

	lastPage = ret;
	lastPageId = pageid;

	return ret;
}

Page loadPage (int pageid) {
	return *loadPagePtr(pageid);
}

/*int lastGoodPageKey = 0; */

Page * lastRallocPage = 0;

recordid ralloc(int xid, size_t size) {
  static unsigned int lastFreepage = 0;
  Page p;
  int blobSize = 0;

  if (size >= BLOB_THRESHOLD_SIZE) { /* TODO combine this with if below */
    blobSize = size;
    size = BLOB_REC_SIZE;
  }

  while(freespace(p = loadPage(lastFreepage)) < size ) { lastFreepage++; }

  if (blobSize >= BLOB_THRESHOLD_SIZE) {
    int fileSize = (int) lseek(blobfd1, 0 , SEEK_END);
    /*    fstat(blobfd1, &sb);
	  fileSize = (int) sb.st_size;	 */
    lseek(blobfd0, fileSize+blobSize-1, SEEK_SET);
    write(blobfd0, "", 1);
    lseek(blobfd1, fileSize+blobSize-1, SEEK_SET);
    write(blobfd1, "", 1);

    return pageBalloc(p, blobSize, fileSize);
  } else {
    return pageRalloc(p, size);
  }

}
long readLSN(int pageid) {

	return pageReadLSN(loadPage(pageid));
}

void writeLSN(long LSN, int pageid) {
	Page *p = loadPagePtr(pageid);
	p->LSN = LSN;
	pageWriteLSN(*p);
}
void writeRecord(int xid, recordid rid, const void *dat) {

	Page *p = loadPagePtr(rid.page);
	assert( (p->id == rid.page) && (p->memAddr != NULL) );	

        pageWriteRecord(xid, *p, rid, dat);  /* Used to attempt to return this. */
}
void readRecord(int xid, recordid rid, void *buf) {
	pageReadRecord(xid, loadPage(rid.page), rid, buf); /* Used to attempt to return this. */
}

int flushPage(Page page) {

	if( munmap(page.memAddr, PAGE_SIZE) )
		return MEM_WRITE_ERROR;

	return 0;
}

int bufTransCommit(int xid) {

  fdatasync(blobfd0);
  fdatasync(blobfd1);

	pageCommit(xid);

	return 0;
}

int bufTransAbort(int xid) {

	pageAbort(xid);

	return 0;
}

void bufDeinit() {
	int ret;
	Page *p;

	for( p = (Page*)pblHtFirst( activePages ); p; p = (Page*)pblHtRemove( activePages, 0, 0 )) {
		if( p->dirty && (ret = flushPage(*p))) {
			printf("ERROR: flushPage on %s line %d", __FILE__, __LINE__);
			exit(ret);
		}
		/*		free(p); */
	}
	pblHtDelete(activePages);

	if( close(stable) ) {
		printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
		exit(errno);
	}

	/*	close(blobfd0);
		close(blobfd1); */
	closeBlobStore();

	return;
}
/**
    Just close file descriptors, don't do any other clean up. (For
    testing.)
*/
void simulateBufferManagerCrash() {
  closeBlobStore();
  /*close(blobfd0);
    close(blobfd1);*/
  close(stable);
  /*  blobfd0 = -1;  
  blobfd1 = -1;  */
  stable = -1;


}
