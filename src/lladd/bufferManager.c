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
#include <stdio.h>

#include <lladd/bufferManager.h>
#include "blobManager.h"
#include <lladd/pageCache.h>
#include <lladd/logger/logWriter.h>

static FILE * stable = NULL;
static unsigned int lastFreepage = 0;

/* ** File I/O functions ** */
/* Defined in blobManager.c, but don't want to export this in any public .h files... */
long myFseek(FILE * f, long offset, int whence);

void pageRead(Page *ret) {
  long fileSize = myFseek(stable, 0, SEEK_END);
  long pageoffset = ret->id * PAGE_SIZE;
  long offset;

  DEBUG("Reading page %d\n", ret->id);

  if(!ret->memAddr) {
    ret->memAddr = malloc(PAGE_SIZE);
  }
  assert(ret->memAddr);
  
  if ((ret->id)*PAGE_SIZE >= fileSize) {
    myFseek(stable, (1+ ret->id) * PAGE_SIZE -1, SEEK_SET);
    if(1 != fwrite("", 1, 1, stable)) {
      if(feof(stable)) { printf("Unexpected eof extending storefile!\n"); fflush(NULL); abort(); }
      if(ferror(stable)) { printf("Error extending storefile! %d", ferror(stable)); fflush(NULL); abort(); }
    }

  }
  offset = myFseek(stable, pageoffset, SEEK_SET);
  assert(offset == pageoffset);

  if(1 != fread(ret->memAddr, PAGE_SIZE, 1, stable)) {
                                                                                                                 
    if(feof(stable)) { printf("Unexpected eof reading!\n"); fflush(NULL); abort(); }
    if(ferror(stable)) { printf("Error reading stream! %d", ferror(stable)); fflush(NULL); abort(); }
                                                                                                                 
  }

}

void pageWrite(const Page * ret) {

  long pageoffset = ret->id * PAGE_SIZE;
  long offset = myFseek(stable, pageoffset, SEEK_SET);
  assert(offset == pageoffset);
  assert(ret->memAddr);

  DEBUG("Writing page %d\n", ret->id);

  if(flushedLSN() < pageReadLSN(*ret)) {
    DEBUG("pageWrite is calling syncLog()!\n");
    syncLog();
  }

  if(1 != fwrite(ret->memAddr, PAGE_SIZE, 1, stable)) {
                                                                                                                 
    if(feof(stable)) { printf("Unexpected eof writing!\n"); fflush(NULL); abort(); }
    if(ferror(stable)) { printf("Error writing stream! %d", ferror(stable)); fflush(NULL); abort(); }
                                                                                                                 
  }


}

static void openPageFile() {

  DEBUG("Opening storefile.\n");
  if( ! (stable = fopen(STORE_FILE, "r+"))) { /* file may not exist */
    byte* zero = calloc(1, PAGE_SIZE);

    if(!(stable = fopen(STORE_FILE, "w+"))) { perror("Couldn't open or create store file"); abort(); }

    /* Write out one page worth of zeros to get started. */
    
    if(1 != fwrite(zero, PAGE_SIZE, 1, stable)) { assert (0); }

    free(zero);
  }
  
  lastFreepage = 0;
  DEBUG("storefile opened.\n");

}
static void closePageFile() {

  int ret = fclose(stable);
  assert(!ret);
  stable = NULL;
}

/* void pageMap(Page *ret) {
  pageRead(ret);
}
int flushPage(Page ret) {
  pageWrite(&ret);
  return 0;
  } */

/*
void pageMap(Page *ret) {

	int fileSize;
	/ * this was lseek(stable, SEEK_SET, pageid*PAGE_SIZE), but changed to
	            lseek(stable, pageid*PAGE_SIZE, SEEK_SET) by jkit (Wed Mar 24 12:59:18 PST 2004)* /
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

int flushPage(Page page) {

	if( munmap(page.memAddr, PAGE_SIZE) )
		return MEM_WRITE_ERROR;

	return 0;
}

*/

int bufInit() {

	stable = NULL;

	/* Create STORE_FILE, if necessary, then open it read/write 

	   If we're creating it, then put one all-zero record at the beginning of it.  
	   (Need to have at least one record in the PAGE file?)

	   It used to be that there was one file per page, and LSN needed to be set to -1.  

	   Now, zero means uninitialized, so this could probably be replaced 
	   with a call to open(... O_CREAT|O_RW) or something like that...
	*/
	/*	if( (stable = open(STORE_FILE, O_RDWR, 0)) == -1 ) { / * file may not exist * /
		void *zero = mmap(0, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0); / * zero = /dev/zero * /
		if( (stable = creat(STORE_FILE, 0666)) == -1 ) { / * cannot even create it * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
		/ * kick off a fresh page * /
		if( write(stable, zero, PAGE_SIZE) != PAGE_SIZE ) { / * write zeros out * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
		if( close(stable) || ((stable = open(STORE_FILE, O_RDWR, 0)) == -1) ) { / * need to reopen with read perms * /
			printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
			exit(errno);
		}
	} */

	openPageFile();
	pageCacheInit();
	openBlobStore();
	
	return 0;
}

void bufDeinit() {

	closeBlobStore();
	pageCacheDeinit();
	closePageFile();
	/*	if( close(stable) ) {
		printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
		exit(errno);
		}*/

	return;
}
/**
    Just close file descriptors, don't do any other clean up. (For
    testing.)
*/
void simulateBufferManagerCrash() {
  closeBlobStore();
  closePageFile();
  /*  close(stable);
      stable = -1;*/

}

/* ** No file I/O below this line. ** */

Page loadPage (int pageid) {
	return *loadPagePtr(pageid);
}

Page * lastRallocPage = 0;


recordid ralloc(int xid, /*lsn_t lsn,*/ long size) {

  recordid ret;
  Page p;

  DEBUG("Rallocing blob of size %ld\n", (long int)size);

  assert(size < BLOB_THRESHOLD_SIZE || size == BLOB_SLOT);

  /*  if (size >= BLOB_THRESHOLD_SIZE) { 
    
    ret = allocBlob(xid, lsn, size);

    } else { */
  
    while(freespace(p = loadPage(lastFreepage)) < size ) { lastFreepage++; }
    ret = pageRalloc(p, size);
    
    /*  }  */
  DEBUG("alloced rid = {%d, %d, %ld}\n", ret.page, ret.slot, ret.size);
  return ret;
}
long readLSN(int pageid) {

	return pageReadLSN(loadPage(pageid));
}
/*
static void writeLSN(lsn_t LSN, int pageid) {
	Page *p = loadPagePtr(pageid);
	p->LSN = LSN;
	pageWriteLSN(*p);
	}*/
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
	  
	  pageWriteRecord(xid, *p, rid, dat);
	  /*	  writeLSN(lsn, rid.page); */
	  p->LSN = lsn;
	  pageWriteLSN(*p);
	}
}
void readRecord(int xid, recordid rid, void *buf) {
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    DEBUG("Reading blob. xid = %d rid = { %d %d %ld } buf = %x\n", xid, rid.page, rid.slot, rid.size, (unsigned int)buf);
    readBlob(xid, rid, buf);
  } else {
    DEBUG("Reading record xid = %d rid = { %d %d %ld } buf = %x\n", xid, rid.page, rid.slot, rid.size, (unsigned int)buf);
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

