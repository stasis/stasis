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
 * @file Implementation of in memory buffer pool
 *
 * $Id$
 * 
 */

/* _XOPEN_SOURCE is needed for posix_memalign */
#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include <lladd/bufferPool.h>
#include <assert.h>
#include <lladd/truncation.h>

/* TODO:  Combine with buffer size... */
static int nextPage = 0;
static pthread_mutex_t pageMallocMutex;

static void * addressFromMalloc = 0;

/** We need one dummy page for locking purposes, so this array has one extra page in it. */
Page pool[MAX_BUFFER_SIZE+1];


void bufferPoolInit() { 

  nextPage = 0;
	
  pthread_mutex_init(&pageMallocMutex, NULL);

  byte * bufferSpace ;

  /*#ifdef HAVE_POSIX_MEMALIGN
  int ret = posix_memalign((void*)&bufferSpace, PAGE_SIZE, PAGE_SIZE * (MAX_BUFFER_SIZE + 1));
  assert(!ret);
  addressFromMalloc = bufferSpace;
  #else*/
  bufferSpace = malloc(PAGE_SIZE * (MAX_BUFFER_SIZE + 2));
  assert(bufferSpace);
  addressFromMalloc = bufferSpace;
  bufferSpace = (byte*)(((long)bufferSpace) + 
			PAGE_SIZE - 
			(((long)bufferSpace) % PAGE_SIZE));
  //#endif

  for(int i = 0; i < MAX_BUFFER_SIZE+1; i++) {
    pool[i].rwlatch = initlock();
    pool[i].loadlatch = initlock();
    pool[i].memAddr = &(bufferSpace[i*PAGE_SIZE]);
  }
}

void bufferPoolDeInit() { 
  for(int i = 0; i < MAX_BUFFER_SIZE+1; i++) {
    deletelock(pool[i].rwlatch);
    deletelock(pool[i].loadlatch);
  }
  free(addressFromMalloc); // breaks efence
  pthread_mutex_destroy(&pageMallocMutex);
}

Page* pageMalloc() { 
  Page *page;

  pthread_mutex_lock(&pageMallocMutex);
  
  page = &(pool[nextPage]);
  
  nextPage++;
  /* There's a dummy page that we need to keep around, thus the +1 */
  assert(nextPage <= MAX_BUFFER_SIZE + 1); 

  pthread_mutex_unlock(&pageMallocMutex);

  return page;

}


static void pageFreeNoLock(Page *p, int id) {
  p->id = id;
  p->LSN = 0;
  p->dirty = 0;
}

void pageFree(Page *p, int id) {
  writelock(p->rwlatch, 10);
  pageFreeNoLock(p,id);
  writeunlock(p->rwlatch);
}
