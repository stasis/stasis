/*---
This software is copyrighted by the Regents of the University of
California and Ashok Sudarsanam. The following terms apply to all files
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


#include "fitest.h"
#include <limits.h>

#define BUFFER_INITIAL_LENGTH 256

recordid hashTable;
int insertLog, commitLog;
pthread_mutex_t hashTableMutex;

int numMallocs    = 0;
int numLogWrites  = 0;
int numPageWrites = 0;

int initHashTable;

static void* threadFunc(void* arg_ptr);
static void  writeKeyToBuffer(char** insertBuffer, int* bufferCurrentLength, 
			      int* bufferTotalSize, int key);
static void  writeBufferToLog(int log, char* insertBuffer, int bufferCurrentLength);

int
runSubject() {
  pthread_t* threads;
  pthread_attr_t attr;
  intptr_t k;
  int xid, fd;

  /* Need the following sleep call for debugging. */
  //sleep(45);

  printf("\tRunning Subject Process\n"); fflush(stdout);

  readGlobalsFromSharedBuffer();

  initHashTable = 1;

  Tinit();
  
  if (iteration == 0) {
    /* Create the transactional hash table data structure. */
    xid = Tbegin();
    hashTable = ThashCreate(xid, sizeof(int), sizeof(int));
    Tcommit(xid);

    /* Write the hash table recordid to a file, so it can be 
     * reloaded during subsequent iterations.
     */
    fd = open("recordid.txt", O_CREAT | O_WRONLY | O_SYNC, 0777);
    write(fd, (char*) &hashTable, sizeof(recordid));
    close(fd);
  }
  else {
    /* Otherwise, read in the hash table from disk. */
    fd = open("recordid.txt", O_RDONLY, 0777);
    read(fd, (char*) &hashTable, sizeof(recordid));
    close(fd);
  }

  initHashTable = 0;
  
  /* Open the insert and commit log files. The insert-log keeps track of all insertions
   * that were made to the hash-table, not all of which may have been committed.  The
   * commit-log keeps track of all insertions that were definitely committed.
   */
  insertLog = open("inserts.txt", O_CREAT | O_RDWR | O_APPEND, 0777);
  commitLog = open("commits.txt", O_CREAT | O_RDWR | O_APPEND, 0777);

  /* Allocate the worker threads. */
  threads = stasis_malloc(numThreads, pthread_t);
  
  pthread_attr_init(&attr);
  pthread_attr_setstacksize (&attr, PTHREAD_STACK_MIN);

  pthread_mutex_init(&hashTableMutex, NULL);

  for (k = 0; k < numThreads; k++) {
    pthread_create(&threads[k], &attr, threadFunc, (void*) k);
  }
  
  for (k = 0; k < numThreads; k++) {
    pthread_join(threads[k], NULL);
  }

  /* Close the insert and commit log files. */
  close(insertLog);
  close(commitLog);
  
  Tdeinit();

  printf("\t\tSubject Process Exiting Normally\n"); fflush(stdout);

  return 0;
}

static void* 
threadFunc(void* arg_ptr) {
  int j, k, startKey, endKey;
  int xid, count = 0;
  int bufferCurrentLength, bufferTotalSize;
  char* insertBuffer;

  /* Allocate the buffer that stores all outstanding hash table insertions. */
  bufferTotalSize = BUFFER_INITIAL_LENGTH;
  bufferCurrentLength = 0;
  insertBuffer = stasis_malloc(bufferTotalSize, char);

  xid = Tbegin();
  
  k = (intptr_t) arg_ptr;

  startKey = baseKey + (k * opsPerThread);
  endKey   = startKey + opsPerThread;

  for (j = startKey; j < endKey; j++) {
    ThashInsert(xid, hashTable, (byte*)&j, sizeof(int), (byte*)&j, sizeof(int));
    writeKeyToBuffer(&insertBuffer, &bufferCurrentLength, &bufferTotalSize, j);
    count++;
    
    if ((count % opsPerTransaction) == 0) {
      /* Prior to committing the transaction, write the hash table insertion buffer to 
       * the insert-log so that we can keep track of which insertions were possibly 
       * committed.  After the Tcommit() call, write the insertion buffer to the commit-
       * log so that we can keep track of which insertions were definitely committed.
       */
      writeBufferToLog(insertLog, insertBuffer, bufferCurrentLength);
      Tcommit(xid);
      writeBufferToLog(commitLog, insertBuffer, bufferCurrentLength);
      bufferCurrentLength = 0;

      xid = Tbegin();
      count = 0;
    }
  }

  /* Prior to committing the transaction, write the hash table insertion buffer to 
   * the insert-log so that we can keep track of which insertions were possibly 
   * committed.  After the Tcommit() call, write the insertion buffer to the commit-
   * log so that we can keep track of which insertions were definitely committed.
   */
  writeBufferToLog(insertLog, insertBuffer, bufferCurrentLength);
  Tcommit(xid);
  writeBufferToLog(commitLog, insertBuffer, bufferCurrentLength);
  
  return NULL;
}

static void  
writeKeyToBuffer(char** insertBuffer, int* bufferCurrentLength, 
		 int* bufferTotalSize, int key) {
  int curLength, totalSize, bytesWritten;

  curLength = *bufferCurrentLength;
  totalSize = *bufferTotalSize;

  /* Assume we need a maximum of 15 characters for the key, including the terminating
   * newline and null character.  Realloc if there's not enough space in the buffer.
   */
  if ((curLength + 15) > totalSize) {
    totalSize *= 2;
    *insertBuffer = (char*) realloc(*insertBuffer, totalSize);
    *bufferTotalSize = totalSize;
  }

  bytesWritten = sprintf(*insertBuffer + curLength, "%d\n", key);
  curLength += bytesWritten;
  assert(curLength < totalSize);

  *bufferCurrentLength = curLength;
}

static void  
writeBufferToLog(int log, char* insertBuffer, int bufferCurrentLength) {
  pthread_mutex_lock(&hashTableMutex);
  write(log, insertBuffer, bufferCurrentLength);
  //fsync(log);
  pthread_mutex_unlock(&hashTableMutex);
}


/* The following prototypes are needed to suppress GCC warnings. */
void*   __real_malloc(int c);
size_t  __real_fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream);
ssize_t __real_pwrite(int fildes, const void *buf, size_t nbyte, __off64_t offset);
ssize_t __real_pwrite64(int fildes, const void *buf, size_t nbyte, __off64_t offset);

void*
__wrap_malloc(size_t size) {
  /* fprintf(stderr, "inside wrapper function for malloc\n"); */

  if (!mallocsBeforeFailure || currentMode != SUBJECT_MODE || initHashTable) {
    return __real_malloc(size);
  }

  pthread_mutex_lock(&hashTableMutex);
  numMallocs++;
    
  if (numMallocs >= mallocsBeforeFailure) {
    printf("\t\tMALLOC FAILURE:  Subject Process Exiting Prematurely\n"); fflush(stdout);
    
    /* Exit the process. */
    exit(1);
  }
    
  pthread_mutex_unlock(&hashTableMutex);

  return __real_malloc(size);
}

size_t 
__wrap_fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream) {
  int updatedSize;

  /* fprintf(stderr, "inside wrapper function for fwrite\n"); */

  if (!logWritesBeforeFailure || currentMode != SUBJECT_MODE || initHashTable) {
    return __real_fwrite(ptr, size, nitems, stream);
  }

  pthread_mutex_lock(&hashTableMutex);
  numLogWrites++;
  
  if (numLogWrites >= logWritesBeforeFailure) {
    printf("\t\tFWRITE FAILURE:  Subject Process Exiting Prematurely\n"); fflush(stdout);
    
    /* Possibly write a percentage of the buffer, to simulate the writing of a torn log. */
    if (logWriteAmountDuringFailure > 0) {
      updatedSize = (int) ((nitems * logWriteAmountDuringFailure) / 100.0);
      __real_fwrite(ptr, size, updatedSize, stream);
    }

    /* Exit the process. */
    exit(1);
  }
  
  pthread_mutex_unlock(&hashTableMutex);

  return __real_fwrite(ptr, size, nitems, stream);
}

ssize_t 
__wrap_pwrite(int fildes, const void *buf, size_t nbyte, __off64_t offset) {
  int updatedSize;

  /* fprintf(stderr, "inside wrapper function for pwrite64\n"); */

  if (!pageWritesBeforeFailure || currentMode != SUBJECT_MODE || initHashTable) {
    return __real_pwrite(fildes, buf, nbyte, offset);
  }

  pthread_mutex_lock(&hashTableMutex);
  numPageWrites++;
      
  if (numPageWrites >= pageWritesBeforeFailure) {
    printf("\t\tPWRITE FAILURE:  Subject Process Exiting Prematurely\n"); fflush(stdout);
    
    /* Possibly write a percentage of the buffer, to simulate the writing of a torn page. */
    if (pageWriteAmountDuringFailure > 0) {
      updatedSize = (int) ((nbyte * pageWriteAmountDuringFailure) / 100.0);
      __real_pwrite64(fildes, buf, updatedSize, offset);
    }
    
    /* Exit the process. */
    exit(1);
  }
      
  pthread_mutex_unlock(&hashTableMutex);

  return __real_pwrite(fildes, buf, nbyte, offset);
}

ssize_t 
__wrap_pwrite64(int fildes, const void *buf, size_t nbyte, __off64_t offset) {
  int updatedSize;

  /* fprintf(stderr, "inside wrapper function for pwrite64\n"); */

  if (!pageWritesBeforeFailure || currentMode != SUBJECT_MODE || initHashTable) {
    return __real_pwrite64(fildes, buf, nbyte, offset);
  }

  pthread_mutex_lock(&hashTableMutex);
  numPageWrites++;
  
  if (numPageWrites >= pageWritesBeforeFailure) {
    printf("\t\tPWRITE64 FAILURE:  Subject Process Exiting Prematurely\n"); fflush(stdout);
    
    /* Possibly write a percentage of the buffer, to simulate the writing of a torn page. */
    if (pageWriteAmountDuringFailure > 0) {
      updatedSize = (int) ((nbyte * pageWriteAmountDuringFailure) / 100.0);
      __real_pwrite64(fildes, buf, updatedSize, offset);
    }
    
    /* Exit the process. */
    exit(1);
  }
  
  pthread_mutex_unlock(&hashTableMutex);

  return __real_pwrite64(fildes, buf, nbyte, offset);
}

