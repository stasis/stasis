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
#include "../check_includes.h"

#include <stasis/ringbuffer.h>

#include <assert.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_ringbuffer.log"

/**
   @test

*/

#define NUM_ENTRIES 10000


static int rb_test(double readBlock, double writeBlock) {

  int i;
  byte ** array = malloc(sizeof(byte*) * NUM_ENTRIES);
  int * length = malloc(sizeof(int) * NUM_ENTRIES);
  for(i = 0; i < NUM_ENTRIES; i++) {
    length[i] =1.0+ 1000.0 * (double)rand() / (double)RAND_MAX;
    array[i] = malloc(length[i]);
    int j;
    for(j = 0; j < length[i]; j++) {
      array[i][j] = i + j;
    }
  }

  ringBufferLog_t * log = openLogRingBuffer(NUM_ENTRIES, 5);

  int doneReading = 0;
  int doneWriting = 0;
  int readPos = 0;
  int writePos = 0;
  while(!doneReading) {
    if(!doneWriting) {
      int numToWrite = 1.0 + writeBlock * (rand() / RAND_MAX);
      //      printf("%d\n", numToWrite);
      for(i = 0; i < numToWrite; i++) {
	if(!ringBufferAppend(log, array[writePos], length[writePos])) {
	  //	  printf("W"); fflush(stdout);
          if(writePos == 0) {
            assert((-2 == ringBufferAppend(log, array[writePos], 1+NUM_ENTRIES - length[writePos])));
          }
	  writePos++;
	  if(writePos ==  NUM_ENTRIES) { break; }
	} else {
	  break;
	}
      }
    }
    // try to truncate a chunk longer than the whole ringbuffer.
    // should be a no-op

    // note: passing 0 for buffer argument is illegal
    assert(-1 == ringBufferTruncateRead(0, log, NUM_ENTRIES * 10));

    // try to append more than the ring buffer can hold
    // should also be a no-op.
    assert(-1 == ringBufferAppend(log, 0, NUM_ENTRIES * 10));


    int numToRead = 1.0 + readBlock * ((double)rand() / (double)RAND_MAX);
    //    printf("%d\n", numToRead);
    for(i = 0; i < numToRead; i++) {
      byte * buf = malloc(length[readPos]);
      if(!ringBufferTruncateRead(buf, log, length[readPos])) {
	int j;
	for(j = 0; j < length[readPos]; j++) {
	  assert(buf[j] == array[readPos][j]);
	}
	free(buf);
	//	printf("R"); fflush(stdout);

	readPos++;
	if(readPos == NUM_ENTRIES) { break; }
      } else {
	break;
      }
    }
    if(readPos == NUM_ENTRIES) {
      doneReading = 1;
    }
    if(writePos == NUM_ENTRIES) {
      doneWriting = 1;
    }
  }
  closeLogRingBuffer(log);
  return 0;
}

START_TEST(ringBufferTest)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);

  srand(tv.tv_sec + tv.tv_usec);

  printf("\nRunning balanced test.\n");
  rb_test(5.0,5.0);
  printf("Running read-intensive test.\n");
  rb_test(10.0, 1.0);
  printf("Running write-intensive test.\n");
  rb_test(1.0, 10);
} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("ringBuffer");
  /* Begin a new test */
  TCase *tc = tcase_create("ringBuffer");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, ringBufferTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
