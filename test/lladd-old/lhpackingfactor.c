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
#include <stdio.h>
#include <assert.h>
#include "test.h"
#include <lladd/transactional.h>

typedef struct {
	int key;
	char *value;
} test_pair_t;

/**
 * Test of logical logging persistant hash with various packing
 * factors.  It is designed to be run by a script to determine the
 * optimum packing factor for the hash. Due to unnecessary overhead
 * with large sets of buckets, this number is currently quite high (~
 * 10 entries per hash bucket, where 9 of those entries are overflow
 * entries.)
 */

int test(int argc, char **argv) {
  int j;
  for(j = 0; j < 1000; j++) {

	unsigned int i, xid;
	unsigned int INSERT_NUM;
	char value[22]; /* should be log(INSERT_NUM)+8 */
	lladdHash_t *ht; 

	assert(argc==2);

	INSERT_NUM = 255; /* should be > JB_HASHTABLE_SIZE */
	int size = atoi(argv[1]);
	xid = Tbegin();
	ht = lHtCreate(xid, size);

	for( i = 0; i < INSERT_NUM; i++ ) {
	  sprintf(value, "value: %u\n", i); 
	  /*		printf(".."); fflush(NULL); */
	  lHtInsert(xid, ht, &i, sizeof(int), value, sizeof(char)*strlen(value));
	}

	Tcommit(xid);
	xid = Tbegin();
	for( i = 0; i < INSERT_NUM; i++ ) {
	  int ret = lHtLookup(xid, ht, &i, sizeof(int), value);
	  if(ret == -1) {
	    return 1;
	  }
	  /*	  printf("%d %d -> %s", ret, i, value); */
	}

	lHtDelete(xid, ht);
	
	Tabort(xid);
  }
	return 0;
}
