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
#include "../../src/apps/cht/cht.h"
#include <assert.h>

#include <string.h>

/** Thanks, jbhtsimple.c!! */

char** broadcast_lists[2];
char* star_nodes[] = { "140.254.26.214:10010" };
char* point_nodes[] = { "127.0.0.1:10011", 
			"127.0.0.1:10012",
			"127.0.0.1:10013",
			"127.0.0.1:10014",
			"127.0.0.1:10015" };

int broadcast_list_host_count [] = { 1, 5 };

int broadcast_lists_count = 1;


typedef struct {
  int key;
  char *value;
} test_pair_t;

int main (int argc, char**argv) {
  
  
  test_pair_t one;  
  test_pair_t two;   
  test_pair_t three;
  test_pair_t four; 
  
  test_pair_t one_s   = {1 , "one"};
  test_pair_t two_s   = {2 , "two"};
  test_pair_t three_s = {3 , "three"};
  test_pair_t four_s  = {4 , "four"};

  int i;
  state_machine_id xid;

  DfaSet * dfaSet; 
  clusterHashTable_t ht;


  memcpy(&one, &one_s, sizeof(test_pair_t));
  memcpy(&two, &two_s, sizeof(test_pair_t));
  memcpy(&three, &three_s, sizeof(test_pair_t));
  memcpy(&four, &four_s, sizeof(test_pair_t));

  one.value = strdup(one_s.value);
  two.value = strdup(two_s.value);
  three.value = strdup(three_s.value);
  four.value = strdup(four_s.value);

  assert(argc == 3);

  Tinit();
  
  broadcast_lists[0] = star_nodes;
  broadcast_lists[1] = point_nodes;

  dfaSet = cHtInit(CHT_CLIENT, argv[2], NULL, atoi(argv[1]), broadcast_lists, broadcast_lists_count, broadcast_list_host_count);

  spawn_main_thread(dfaSet);
  
  xid = NULL_MACHINE;
  /* assert ( cHtGetXid(&xid, dfaSet)); */

  printf("Got xid: %ld\n", xid);

  printf("cHtCreate\n");
  fflush(NULL);
  

  assert( cHtCreate(xid, dfaSet, &ht)                                            );


  printf("Got hashtable: %d\n", ht.id);

  printf("cHtInserts\n");
  fflush(NULL);

  if( (!cHtInsert(xid, dfaSet, &ht, &one.key, sizeof(int), one.value, sizeof(char)*4)) ||
      (!cHtInsert(xid, dfaSet, &ht, &two.key, sizeof(int), two.value, sizeof(char)*4)) ||
      (!cHtInsert(xid, dfaSet, &ht, &three.key, sizeof(int), three.value, sizeof(char)*6)) ||
       (!cHtInsert(xid, dfaSet, &ht, &four.key, sizeof(int), four.value, sizeof(char)*5))
      ) {
    printf("Insert failed! \n");
    return -1;
  }
  for(i = 0; i < 99; i++) {
    one.key+=4;
    two.key+=4;
    three.key+=4;
    four.key+=4;
    if( (!cHtInsert(xid, dfaSet, &ht, &one.key, sizeof(int), one.value, sizeof(char)*4)) ||
	(!cHtInsert(xid, dfaSet, &ht, &two.key, sizeof(int), two.value, sizeof(char)*4)) ||
	(!cHtInsert(xid, dfaSet, &ht, &three.key, sizeof(int), three.value, sizeof(char)*6)) ||
	(!cHtInsert(xid, dfaSet, &ht, &four.key, sizeof(int), four.value, sizeof(char)*5))
	) {
      printf("Insert failed! \n");
      return -1;
    }
  }

  printf("Last key: %d\n", four.key);

  /*  assert ( cHtGetXid(&xid, dfaSet));  */

  for( i = 1; i <= 400; i++ ) {
    char buf[7];
    size_t buflen = 7;
    /*    int j = (i % 8) + 1;*/
    int j = i;
    if(!cHtLookup(xid, dfaSet, &ht, &j, sizeof(int), buf, &buflen)) { printf ("lookup failed!"); }
    printf("                                                        looked up !! key %d -> %d: %s %d\n", i, j, buf, buflen);
  }
  
  cHtDelete(xid, dfaSet, &ht);
  
  return 0;

}



