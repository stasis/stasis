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
#include "../../src/libdfa/monotree.h"

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>

int main () {

  MonoTree * rb;

  srand (1);

  while (1) {

    int i = 0;
    int rb_size = 1 + (int) (5000.0*rand()/(RAND_MAX+1.0));
    int tx_count = 1 + (int)(30000.0*rand()/(RAND_MAX+1.0));
    unsigned char * bitmap = calloc(tx_count, sizeof(unsigned char));
    unsigned int new_seed = (int) ((1.0* INT_MAX*rand())/(RAND_MAX+1.0));
    state_machine_id remaining_xact = 0;
    state_machine_id last_xact = 0;
    rb = malloc (sizeof(MonoTree));
    rb->buffer = malloc(sizeof(StateMachine) * rb_size);

    init_MonoTree(rb, rb_size);
    
    printf("Created a mono tree of size %d.\n", rb_size);
    printf("Running %d random 'machines through it.\n", tx_count);
    
    for( ; remaining_xact != 0 || last_xact < tx_count; ) {
      int choice = 1+ (int) (7.0*rand()/(RAND_MAX+1.0));
      int victim;
      if(!(i % 50000)) {
	printf("%d %ld %ld\n", i, last_xact, remaining_xact);
      } i++;
      switch (choice) {
      case 1:
	/*      case 2: */
	/* Initiate new */
	if(last_xact < tx_count) {
	  StateMachine * sm = allocMachine(rb);
	  if(sm != 0) {
	    sm->current_state = 1;
	    assert(sm->machine_id == last_xact);
	    assert(bitmap[last_xact]==0);
	    bitmap[last_xact] = 1;
	    last_xact++;
	    remaining_xact++;
	  } 
	}
	break;
      case 3:
	/* Kill random */
	victim = (int)((1.0 * last_xact * rand())/RAND_MAX+1.0);
	while(!bitmap[victim] && victim < last_xact) {
	  victim++;
	}
	if(victim == last_xact) {
	  victim = 0;
	  while(!bitmap[victim] && victim < last_xact) {
	    victim++;
	  }
	}
	if(victim < last_xact) {
	  freeMachine(rb, victim);
	  assert(bitmap[victim] == 1);
	  assert(0 == getMachine(rb, victim));
	  bitmap[victim] = 0;
	  remaining_xact--;
	}
	break;
      case 4:
	/* Lookup random empty */
	victim = (int)((1.0 * last_xact * rand())/RAND_MAX+1.0);
	while(bitmap[victim] && victim < last_xact) {
	  victim++;
	}
	if(victim < last_xact) {
	  assert(0 == getMachine(rb, victim));
	}
	break;
      case 5:
      case 2:
	/* Insert random empty */
	if(last_xact < tx_count) {
	  victim = (int)((1.0 * last_xact * rand())/RAND_MAX+1.0);
	  while(bitmap[victim] && victim < last_xact) {
	    victim++;
	  }
	  if(victim < last_xact) {
	    if(rb->high_water_mark == rb->size) {
	      assert(0 == getMachine(rb, victim));
	      if(insertMachine(rb, victim)) {
		bitmap[victim] = 1;
		remaining_xact++;
	      }
	    } else {
	      assert(0 != insertMachine(rb, victim));
	      assert(victim == (getMachine(rb, victim)->machine_id));
	      bitmap[victim] = 1;
	      remaining_xact++;
	    }
	  } 
	}
	break;
      case 6:
	/* Insert random existing */
	victim = (int)((1.0 * last_xact * rand())/RAND_MAX+1.0);
	while(!bitmap[victim] && victim < last_xact) {
	  victim++;
	}
	if(victim < last_xact) {
	  assert(0 == insertMachine(rb, victim));
	} 
	break;
      default:
	/* Lookup random existing */
	victim = (int)((1.0 * last_xact * rand())/RAND_MAX+1.0);
	while(!bitmap[victim] && victim < last_xact) {
	  victim++;
	}
	if(victim < last_xact) {
	  assert(0 != getMachine(rb, victim));
	}
	break;
      }
      /*      for(i = 1; i < rb->size; i++) {
	assert(rb->buffer[i-1].machine_id < rb->buffer[i].machine_id  || 
	       (rb->buffer[i-1].machine_id == ULONG_MAX &&  rb->buffer[i].machine_id == ULONG_MAX));
	       }*/
      
    }
    

    free(rb->buffer);
    free(rb);
    free(bitmap);

    printf("New seed %d\n", new_seed);
    srand(new_seed);
    

  }

  return 0;


}
