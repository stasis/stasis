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
#include "monotree.h"
#include <limits.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#define EPOCH ((time_t)0)

int getMachineIndex(MonoTree * rb, state_machine_id id);
void compactBuffer(MonoTree * rb);
/** 
   Called when a new MonoTree is initialized (NOT after recovery.)

   This code takes a pointer to a MonoTree as an argument, since the
   MonoTree should be allocated elsewhere (by the transactional
   layer).
*/

void initStateMachine(StateMachine * stateMachine) {

    stateMachine->machine_id = ULONG_MAX;
    stateMachine->last_transition = EPOCH;
    stateMachine->page            = NULL;
    /*    stateMachine->page_id; */
    stateMachine->current_state   = NULL_STATE;      
    stateMachine->pending = 0;

    /* Intentionally does not free mutexes. */
}

void init_MonoTree(MonoTree * rb, int size) {
  int i;

  for(i = 0; i < size; i++) {
    StateMachine * stateMachine = &(rb->buffer[i]);
    initStateMachine(stateMachine);


  }
  rb->size = size;
  rb->low_water_mark = 0;
  rb->high_water_mark = 0;
  rb->next_id = 0;
}

StateMachine * allocMachine(MonoTree * rb/*, state_machine_id id*/) {

  StateMachine * new;

  if(rb->high_water_mark >= rb->size) {
    compactBuffer(rb);
  } 
  if(rb->high_water_mark >= rb->size) {
    return (StateMachine *)0;
  }
  new = &(rb->buffer[rb->high_water_mark]);
  rb->high_water_mark++;
  new->machine_id = rb->next_id;
  new->mutex = malloc(sizeof(pthread_mutex_t));
  new->sleepCond = malloc(sizeof(pthread_cond_t));

  pthread_mutex_init(new->mutex, NULL);
  pthread_cond_init(new->sleepCond, NULL);

  rb->next_id++;
  new->current_state = START_STATE;
  
  return new;
}

StateMachine * insertMachine(MonoTree * rb, state_machine_id id) {
  int new_index;
  StateMachine * new;
  int insertion_point;
  /* allocMachine is much less expensive than insertMachine, so this
     check is probably worth the trouble 

     I don't understand why, but this optimization was causing 2pc to
     fail, so I commented it out.

     -Rusty.
  */
  /*    if(id == rb->next_id) { 
    return allocMachine(rb); 
    } */
  if(rb->high_water_mark >= rb->size) {
    compactBuffer(rb);
  } 
  if(rb->high_water_mark >= rb->size) {
    return (StateMachine *)0;
  }
  /* Look up the new machine */

  new_index = getMachineIndex(rb, id);

  if(new_index < 0) {
    insertion_point = -(1+new_index);
    memmove(&(rb->buffer[insertion_point+1]), 
	    &(rb->buffer[insertion_point]),
	    sizeof(StateMachine) * (rb->high_water_mark - insertion_point));
    rb->high_water_mark++;
  } else {
    if(rb->buffer[new_index].current_state == NULL_STATE) {
      insertion_point = new_index;
    } else {
      return 0;
    }
  }
  
  new = &(rb->buffer[insertion_point]);
  new->machine_id = id;
  new->current_state = START_STATE;
  new->mutex = malloc(sizeof(pthread_mutex_t));
  new->sleepCond = malloc(sizeof(pthread_cond_t));
  pthread_mutex_init(new->mutex, NULL);
  pthread_cond_init(new->sleepCond, NULL);

  return new;  

}


void freeMachine(MonoTree * rb, state_machine_id id) {
  StateMachine * stateMachine;
  int old_index = getMachineIndex(rb, id);

  /* Needed for optimization to garbage collector. */

  if (old_index < rb->low_water_mark) { 
    rb->low_water_mark = old_index;
  }

  stateMachine = &(rb->buffer[old_index]);

  /* If either of these fail, then there's a bug above this line, 
     or someone attempted to free a machine that doesn't exist (anymore?). */

  assert(stateMachine->machine_id == id);
  assert(stateMachine->current_state != NULL_STATE);

  /* Leave the machine's id intact for now so that it can be used for binary search. */
  stateMachine->current_state = NULL_STATE;

  /* Needed so that alloc_machine can be correctly implemented. */

  if ((old_index + 1) == rb->high_water_mark) {
    rb->high_water_mark--;
    /* Since we're at the end of the array, we can do this. */
    stateMachine->machine_id = ULONG_MAX;
  }
  pthread_mutex_destroy(stateMachine->mutex);
  pthread_cond_destroy(stateMachine->sleepCond);
  free(stateMachine->mutex);
  free(stateMachine->sleepCond);


  /* The application is responsible for the memory management for page, so don't touch that either. */
}

StateMachine * getMachine(MonoTree * rb, state_machine_id id) {
  int index = getMachineIndex(rb, id);
  StateMachine * stateMachine;
  if(index < 0) { 
    return 0;
  } 
  stateMachine = &(rb->buffer[index]);
  if (stateMachine->current_state == NULL_STATE) {
    return 0;
  }
  assert(stateMachine->machine_id==id);

  return stateMachine;
}

StateMachine * enumerateMachines(MonoTree * rb, int* count) {
  compactBuffer(rb);

  *count = rb->high_water_mark;

  return rb->buffer;
}

/*-----------  PRIVATE FUNCTIONS ----------*/

/* Return the highest possible machine id if binary search 
 * falls off the end of the array.
 */
state_machine_id getMachineID(MonoTree *  rb, int index) {
  if(index >= rb->size) { 
    return ULONG_MAX;
  } else {
    return rb->buffer[index].machine_id;
  }
}

int __round_size_up_last_in = -1;
int __round_size_up_last_out = -1;

/* Find the smallest power of 2 greater than orig_size. */
int round_size_up(int orig_size) {
  int power_of_two = 0;
  
  int size = orig_size-1;
  
  if(orig_size == __round_size_up_last_in) {
    return __round_size_up_last_out;
  }
  
  
  assert(orig_size >= 0);

  if(orig_size == 0) {
    return 0;
  }

  /* What is the first power of two > size? */
  while(size != 0) {
    size /= 2;
    power_of_two++;
  }
  
  size = 1 << power_of_two;
  __round_size_up_last_in = orig_size;
  __round_size_up_last_out = size;

  return (int) size;

}

/**  
   Thanks to the spec of Java's Arrays.binarySearch() for this one. ;)

   This code doesn't check to see if the index that it returns is the
   correct one, or even that it contains a real machine, or even that
   it's less than the size of rb's buffer.
   
*/

int getMachineIndex(MonoTree * rb, state_machine_id id) {

  int size = round_size_up(rb->size);
  
  int start = 0;
  int stop  = size;
  int index;
  state_machine_id index_id;

  while(stop - start > 1) {
    assert(((start+stop) % 2) == 0);
    index = (start + stop) / 2;
    index_id = getMachineID(rb, index);

    if(index_id == id) {
      start = index;
      break;
    } else if(index_id < id) {
      start = index;
    } else {
      stop = index;
    }


  }
  if(id == getMachineID(rb, start)) {
    return start;
  }  else if(id == getMachineID(rb, stop)) {
    return stop;
  } else {
    int insertionPoint;
    int startID = getMachineID(rb,start);
    int stopID  = getMachineID(rb,stop);
    if(id < startID) {
      insertionPoint = start;
      assert(start == 0 || getMachineID(rb, start-1) < id);
    } else if(id < stopID) {
      insertionPoint = stop;
    } else {
      assert(id < getMachineID(rb, stop+1));
      insertionPoint = stop+1;
    }
    return (-(insertionPoint) - 1);
  }
  
}

void compactBuffer(MonoTree * rb) {

  int i;
  int buffer_pos = 0;
  int new_buffer_count = rb->high_water_mark-rb->low_water_mark;
  size_t new_buffer_size = sizeof(StateMachine)*new_buffer_count;
  StateMachine * new_buffer;
  int number_of_machines = rb->low_water_mark;
  if(rb->high_water_mark == rb->low_water_mark) {
    return;
  }

  new_buffer = malloc(new_buffer_size);
  for(i = rb->low_water_mark; i < rb->high_water_mark; i++) {
    if(rb->buffer[i].current_state != NULL_STATE) {
      memcpy(&(new_buffer[buffer_pos]), &(rb->buffer[i]), sizeof(StateMachine));
      buffer_pos++;
      number_of_machines++;
    }
  }
  for(i = buffer_pos; i < new_buffer_count; i++) {
    initStateMachine(&(new_buffer[i]));
  }
  memcpy(&(rb->buffer[rb->low_water_mark]), new_buffer, new_buffer_size);
  free(new_buffer);
  rb->low_water_mark = rb->high_water_mark = number_of_machines;
  return;
}
