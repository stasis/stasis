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
#include "../../libdfa/libdfa.h"
#include <stdio.h>
#include <malloc.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
callback_fcn recv_ping, recv_pong;

#define PING1 3
#define PING2 4
#define PONG1 1
#define PONG2 2
int main (int argc, char** argv) {
  DfaSet * dfaSet = calloc(1, sizeof(DfaSet));
  /*  callback_fcn* callbacks[MAX_MESSAGE_COUNT]; */
      
  Transition * transitions = malloc (sizeof(Transition) * 4);

  State * states = malloc(sizeof(State) * MAX_STATE_COUNT);
  StateMachine * initial_sm1;
  StateMachine * initial_sm2;
  int i;

  /*dfaSet->monoTree.buffer = calloc(DFA_MACHINE_COUNT, sizeof(StateMachine)); */
  dfa_initialize_new (dfaSet, 10000, DFA_MACHINE_COUNT);
  for(i = 0; i < DFA_MACHINE_COUNT/4; i++) {
    /*    StateMachine initial_sm1_stack;
	  StateMachine initial_sm2_stack;*/
    
    /*    initial_sm1 = allocMachine(&(dfaSet->monoTree));
	  initial_sm2 = allocMachine(&(dfaSet->monoTree)); */

    initial_sm1 = allocSmash(dfaSet->smash);
    initial_sm2 = allocSmash(dfaSet->smash);

    
    initial_sm1->message.from_machine_id = initial_sm1->machine_id;
    initial_sm1->message.to_machine_id = initial_sm2->machine_id;
    initial_sm2->message.from_machine_id = initial_sm2->machine_id;
    initial_sm2->message.to_machine_id = initial_sm1->machine_id;
    
    printf("sm1 %ld, sm2 %ld\n", initial_sm1->machine_id, initial_sm2->machine_id);
    
    initial_sm1->current_state = PING1;
    initial_sm2->current_state = PONG1;
    
    initial_sm1->last_transition = (time_t)0;
    initial_sm2->last_transition = (time_t)0;
    
    strcpy(initial_sm1->message_recipient, "127.0.0.1:10000");
    strcpy(initial_sm2->message_recipient, "127.0.0.1:10000");
  }

  states[0].name      = PING1;
  states[0].retry_fcn = NULL;
  states[0].abort_fcn = NULL;
  
  states[1].name      = PING2;
  states[1].retry_fcn = NULL;
  states[1].abort_fcn = NULL;
  
  states[2].name      = PONG1;
  states[2].retry_fcn = NULL;
  states[2].abort_fcn = NULL;
  
  states[3].name      = PONG2;
  states[3].retry_fcn = NULL;
  states[3].abort_fcn = NULL;
  
  transitions[0].remote_state   = PONG1;
  transitions[0].pre_state = PING1;
  transitions[0].post_state= PING2;
  transitions[0].fcn_ptr   = &recv_ping;
  transitions[0].force     = 1;
  
  transitions[1].remote_state   = PONG2;
  transitions[1].pre_state = PING2;
  transitions[1].post_state= PING1;
  transitions[1].fcn_ptr   = &recv_ping;
  transitions[1].force     = 1;

  transitions[2].remote_state   = PING1;
  transitions[2].pre_state = PONG2;
  transitions[2].post_state= PONG1;
  transitions[2].fcn_ptr   = &recv_pong;
  transitions[2].force     = 1;

  transitions[3].remote_state   = PING2;
  transitions[3].pre_state = PONG1;
  transitions[3].post_state= PONG2;
  transitions[3].fcn_ptr   = &recv_pong;
  transitions[3].force     = 1;

  if(dfa_reinitialize(dfaSet, "127.0.0.1:10000", transitions, 4, states, 4) < 0) {
    printf("Error.  Exiting.\n");
  }

  main_loop(dfaSet);
  /* Can't get here. */
  return 0;
}

state_name recv_ping(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  if(stateMachine != NULL) {
    printf("%ld(%d): Got a ping from %s Machine %ld\n", stateMachine->machine_id, stateMachine->current_state, from, m->from_machine_id);
    return 1;
  } else {
    printf("Got message from %s for non-existant machine.\n", from);
    return 0;
  }
}

state_name recv_pong(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  if(stateMachine != NULL) {
    printf("%ld(%d): Got a pong from %s Machine %ld\n", stateMachine->machine_id, stateMachine->current_state, from, m->from_machine_id);
    return 1;
  } else {
    printf("Got message from %s for non-existant machine.\n", from);
    return 0;
  }
}
