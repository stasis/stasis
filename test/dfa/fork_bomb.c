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
#include <stdlib.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
callback_fcn recv_new, recv_ack;

#define FORKED 1

  /*

   ->START
     |
     |            
   <-|        <---  S:S->F1
     FORKED    START:nil->S ---->
     |        
     |        <---  S:F1->nil
     |        START:nil->S ---->
   -----
    ---
     -

  TODO:  Need api to let me say:

    Abort timeout is 5 sec.
    Infininte retry timout.

   What does infinite retry timeout mean for recovery?  

   Should it be supported by libdfa?

  */

int main (int argc, char** argv) {
  DfaSet * dfaSet = calloc(1, sizeof(DfaSet));
  /*  callback_fcn* callbacks[MAX_MESSAGE_COUNT]; */
  
  Transition * transitions = malloc (sizeof(Transition) * 3);

  State * states = malloc(sizeof(State) * MAX_STATE_COUNT);
  /*  StateMachine initial_sm1_stack; */
  StateMachine * initial_sm1;
  int transition_count;

  /*  dfaSet->monoTree.buffer = calloc(DFA_MACHINE_COUNT, sizeof(StateMachine));
      dfa_initialize_new (dfaSet, 10001, DFA_MACHINE_COUNT); */
  /*   dfaSet->monoTree.buffer = calloc(100, sizeof(StateMachine)); */
  dfa_initialize_new (dfaSet, 10001, 100);
  
  /*  initial_sm1 = allocMachine(&(dfaSet->monoTree)); */
  assert(NULL != (initial_sm1 = allocSmash(dfaSet->smash)));
  
  initial_sm1->message.from_machine_id = initial_sm1->machine_id;
  initial_sm1->message.to_machine_id   = NULL_MACHINE;
  initial_sm1->message.type = NULL_STATE;
  /* What to do here ???? 
     initial_sm1->message.machine_id = initial_sm2->machine_id;
     initial_sm2->message.machine_id = initial_sm1->machine_id;
  */
  
  printf("sm1 %ld\n", initial_sm1->machine_id);

  initial_sm1->current_state = START_STATE;
    
  initial_sm1->last_transition = (time_t)0;
  
  strcpy(initial_sm1->message_recipient, "127.0.0.1:10001");

  setSmash(dfaSet->smash, initial_sm1->machine_id);
  
  states[0].name      = FORKED;
  states[0].retry_fcn = NULL;
  states[0].abort_fcn = NULL;

  transitions[0].remote_state = NULL_STATE;
  transitions[0].pre_state    = NULL_STATE;
  transitions[0].post_state   = START_STATE;
  transitions[0].fcn_ptr      = &recv_new;
  transitions[0].force        = 0;

  transitions[1].remote_state = START_STATE;
  transitions[1].pre_state    = START_STATE;
  transitions[1].post_state   = FORKED;
  transitions[1].fcn_ptr      = &recv_ack;
  transitions[1].force        = 0;

  transitions[2].remote_state = START_STATE;
  transitions[2].pre_state    = FORKED;
  transitions[2].post_state   = NULL_STATE;
  transitions[2].fcn_ptr      = &recv_ack;
  transitions[2].force        = 0;

  /* Needed for bootstrap... */

  transitions[3].remote_state = START_STATE;
  transitions[3].pre_state    = NULL_STATE;
  transitions[3].post_state   = START_STATE;
  transitions[3].fcn_ptr      = &recv_new;
  transitions[3].force        = 0;

  transition_count = 4;

  if(dfa_reinitialize(dfaSet, "127.0.0.1:10001", transitions, transition_count, states, 1) < 0) {
    printf("Error.  Exiting.\n");
  }

  main_loop(dfaSet);


  return 0;
}

void send_new_fork_request(DfaSet * dfaSet, StateMachine * stateMachine, char * from) {
  
  Message new_m;

  new_m.from_machine_id = stateMachine->machine_id;
  new_m.to_machine_id   = NULL_MACHINE;
  new_m.type       = NULL_STATE;
  
  send_message(&(dfaSet->networkSetup), &(new_m), from);
}

state_name recv_new(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  if(stateMachine == NULL) {
    printf("Got a request for a new thread from %s, machine %ld\n", from, m->from_machine_id);    
  } else {
    printf("%d: Machine %ld got a request for a new thread from %s, machine %ld\n", stateMachine->current_state, stateMachine->machine_id,
	   from, m->from_machine_id);
    send_new_fork_request(dfaSet, stateMachine, from);
  }

  
  return 1;
}

state_name recv_ack(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  if(stateMachine != NULL) {
    printf("%d: Machine %ld heard back from child: from %s : machine %ld\n", stateMachine->current_state, stateMachine->machine_id,
	   from, m->from_machine_id);
    send_new_fork_request(dfaSet, stateMachine, from);
    return 1;
  } else {
    printf("Dropping ack for machine that doesn't exist.\n");
    return 0;
  }


}
