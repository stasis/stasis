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
#include "2pc.h"
#include <assert.h>
#include <string.h>
#include "../libdfa/callbacks.h"

#define TRUE  1
#define FALSE 0
/* #define _TWO_PC 1 */

const int transition_count_2pc = 14;
const int client_transition_count_2pc = 4;
const int state_count_2pc = 9;



callback_fcn check_veto_2pc;
callback_fcn tally_2pc;
callback_fcn send_ack_2pc;
callback_fcn coordinator_init_xact_2pc; 
callback_fcn veto_or_prepare_2pc;
callback_fcn abort_2pc;
callback_fcn commit_2pc;


/* Remember to update transition_count_2pc if you add/remove transitions */
Transition transitions_2pc[] = {

  /* Coordinator transitions */

  /* Library user must provide callback that init_xact_2pc calls. */
  { AWAIT_ARRIVAL,            NULL_STATE,                 COORDINATOR_START_2PC,      coordinator_init_xact_2pc,       FALSE },
  { AWAIT_COMMIT_POINT,       NULL_STATE,                 COORDINATOR_START_2PC,      coordinator_init_xact_2pc,       FALSE },
  { AWAIT_RESULT,             NULL_STATE,                 COORDINATOR_START_2PC,      coordinator_init_xact_2pc,       FALSE },
  { NULL_STATE,               NULL_STATE,                 COORDINATOR_START_2PC,      coordinator_init_xact_2pc,       FALSE },

  /* TODO: tally_2pc and check_veto_2pc should respond to initiator where applicable. */
  { SUBORDINATE_VETO_2PC,     COORDINATOR_START_2PC,      COORDINATOR_ABORTING_2PC,   &check_veto_2pc,      TRUE }, 
  { SUBORDINATE_PREPARED_2PC, COORDINATOR_START_2PC,      COORDINATOR_COMMITTING_2PC, &tally_2pc,           TRUE }, 
  { SUBORDINATE_ACKING_2PC,   COORDINATOR_ABORTING_2PC,   NULL_STATE,                 &tally_2pc,           FALSE},
  { SUBORDINATE_ACKING_2PC,   COORDINATOR_COMMITTING_2PC, NULL_STATE,                 &tally_2pc,           FALSE},

  /* Subordinate transitions */
      /* veto_or_prepare overrides target state. */

  /* Library user must provide the subordinate function pointers for these transitions */
  { COORDINATOR_START_2PC,      NULL_STATE,               OVERRIDDEN_STATE,           &veto_or_prepare_2pc, TRUE },
  { COORDINATOR_ABORTING_2PC,   SUBORDINATE_PREPARED_2PC, NULL_STATE,                 &abort_2pc,           FALSE}, 
  { COORDINATOR_COMMITTING_2PC, SUBORDINATE_PREPARED_2PC, NULL_STATE,                 &commit_2pc,          FALSE},
  { COORDINATOR_ABORTING_2PC,   SUBORDINATE_VETO_2PC,     NULL_STATE,                 NULL,                 FALSE},

  /* transition fcn always fails, but sends ack to coordinator */
  { COORDINATOR_COMMITTING_2PC, NULL_STATE,               OVERRIDDEN_STATE,           &send_ack_2pc,        TRUE}, 
  { COORDINATOR_ABORTING_2PC,   NULL_STATE,               OVERRIDDEN_STATE,           &send_ack_2pc,        TRUE}, 

  

   
};

Transition client_transitions_2pc[] = {

 /* Caller transitions */

  { COORDINATOR_START_2PC,      AWAIT_ARRIVAL,          NULL_STATE,   NULL, FALSE},

  { COORDINATOR_COMMITTING_2PC, AWAIT_COMMIT_POINT,     NULL_STATE,   NULL, FALSE},
  { COORDINATOR_ABORTING_2PC,   AWAIT_COMMIT_POINT,     NULL_STATE,   NULL, FALSE},
  
  { SUBORDINATE_ACKING_2PC,     AWAIT_RESULT,           NULL_STATE,   NULL, FALSE},
  


};


State states_2pc[MAX_STATE_COUNT] = {

/* Coordinator states */

  { COORDINATOR_START_2PC,      NULL, NULL },  /* Need abort fcn */
  { COORDINATOR_COMMITTING_2PC, NULL, NULL },
  { COORDINATOR_ABORTING_2PC,   NULL, NULL },

/* Subordinate states */

  { SUBORDINATE_VETO_2PC,       NULL, NULL },  /* Need to think about callback fcns */
  { SUBORDINATE_PREPARED_2PC,   NULL, NULL },
  { SUBORDINATE_ACKING_2PC,     NULL, NULL },

  /* Client states */

  { AWAIT_ARRIVAL,      NULL, NULL},
  { AWAIT_RESULT,       NULL, NULL},
  { AWAIT_COMMIT_POINT, NULL, NULL},

};




/* 

   - add broadcast to messages.h  (Done)
   - change libdfa so that it scans the states array (Done)
   - add support for OVERRIDDEN_STATE to libdfa (Done, test it)
   - add support for transactions to libdfa (Need to replace monotree)
   - write the callback fcns. (~Done, test it!)

*/


/* Probably should ack the client that called commit(), instead of
   hoping that UDP got it here for them... ;) So, they should have a
   state machine that initiated the transaction, and waits for an ACK
   or NAK from us. */
state_name coordinator_init_xact_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCMachineState * state = (TwoPCMachineState*) &(stateMachine->app_state);
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));
  short bc_group = app_state->get_broadcast_group(dfaSet, m);
  int ret;

  if(!app_state->is_coordinator) {
    return 0;
  }
  
  printf("bc_group %d\n", bc_group);

  /* Need to check for this somewhere... */
  assert(sizeof(TwoPCAppState) <= MAX_APP_STATE_SIZE);

  memset(state->subordinate_votes, 0, MAX_SUBORDINATES);
  /*  state->xid = m->from_machine_id; */
  state->xid = stateMachine->machine_id;
  printf("From: %s", from);
  /*strncpy(state->initiator, from, MAX_ADDRESS_LENGTH);*/

  sprintf(from, "bc:%d\n", bc_group);

  /* TODO:  (n)ack the client.  (Implies yes / no / already pending return values for callback on last line)
     Currently, this is handled by the library user.  It could be moved back into here.

  */
  if(app_state->init_xact_2pc != NULL) {
    ret = app_state->init_xact_2pc(dfaSet, stateMachine, m, from);
  } else {
    ret = 1;
  }

  if(m->type==AWAIT_ARRIVAL && ret) {
    // need to (n)ack the client:
    // Respond using the machine id expected by the client.
    m->from_machine_id = m->initiator_machine_id;

    printf("Responding\n");
    
    respond_once(&((DfaSet*)dfaSet)->networkSetup, 
    		 COORDINATOR_START_2PC, m, m->initiator);
  }

  m->from_machine_id = stateMachine->machine_id;

  return ret;


}
state_name send_ack_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_ACKING_2PC, m, from);
  return OVERRIDDEN_STATE;
}

/**
   TODO: Can this be done in a way that avoids blocking?
*/
state_name veto_or_prepare_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));

  int ret = app_state->veto_or_prepare_2pc(dfaSet, stateMachine, m, from);
  
  return ret;
}

/**
   TODO:  The next two functions should fork and immediately return true.
*/

state_name abort_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));
  send_ack_2pc(dfaSet, stateMachine, m, from);
  int ret = app_state->abort_2pc(dfaSet, stateMachine, m, from);

  //if((*responseType(m) == AWAIT_COMMIT_POINT || *responseType(m) == AWAIT_RESULT)) {
  if(m->response_type == AWAIT_COMMIT_POINT || m->response_type == AWAIT_RESULT) {
    state_machine_id tmp = m->from_machine_id;

    /* TODO:  Could the chages to from_machine_id be moved into libdfa (it does this anyway, but it does it too late.) */
    m->from_machine_id = m->initiator_machine_id; /*stateMachine->machine_id;*/

    printf("Response being sent to: %s:%ld\n", m->initiator, m->to_machine_id);
    respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_VETO_2PC, m, m->initiator);
    m->from_machine_id = tmp;
  }
  return ret;
}

state_name commit_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));

  send_ack_2pc(dfaSet, stateMachine, m, from);
  return app_state->commit_2pc(dfaSet, stateMachine, m, from);
}

state_name check_veto_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  /* Clear subordinate_votes array, so that it can be used to
     tally acks after the votes are tallied. */

  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));
  TwoPCMachineState * machine_state = (TwoPCMachineState*)&(stateMachine->app_state);

  /* if (!check_from()) { return 0; } */
  short bc_group = app_state->get_broadcast_group(dfaSet, m);
  
  printf("bc_group:veto %d\n", bc_group);

  memset(machine_state->subordinate_votes, 0, MAX_SUBORDINATES);
  sprintf(from, "bc:%d", bc_group);

  return 1;
}

state_name tally_2pc(void * dfaSetPtr, StateMachine * stateMachine, Message * m, char * from) {

  TwoPCMachineState * machine_state = (TwoPCMachineState*)&(stateMachine->app_state);
  DfaSet * dfaSet = (DfaSet*) dfaSetPtr;

  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));

  /* if (!check_from()) { return 0; } */
  short bc_group = app_state->get_broadcast_group(dfaSet, m);

  if(bc_group < dfaSet->networkSetup.broadcast_lists_count) {
    state_name ret = tally(dfaSet->networkSetup.broadcast_lists[bc_group],
		 dfaSet->networkSetup.broadcast_list_host_count[bc_group],
		 (char*)(machine_state->subordinate_votes), from);
    if(ret) {
      /* Clear subordinate_votes array, so that it can be used to
	 tally acks after the votes are tallied. */
      memset(machine_state->subordinate_votes, 0, MAX_SUBORDINATES);
    }


    /* Needed to use the from variable to do the tally, so this
       sprintf needs to be down here. */

    sprintf(from, "bc:%d", bc_group);

    if(ret && app_state->tally_2pc != NULL) {  
      ret = app_state->tally_2pc(dfaSet, stateMachine, m, from);
    }
  /* TODO: CORRECTNESS BUG Make sure this is after tally forces the log. Also, need to
     make sure that it increments the (currently unimplemented)
     sequence number before flushing... */

    if(ret && (m->response_type == AWAIT_COMMIT_POINT && m->response_type == COORDINATOR_START_2PC)) { 
//    if(ret && (*responseType(m) == AWAIT_COMMIT_POINT && stateMachine->current_state==COORDINATOR_START_2PC)) {
	state_machine_id tmp = m->from_machine_id;
	m->from_machine_id = m->initiator_machine_id;
	//printf("Coordinator responding: ? ht=? (key length %d) %d ->   to %s:%ld\n",  getKeyLength(m), *(int*)getKeyAddr(m), /*getValAddr(m),*/ m->initiator, m->initiator_machine_id );
	respond_once(&((DfaSet*)dfaSet)->networkSetup, COORDINATOR_COMMITTING_2PC, m, m->initiator);
	m->from_machine_id = tmp;
    }
    return ret; 
  } else {
    sprintf(from, "bc:%d", bc_group);
  
    return FALSE;
  }
  
}
