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



callback_fcn check_veto_2pc;
callback_fcn tally_2pc;
callback_fcn send_ack_2pc;
callback_fcn coordinator_init_xact_2pc; 
callback_fcn veto_or_prepare_2pc;
callback_fcn abort_2pc;
callback_fcn commit_2pc;
callback_fcn eval_action_2pc;
/* new fcns */

callback_fcn coordinator_continue_xact_2pc;

/* Remember to update transition_count_2pc if you add/remove transitions */

const int transition_count_2pc = 25;

Transition transitions_2pc[] = {

  /* Coordinator transitions */

  { XACT_ACK_ARRIVAL,       NULL_STATE,                 XACT_ACTION_RUNNING,   coordinator_init_xact_2pc,       FALSE },
  { XACT_ACK_RESULT,        NULL_STATE,                 XACT_ACTION_RUNNING,   coordinator_init_xact_2pc,       FALSE },

  { XACT_ACK_ARRIVAL,       XACT_ACTIVE,                XACT_ACTION_RUNNING,   coordinator_continue_xact_2pc,   FALSE },
  { XACT_ACK_RESULT,        XACT_ACTIVE,                XACT_ACTION_RUNNING,   coordinator_continue_xact_2pc,   FALSE },
  { XACT_COMMIT,            XACT_ACTIVE,                COORDINATOR_START_2PC, coordinator_continue_xact_2pc,     FALSE },
  { XACT_SUBORDINATE_ACK,   XACT_ACTION_RUNNING,        XACT_ACTIVE,           &tally_2pc,                        FALSE },

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

  { XACT_ACTION_RUNNING,   NULL_STATE,               XACT_ACTIVE,           &eval_action_2pc,         FALSE},
  { XACT_ACTION_RUNNING,   XACT_ACTIVE,              XACT_ACTIVE,           &eval_action_2pc,         FALSE},

  /* Library user must provide the subordinate function pointers for these transitions */
  { COORDINATOR_START_2PC,      NULL_STATE,               OVERRIDDEN_STATE,           &veto_or_prepare_2pc, TRUE },
  { COORDINATOR_START_2PC,      XACT_ACTIVE,             OVERRIDDEN_STATE,            &veto_or_prepare_2pc, TRUE },
  { COORDINATOR_ABORTING_2PC,   SUBORDINATE_PREPARED_2PC, NULL_STATE,                 &abort_2pc,           FALSE}, 
  { COORDINATOR_COMMITTING_2PC, SUBORDINATE_PREPARED_2PC, NULL_STATE,                 &commit_2pc,          FALSE},
  { COORDINATOR_ABORTING_2PC,   SUBORDINATE_VETO_2PC,     NULL_STATE,                 NULL,                 FALSE},

  /* transition fcn always fails, but sends ack to coordinator */
  { COORDINATOR_COMMITTING_2PC, NULL_STATE,               OVERRIDDEN_STATE,           &send_ack_2pc,        TRUE}, 
  { COORDINATOR_ABORTING_2PC,   NULL_STATE,               OVERRIDDEN_STATE,           &send_ack_2pc,        TRUE}, 
  { COORDINATOR_COMMITTING_2PC, XACT_ACTIVE,               OVERRIDDEN_STATE,           &send_ack_2pc,        TRUE}, 
  { COORDINATOR_ABORTING_2PC,   XACT_ACTIVE,               OVERRIDDEN_STATE,           &send_ack_2pc,        TRUE}, 

};

const int client_transition_count_2pc = 7;

Transition client_transitions_2pc[] = {

 /* Caller transitions */

  { COORDINATOR_START_2PC,      AWAIT_ARRIVAL,          NULL_STATE,   NULL, FALSE},

  { COORDINATOR_COMMITTING_2PC, AWAIT_COMMIT_POINT,     NULL_STATE,   NULL, FALSE},
  { COORDINATOR_ABORTING_2PC,   AWAIT_COMMIT_POINT,     NULL_STATE,   NULL, FALSE},
  
  { SUBORDINATE_ACKING_2PC,     AWAIT_RESULT,           NULL_STATE,   NULL, FALSE},

  { XACT_ACTION_RUNNING,        XACT_ACK_ARRIVAL,       NULL_STATE,   NULL, FALSE},
  { XACT_ACTIVE,                XACT_ACK_RESULT,        NULL_STATE,   NULL, FALSE},
  { COORDINATOR_COMMITTING_2PC, XACT_COMMIT,            NULL_STATE,   NULL, FALSE}

};


const int state_count_2pc = 15;

State states_2pc[MAX_STATE_COUNT] = {

/* Coordinator states */

  { COORDINATOR_START_2PC,      NULL, NULL },  /* Need abort fcn */
  { COORDINATOR_COMMITTING_2PC, NULL, NULL },
  { COORDINATOR_ABORTING_2PC,   NULL, NULL },

  { XACT_ACTION_RUNNING,   NULL, NULL },

/* Subordinate states */

  { SUBORDINATE_VETO_2PC,       NULL, NULL },  /* Need to think about callback fcns */
  { SUBORDINATE_PREPARED_2PC,   NULL, NULL },
  { SUBORDINATE_ACKING_2PC,     NULL, NULL },

  { XACT_SUBORDINATE_ACK,       NULL, NULL },  /* Just used for one-time acks. */

  /* mixed client/subordinate states */

  { XACT_ACTIVE,        callback_false, NULL}, /* Never send a message indicating the current state. */
 
  /* Client states */

  { AWAIT_ARRIVAL,      NULL, NULL},
  { AWAIT_RESULT,       NULL, NULL},
  { AWAIT_COMMIT_POINT, NULL, NULL},

  { XACT_ACK_RESULT,    NULL, NULL},
  { XACT_ACK_ARRIVAL,   NULL, NULL},

  { XACT_COMMIT,        NULL, NULL},

};

/* 

   - add broadcast to messages.h  (Done)
   - change libdfa so that it scans the states array (Done)
   - add support for OVERRIDDEN_STATE to libdfa (Done, test it)
   - add support for transactions to libdfa (Need to replace monotree)
   - write the callback fcns. (~Done, test it!)

*/

state_name coordinator_continue_xact_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  int ret;
  TwoPCMachineState * state = (TwoPCMachineState*) &(stateMachine->app_state);
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));

  /** Overwrite the from parameter, with the appropriate broadcast group. */
  short bc_group = app_state->get_broadcast_group(dfaSet, m);
  sprintf(from, "bc:%d\n", bc_group);

  memset(state->subordinate_votes, 0, MAX_SUBORDINATES);

  if(app_state->continue_xact_2pc != NULL) {
    ret = app_state->continue_xact_2pc(dfaSet, stateMachine, m, from);
  } else {
    ret = 1;
  }

  //  printf("CONTINUE %ld\n", m->initiator_machine_id);
  if(m->type==AWAIT_ARRIVAL && ret) {
    // need to (n)ack the client:
    // Respond using the machine id expected by the client.
    m->from_machine_id = m->initiator_machine_id;

    /** @todo What if the message to the client is dropped?  Is there an easy way to just ACK if it resends? */
    printf("ACK %ld (to %s)\n", m->initiator_machine_id, m->initiator);
    
    respond_once(&((DfaSet*)dfaSet)->networkSetup, 
    		 COORDINATOR_START_2PC, m, m->initiator);
  }

  m->from_machine_id = stateMachine->machine_id;

  return ret;
}

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
  
  /* Need to check for this somewhere... */
  assert(sizeof(TwoPCAppState) <= MAX_APP_STATE_SIZE);

  memset(state->subordinate_votes, 0, MAX_SUBORDINATES);
  state->xid = stateMachine->machine_id;

  if(strncmp(m->initiator, from, MAX_ADDRESS_LENGTH)) {
    printf("WARNING:  Mismatch between request source (%s) and initiator field (%s).   Proceeding. (Trusting the client)\n", m->initiator, from);
  }
  
  sprintf(from, "bc:%d\n", bc_group);

  if(app_state->init_xact_2pc != NULL) {
    ret = app_state->init_xact_2pc(dfaSet, stateMachine, m, from);
  } else {
    ret = 1;
  }

  printf("INIT %ld\n", m->initiator_machine_id); fflush(stdout);
  if(m->type==AWAIT_ARRIVAL && ret) {
    // (n)ack the client: Respond using the machine id expected by the client.
    m->from_machine_id = m->initiator_machine_id;
    
    printf("ACK %ld (to %s)\n", m->initiator_machine_id, m->initiator);
    
    respond_once(&((DfaSet*)dfaSet)->networkSetup, 
    		 COORDINATOR_START_2PC, m, m->initiator);
  }

  m->from_machine_id = stateMachine->machine_id;

  return ret;


}
state_name send_ack_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  printf("ACK %ld\n", m->to_machine_id);
  respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_ACKING_2PC, m, from);
  return OVERRIDDEN_STATE;
}

/**
   TODO: Can this be done in a way that avoids blocking?
*/
state_name veto_or_prepare_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));

  int ret = app_state->veto_or_prepare_2pc(dfaSet, stateMachine, m, from);

  if(ret == SUBORDINATE_VETO_2PC) {
    printf("VETO %ld\n", m->to_machine_id);
  } else {
    assert(ret == SUBORDINATE_PREPARED_2PC);
    printf("PREPARE %ld\n", m->to_machine_id);
  }
  return ret;
}

state_name eval_action_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));
  int ret = app_state->eval_action_2pc(dfaSet, stateMachine, m, from);
  if(ret) {

    respond_once(&((DfaSet*)dfaSet)->networkSetup, 
    		 XACT_SUBORDINATE_ACK, m, from);
  } 
  /* Just ack with the respond_once.  Don't need to change states. */
  if(stateMachine->current_state == XACT_ACTIVE) {
    return 0;
  } else {
    return 1;
  }
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

    printf("ABORT SUBORDINATE_VETO being sent to: %s:%ld\n", m->initiator, m->to_machine_id);
    respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_VETO_2PC, m, m->initiator);
    m->from_machine_id = tmp;
  } else {
    printf("ABORT %ld\n", m->to_machine_id);
  }
  return ret;
}

state_name commit_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));

  send_ack_2pc(dfaSet, stateMachine, m, from);
  int ret = app_state->commit_2pc(dfaSet, stateMachine, m, from);
  if(ret) { printf("COMMIT %ld\n", m->to_machine_id); }
  if(ret && m->response_type == AWAIT_RESULT) {
    printf("responding w/ commit to %s\n", m->initiator); fflush(stdout);
    respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_ACKING_2PC, m, m->initiator);
  }
  return ret;
}
/** @todo 2pc can't handle replica groups of size one. */
state_name check_veto_2pc(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  /* Clear subordinate_votes array, so that it can be used to
     tally acks after the votes are tallied. */

//  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));
  TwoPCMachineState * machine_state = (TwoPCMachineState*)&(stateMachine->app_state);

  /* if (!check_from()) { return 0; } */
//  short bc_group = app_state->get_broadcast_group(dfaSet, m);
  
//  printf("bc_group:veto %d\n", bc_group);

  memset(machine_state->subordinate_votes, 0, MAX_SUBORDINATES);
 // sprintf(from, "bc:%d", bc_group);
  printf("Sending commit message to %s\n", m->initiator);
  respond_once(&((DfaSet*)dfaSet)->networkSetup, COORDINATOR_COMMITTING_2PC, m, m->initiator);

  if(tally_2pc(dfaSet, stateMachine, m, from)) {
    printf("YOU FOUND A BUG: 2pc doesn't support size one replication groups!\n");
  }

  return 1;
}

state_name tally_2pc(void * dfaSetPtr, StateMachine * stateMachine, Message * m, char * from) {

  TwoPCMachineState * machine_state = (TwoPCMachineState*)&(stateMachine->app_state);
  DfaSet * dfaSet = (DfaSet*) dfaSetPtr;

  TwoPCAppState * app_state = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));
  
  short bc_group = app_state->get_broadcast_group(dfaSet, m);
  //  fprintf(stderr, "tally: %s, broadcast group: %d\n", from, bc_group);
  if(bc_group < dfaSet->networkSetup.broadcast_lists_count+1) {
    state_name ret;
    if(bc_group == ALL_BUT_GROUP_ZERO) {
      char ** list;
      int count = consolidate_bc_groups(&list, &dfaSet->networkSetup);
      //      int i;
      /*      for(i = 0; i < count; i++) {
	fprintf(stderr, "count = %d tallyhost %d: %s\n", count, i, list[i]);
	}  */
      ret = tally(list, count, (char*)(machine_state->subordinate_votes), from);
      free(list);
    } else {
      ret = tally(dfaSet->networkSetup.broadcast_lists[bc_group-1],
		   dfaSet->networkSetup.broadcast_list_host_count[bc_group-1],
		   (char*)(machine_state->subordinate_votes), from);
    }
    //    fprintf(stderr, "Tally returned: %d", ret);
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


    //    fprintf(stderr, " response type %d current state %d\n", m->response_type, stateMachine->current_state);
    if(ret && ( 
                (  
                  (
		    m->response_type == AWAIT_COMMIT_POINT || 
		    m->response_type==XACT_COMMIT
	          ) && (
		    stateMachine->current_state == COORDINATOR_START_2PC ||
		    stateMachine->current_state == COORDINATOR_ABORTING_2PC
		  )
		) || ( 
                    m->response_type == XACT_ACK_RESULT && 
		    stateMachine->current_state == XACT_ACTION_RUNNING
                )
	       )
       ) { 
      //      printf("sending ack to %s (%d)\n", m->initiator, m->response_type); fflush(stdout);

	state_machine_id tmp = m->from_machine_id;
	m->from_machine_id = m->initiator_machine_id;
//	printf("Coordinator responding: ? ht=? (key length %d) %d -> /*%d*/  to %s:%ld\n",  0/*getKeyLength(m),*/ , 0, 0, /**(int*)getKeyAddr(m), *//**(int*)getValAddr(m),*/ m->initiator, m->initiator_machine_id );
        //debug_print_message(m);
	if((m->response_type == AWAIT_COMMIT_POINT) || (m->response_type == XACT_COMMIT)) {
	  printf("COMMIT POINT %ld\n", m->to_machine_id); 
	  fflush(stdout);
	  printf("Sending commit point message to %s\n", m->initiator); fflush(stdout);
	  respond_once(&((DfaSet*)dfaSet)->networkSetup, COORDINATOR_COMMITTING_2PC, m, m->initiator);
	} else {
	  //	  printf("COMPLETED %ld\n", m->to_machine_id);
	  respond_once(&((DfaSet*)dfaSet)->networkSetup, XACT_ACTIVE, m, m->initiator);
	}
	m->from_machine_id = tmp;
    }
 //   printf("\n");
    return ret; 
  } else {
    sprintf(from, "bc:%d", bc_group);
  
    return FALSE;
  }
  
}
