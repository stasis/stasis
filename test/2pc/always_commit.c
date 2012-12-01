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
#include "../../src/2pc/2pc.h"

#include <assert.h>
#include <string.h>

#define TRUE 1
#define FALSE 0

char** broadcast_lists[2];
char* star_nodes[] = { "127.0.0.1:10010" };
char* point_nodes[] = { "127.0.0.1:10011", 
			"127.0.0.1:10012",
			"127.0.0.1:10013",
			"127.0.0.1:10014",
			"127.0.0.1:10015" };

state_name always_prepare(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  return SUBORDINATE_PREPARED_2PC;
}
state_name assert_false(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  assert(0);
  return FALSE;
}
state_name print_commit(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  printf("commit xid:%ld\n", stateMachine->machine_id);
  return TRUE;
}

short bc_group_1(DfaSet* dfaSet, Message * m) {
  return 1;
}

int broadcast_list_host_count [] = { 1, 5 };

int broadcast_lists_count = 2;

int main (int argc, char ** argv) {

  short port;
  DfaSet * dfaSet;
  TwoPCAppState * app_state;
  char * localhost;
  assert(argc >= 2);

  broadcast_lists[0] = star_nodes;
  broadcast_lists[1] = point_nodes;

  app_state = stasis_alloc(TwoPCAppState);
  
  if (!strcmp(argv[1], "c")) {
    assert(argc == 2);
    port = parse_port(broadcast_lists[0][0]);
    app_state->is_coordinator = TRUE;
    dfaSet = dfa_malloc_old(DFA_MACHINE_COUNT, port, broadcast_lists, 
			broadcast_lists_count, broadcast_list_host_count);

    localhost = broadcast_lists[0][0];
  } else if (!strcmp(argv[1], "s")) {
    int replica;
    
    assert(argc == 3);
    replica = atoi(argv[2]);
    port = parse_port(broadcast_lists[1][replica]);
    app_state->is_coordinator = FALSE;
      dfaSet = dfa_malloc_old(DFA_MACHINE_COUNT * 10, port, broadcast_lists, 
		      broadcast_lists_count, broadcast_list_host_count);
    localhost = broadcast_lists[1][replica];
  }else {
    Message * m = stasis_alloc(Message);
    NetworkSetup * ns = stasis_alloc(NetworkSetup);
    init_network_broadcast(ns, 12345, "127.0.0.1:12345", broadcast_lists, broadcast_lists_count, broadcast_list_host_count);

    m->to_machine_id = atoi(argv[2]);
    m->from_machine_id = m->to_machine_id;
    m->type = NULL_STATE;

    send_message(ns, m, "bc:0");
    
    return 0;
  }

  
  
  if(dfa_reinitialize(dfaSet, localhost, transitions_2pc, transition_count_2pc, states_2pc, state_count_2pc) < 0) {
    perror("dfa_reinitialize failed");
  }

  
  app_state->init_xact_2pc = NULL;
  app_state->veto_or_prepare_2pc = &always_prepare;
  app_state->abort_2pc = &assert_false;
  app_state->commit_2pc = &print_commit;
  app_state->get_broadcast_group = &bc_group_1;
  app_state->tally_2pc = NULL;

  dfaSet->app_setup = app_state;
  
  if(!strcmp(argv[1], "c")) {

    main_loop(dfaSet);
  } else if (!strcmp(argv[1], "s")) {
    main_loop(dfaSet);

  } 


  printf("transition_count_2pc=%d\n", transition_count_2pc);
  printf("transitions[0].initial_state=%d\n", transitions_2pc[0].remote_state);
  return 0;
}
