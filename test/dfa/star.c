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
#include "../../src/libdfa/callbacks.h"
#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "assert.h"

#define TRUE 1
#define FALSE 0

char** broadcast_lists[2];
char* star_nodes[] = { "127.0.0.1:10010" };
char* point_nodes[] = { "127.0.0.1:10011", 
			"127.0.0.1:10012",
			"127.0.0.1:10013",
			"127.0.0.1:10014",
			"127.0.0.1:10015" };




int broadcast_list_host_count [] = { 1, 5 };

int broadcast_lists_count = 2;

/* States */

#define HUB_START 101

#define POINT_START 201

callback_fcn tally_star, respond_once_star;

State states_star[MAX_STATE_COUNT] = {
  { HUB_START, NULL, NULL },
  { POINT_START, NULL, NULL },

};

Transition transitions_star[] = {

  {POINT_START, HUB_START,  NULL_STATE,  tally_star, FALSE},
  {HUB_START,   NULL_STATE, POINT_START, respond_once_star,  FALSE},

};

/*state_machine_id init_hub(DfaSet * dfaSet) {
  StateMachine * initial_sm;

  initial_sm = allocMachine(&(dfaSet->monoTree));
  initial_sm->current_state = HUB_START;
  initial_sm->message.from_machine_id = initial_sm->machine_id;
  initial_sm->message.to_machine_id = NULL_MACHINE;
  initial_sm->message.type = HUB_START;
  
  strcpy(initial_sm->message_recipient, "bc:1");
  return initial_sm->machine_id;
  }

void init_point(DfaSet * dfaSet, int node_number) {

} */

int main (int argc, char ** argv) {
  int list_number;
  int node_number;


  DfaSet * dfaSet;
  short port;

  broadcast_lists[0] = star_nodes;
  broadcast_lists[1] = point_nodes;

  assert(argc == 3);

  list_number = atoi(argv[1]); 
  node_number = atoi(argv[2]);
  
  assert(list_number < broadcast_lists_count);
  assert(node_number < broadcast_list_host_count[list_number]);

  port = parse_port(broadcast_lists[list_number][node_number]);

  dfaSet = dfa_malloc_old(DFA_MACHINE_COUNT, port, broadcast_lists, 
			       broadcast_lists_count, broadcast_list_host_count);

  if(list_number == 0) {
    int ret;

    dfa_reinitialize(dfaSet, broadcast_lists[list_number][node_number], transitions_star, 2, states_star, 2);

    spawn_main_thread(dfaSet);
    
    ret =(int) request(dfaSet, HUB_START, "bc:1", NULL_MACHINE, NULL);
    
    printf("run_request_returned: %x\n", ret);
    
  } else {
    
    /*    init_point(dfaSet, node_number); */

    if(dfa_reinitialize(dfaSet, broadcast_lists[list_number][node_number], transitions_star, 2, states_star, 2) < 0) {
      printf("Error.  Exiting.\n");
    }
    main_loop(dfaSet);
  }

  return 0;
}

state_name respond_once_star(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  respond_once(&((DfaSet*)dfaSet)->networkSetup, POINT_START, m, from);
  return 0;
}

state_name tally_star(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  return tally(broadcast_lists[1], broadcast_list_host_count[1], (char*)(&(stateMachine->app_state)), from);
}
