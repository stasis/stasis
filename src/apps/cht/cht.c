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
#include "cht.h"
#include <netinet/in.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "../../2pc/2pc.h"
#include "../../libdfa/callbacks.h"
#include <pbl/jbhash.h>
#include "cht_message.h"





/** TODO: Endianness. (ICK) */
//state_name request_type =*(requestType(m));                                                 \sdsd
//state_name response_type =*(responseType(m));                                               \sd
//TwoPCMachineState * machine_state_2pc = (TwoPCMachineState*) &(stateMachine->app_state);    \sd





/**
 * Hash function generator, taken directly from pblhash
 */
static int hash( const unsigned char * key, size_t keylen , int table_length) {
    int ret = 104729;

    for( ; keylen-- > 0; key++ )
    {
        if( *key )
        {
            ret *= *key + keylen;
            ret %= table_length;
        }
    }

    return( ret % table_length );
}


/** TODO: multiplex_interleaved needs to return a special group for
    begin/commit/abort requests.  There will then be two types of
    transactions.  Explicit ones will create xids on all replicas, and
    span multiple operations.  Implcit ones will only span one replica
    group, but may only contain a single operation.
*/
short multiplex_interleaved(DfaSet * dfaSet, Message * m) {
  short table_length = dfaSet->networkSetup.broadcast_lists_count-2;
  short bc_group;
  if((*requestType(m) == CREATE) || (*requestType(m) == DELETE)) {
    /* Special case: Send to all replicas...bc_group one should contain all replicas... */
    bc_group = 1;
  } else {
    /* Need to add one so that no requests are assigned to the coordinator (bc:0) */
    bc_group = hash(getKeyAddr(m), getKeyLength(m), table_length) + 2;  
  }

  printf("request %d bc group: %d\n", *requestType(m), bc_group);

  return bc_group;

}

state_name do_work(void * dfaSet, StateMachine * stateMachine, Message * m, char * from);




int xid_exists(int ht_xid, jbHashTable_t * xid_ht, StateMachine * stateMachine) {
  int xid;
  if (-1 == jbHtLookup(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid)) {
      return 0;
  } else {
      assert(xid);
      return xid;
  }
}

DfaSet * cHtInit(int cht_type, char * localhost,
		 short (* get_broadcast_group)(DfaSet *, Message *),
		 short port,
		 char *** broadcast_lists,
		 int  broadcast_lists_count,
		 int* broadcast_list_host_count) {
  
  DfaSet * dfaSet;
  int xid = Tbegin();
  TwoPCAppState * twoPC_state; 
  CHTAppState * chtApp_state;
  
  int error; 

  dfaSet = dfa_malloc(DFA_MACHINE_COUNT, port, broadcast_lists, broadcast_lists_count, broadcast_list_host_count);

  /* srand(time(NULL)); */

  twoPC_state  = calloc(1, sizeof(TwoPCAppState));
  chtApp_state = calloc(1, sizeof(CHTAppState));

  if(cht_type == CHT_CLIENT) {
    error = dfa_reinitialize(dfaSet, localhost, client_transitions_2pc, client_transition_count_2pc, states_2pc, state_count_2pc);
  } else {
    error = dfa_reinitialize(dfaSet, localhost, transitions_2pc, transition_count_2pc, states_2pc, state_count_2pc);
  }

  if(error < 0) { 
    perror("dfa_reinitialize failed");
    return NULL;
  }

  if(cht_type != CHT_CLIENT) {
    chtApp_state->xid_ht     = jbHtCreate(xid, 79);
    chtApp_state->ht_ht      = jbHtCreate(xid, 79);
    chtApp_state->ht_xid = Tbegin(); // !!!!
    chtApp_state->next_hashTableId = 0; /* This gets incremented each time a new hashtable is allocated. */

    twoPC_state->is_coordinator = (cht_type == CHT_COORDINATOR);
    twoPC_state->init_xact_2pc = init_xact_cht;
    twoPC_state->veto_or_prepare_2pc = veto_or_prepare_cht;
    twoPC_state->abort_2pc = abort_cht;
    twoPC_state->commit_2pc = commit_cht;
    twoPC_state->tally_2pc = tally_cht;
    if(get_broadcast_group == NULL) get_broadcast_group = multiplex_interleaved;
    twoPC_state->get_broadcast_group = get_broadcast_group;
    twoPC_state->app_state_record_id = Talloc(xid, sizeof(CHTAppState));
    twoPC_state->app_state = chtApp_state;
    
    Tset(xid, twoPC_state->app_state_record_id, chtApp_state);
  
    dfaSet->app_setup = twoPC_state;
  }

  Tcommit(xid);

  return dfaSet;
}
