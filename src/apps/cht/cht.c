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
#define CREATE 1
#define INSERT 2
#define LOOKUP 3
#define REMOVE 4
#define DELETE 5
/** Unimplemented: Evaluate a function call from a table provided by the library user. */
#define TSTSET 6
#define GETXID  7
/* #define COMMIT 8
   #define ABORT  9 */

typedef struct {
  unsigned short key_length;
  unsigned short value_length;
  unsigned char  request_type;
  unsigned char  response_type;
  clusterHashTable_t hashTable;
} payload_header;

typedef struct {
  int ht_xid;
  jbHashTable_t * xid_ht;
  jbHashTable_t * ht_ht;
  int next_hashTableId;
} CHTAppState;



#define __header_ptr(m) ((payload_header*)(&((m)->payload)))

static unsigned short* _key_length(Message * m) {
  return &(__header_ptr(m)->key_length);
}

static unsigned short* _value_length(Message *m) {
  return &(__header_ptr(m)->value_length);
}

#define getKeyLength(m)    (ntohs(*_key_length(m)))
#define setKeyLength(m, x) (*_key_length(m)=htons(x))


#define getValLength(m)    (ntohs(*_value_length(m)))
#define setValLength(m, x) (*_value_length(m)=htons(x))
  
static unsigned char * requestType(Message *m) {
  return &(__header_ptr(m)->request_type);
}

static unsigned char * responseType(Message *m) {
  return &(__header_ptr(m)->response_type);
}

/** TODO: Endianness. (ICK) */
//state_name request_type =*(requestType(m));                                                 \sdsd
//state_name response_type =*(responseType(m));                                               \sd
//TwoPCMachineState * machine_state_2pc = (TwoPCMachineState*) &(stateMachine->app_state);    \sd

#define setup_vars                                                                          \
TwoPCAppState * app_state_2pc = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));           \
CHTAppState * app_state_cht = app_state_2pc->app_state;                                     \
jbHashTable_t * xid_ht = app_state_cht->xid_ht;                                             \
jbHashTable_t * ht_ht = app_state_cht->ht_ht;                                               \
int ht_xid = app_state_cht->ht_xid;                                                         \
int xid;                                                                                    \
int xid_exists = (-1 != jbHtLookup(ht_xid, xid_ht,(byte*) &(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid)); \
jbHashTable_t ht;                                                                                    \
int ht_exists = (-1 != jbHtLookup(ht_xid, ht_ht, (byte*)&(__header_ptr(m)->hashTable), sizeof(clusterHashTable_t),(byte*) &ht))




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

void * getKeyAddr(Message *m) {
  char * stuff = m->payload;

  return (stuff + sizeof(payload_header)); /* Just add the header length. */

}

void * getValAddr(Message * m) {
  return ((char*)getKeyAddr(m)) + getKeyLength(m);     /* key address + key length. */
}

/**

   @return 1 if the payload is valid (key_length and value length do not over-run the message's memory, 0 otherwise.)

*/
int checkPayload(Message * m) {
  char * a = (char*)m;
  char * b = getValAddr(m);
  return (a+ sizeof(Message) ) >= (b + getValLength(m));
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

/**
   The client side function that 'does everything'  

   @param request_type  The type of request to be run.

   @param response_type The expected response type.  Returns 1 if this
                        remote state is returned, 0 otherwise.  (TODO:
                        Double check this documentation.)

   @param xid The (stateMachine) transaction id.  Set to a random
              number when calling BEGIN.  To prevent deadlock, it's
              best to choose a number unlikely to correspond to an
              active transaction.  (A random number within 2^32 of the
              highest 64-bit integer will work.)

   @param reply_type When should the local call return?  The choices
                     are AWAIT_ARRIVAL, which returns after hearing
                     back from the coordinator, AWAIT_COMMIT_POINT to
                     wait until after the transaction commits/aborts,
                     AWAIT_RESULT, which waits for the actual result
                     from one of the replicas.

   @param key, key_size, value, value_size depend on the value of request_type.

   @return 1 on success, 0 on failure.
*/

int _chtEval(DfaSet * dfaSet, 
	     unsigned char request_type, 
	     unsigned char response_type,
	     state_machine_id * xid,
	     clusterHashTable_t * ht, 
	     void * key,   size_t * key_size,
	     void * value, size_t * value_size) {
  
  /* Fill out a message payload. */

  Message m;
  
  if(ht != NULL) {
    printf("_chtEval(request=%d, response=%d, xid=%ld, ht=%d ", request_type, response_type, *xid, ht->id);
  } else {
    printf("_chtEval(request=%d, response=%d, xid=%ld, ht=NULL ", request_type, response_type, *xid);
  }
  if(key == NULL) {
    printf(")\n");
  } else {
    printf("key=%d)\n", *(int*)key);
  }
  * requestType(&m) = request_type;
  * responseType(&m) = response_type;

  setKeyLength(&m, *key_size);
  setValLength(&m, *value_size);

  assert(checkPayload(&m));
  if(key_size != 0) {
    memcpy(getKeyAddr(&m), key, *key_size);
  } 
  if(value_size != 0) {
    memcpy(getValAddr(&m), value, *value_size);
  }
  if(ht != NULL) {
    memcpy(&(__header_ptr(&m)->hashTable), ht, sizeof(clusterHashTable_t));
  }

  /*  printf("%s <- %s\n", __header_ptr(&m)->initiator, dfaSet->networkSetup.localhost); */

  /* Synchronously run the request */
  request(dfaSet, response_type, "bc:0", *xid, &m);

  if(!checkPayload(&m)) {
    printf("_chtEval failed: Invalid response.\n");
    assert(0);
  } 

  /* Copy message contents back into caller's buffers, even if the
     request failed.  (There may be app-specific information in the
     response...) */

  if(ht != NULL) {
    memcpy(ht, &(__header_ptr(&m)->hashTable), sizeof(clusterHashTable_t));
  }
  if (*key_size != 0) {
    /*    printf("\n+%x<-%x+, length %d value=%s and %s\n", (unsigned int) value, (unsigned int)getValAddr(&m), getValLength(&m), value, getValAddr(&m)); */
    memcpy(value, getValAddr(&m), getValLength(&m));
  }
  if (*value_size != 0) {
    memcpy(key, getKeyAddr(&m), getKeyLength(&m));
  }

  *xid = m.to_machine_id;

  printf("+chtEval returning %d\n", m.type);

  return m.type;
}


state_name init_xact_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {


  /*  TwoPCMachineState * state = (TwoPCMachineState*) &(stateMachine->app_state);*/
  TwoPCAppState * app_state_2pc = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));           
  CHTAppState * app_state_cht = app_state_2pc->app_state;                                     

  if(m->type != *responseType(m)) {
    printf("Bug in client!  m->type != response_type(m).\n");
  }

  if(*requestType(m) == CREATE) {
    clusterHashTable_t new_cht;
    new_cht.id = app_state_cht->next_hashTableId;
    app_state_cht->next_hashTableId++;

    memcpy(&(__header_ptr(m)->hashTable), &new_cht, sizeof(clusterHashTable_t));

    printf("Allocated hashtable %d\n", new_cht.id);
  }

  printf("requestType: %d, responseType: %d key: %d from %s:%ld\n", *requestType(m), *responseType(m), *(int*)getKeyAddr(m), m->initiator, m->initiator_machine_id);

  if(*responseType(m) == AWAIT_ARRIVAL) {
    state_machine_id tmp = m->from_machine_id;

    /* TODO:  Could the chages to from_machine_id be moved into libdfa (it does this anyway, but it does it too late.) */
    m->from_machine_id = m->initiator_machine_id; /*stateMachine->machine_id;*/

    printf("Responding\n");
    
    respond_once(&((DfaSet*)dfaSet)->networkSetup, COORDINATOR_START_2PC, m, m->initiator);

    m->from_machine_id = tmp;
  }

  return 1;
}

int xid_exists(int ht_xid, jbHashTable_t * xid_ht, StateMachine * stateMachine) {
  int xid;
  if (-1 == jbHtLookup(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid)) {
      return 0;
  } else {
      assert(xid);
      return xid;
  }
}

callback_fcn abort_cht;

/* Begins new transaction, does the work the transaction requests, stores the message, and returns the corresponding error code. */
state_name veto_or_prepare_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {

  state_name ret;
  setup_vars;

//  int xid_exists = (-1 != jbHtLookup(ht_xid, app_state_cht->xid_ht, &(stateMachine->machine_id), sizeof(state_machine_id, &xid));
  
  if(xid_exists) { printf("Warning:  Stale xid found!\n"); }
  assert(!xid_exists);
  printf("requestType: %d, responseType: %d key: %d from %s:%ld\n", *requestType(m), *responseType(m), *(int*)getKeyAddr(m), m->initiator, m->initiator_machine_id);

  /* This is the start of a new transaction */
  xid = Tbegin();  // !!!!
  if(xid < 0) {

    printf("Tbegin failed; %d\n", xid);

  } else if(jbHtInsert(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid, sizeof(int)) == -1) {

    printf("jbHtInsert failed.\n");

  } else {

	xid_exists = 1;
  }
  Tcommit(app_state_cht->ht_xid);
  app_state_cht->ht_xid = Tbegin();
  
  if(xid_exists) {
    
    ret = do_work(dfaSet, stateMachine, m, from);
    
    ret = ret ? SUBORDINATE_PREPARED_2PC : SUBORDINATE_VETO_2PC;

  } else {

    ret = SUBORDINATE_VETO_2PC;

  }

  if(ret == SUBORDINATE_VETO_2PC) {
    abort_cht(dfaSet, stateMachine, m, from);
  }
  return ret;
}

state_name abort_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  setup_vars;

  printf("Aborting!!\n");

  if(*responseType(m) == AWAIT_COMMIT_POINT || *responseType(m) == AWAIT_RESULT) {
    state_machine_id tmp = m->from_machine_id;

    /* TODO:  Could the chages to from_machine_id be moved into libdfa (it does this anyway, but it does it too late.) */
    m->from_machine_id = m->initiator_machine_id; /*stateMachine->machine_id;*/

    printf("Response being sent to: %s:%ld\n", m->initiator, m->to_machine_id);
    respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_VETO_2PC, m, m->initiator);
    m->from_machine_id = tmp;
  }

  assert(xid_exists);

  Tabort(xid); // !!!!
  jbHtRemove(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid);
  Tcommit(app_state_cht->ht_xid);
  app_state_cht->ht_xid = Tbegin();
  return 1;
}

/** TODO For now, we ignore the possiblity that jbHashTable's functions
    return error codes.  Instead, we assume that they always
    succeed. */
state_name do_work(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  int ret;
  setup_vars;
  
  /*  printf("ht_ht = %x, ht = %x\n", ht_ht, ht); */

  switch(*requestType(m)) 
    {  
    case CREATE: 
      {
	jbHashTable_t * new = jbHtCreate(ht_xid, 79);
	if(new != NULL) {
	  ret = (jbHtInsert(ht_xid, ht_ht, (byte*)&(__header_ptr(m)->hashTable), sizeof(clusterHashTable_t), (byte*)new, sizeof(jbHashTable_t)) >= 0);
	} else {
	  ret = 0;
	}
	if(ret) {
	  printf("Created local slice of global hash table %d\n", (__header_ptr(m)->hashTable).id);
	  Tcommit(app_state_cht->ht_xid);
	  app_state_cht->ht_xid = Tbegin();	  
	} else {
	  printf("Failed to insert new hash table slice!");
	}

      } break;
      
    case INSERT: 
      {
	if(!ht_exists) { printf ("Hash table %d doesn't exist!\n", (__header_ptr(m)->hashTable).id); fflush(NULL); ret = 0; } else {
	  ret = (jbHtInsert(xid, &ht, getKeyAddr(m), getKeyLength(m), getValAddr(m), getValLength(m)) >= 0);
	  printf("Insert: %d ht=%d (key length %d) %d -> %s\n", ret, (__header_ptr(m)->hashTable).id, getKeyLength(m), *(int*)getKeyAddr(m), getValAddr(m));
	  (jbHtInsert(ht_xid, ht_ht, (byte*)&(__header_ptr(m)->hashTable), sizeof(clusterHashTable_t), (byte*)&ht, sizeof(jbHashTable_t)));
	  
	}
      } break;
      
    case LOOKUP:
      {
	if(!ht_exists) { printf ("Hash table doesn't exist!\n"); fflush(NULL); ret = 0; } else {
	  ret = (jbHtLookup(xid, &ht, getKeyAddr(m), getKeyLength(m), getValAddr(m)) >= 0);
	  printf("Lookup: %d ht=%d (key length %d) %d -> %s\n", ret, (__header_ptr(m)->hashTable).id, getKeyLength(m), *(int*)getKeyAddr(m), getValAddr(m));
	} 
      } break;
      
    case REMOVE:
      {
	if(!ht_exists) { printf ("Hash table doesn't exist!\n"); fflush(NULL); ret = 0; } else {
	  ret = (jbHtRemove(xid, &ht, getKeyAddr(m), getKeyLength(m), getValAddr(m)) >= 0);
	  (jbHtInsert(ht_xid, ht_ht, (byte*)&(__header_ptr(m)->hashTable), sizeof(clusterHashTable_t), (byte*)&ht, sizeof(jbHashTable_t)));
	}
      } break;
      
    case DELETE: 
      {
	if(!ht_exists) { printf ("Hash table doesn't exist!\n"); fflush(NULL); ret = 0; } else {
	  jbHtRemove(xid, ht_ht, getKeyAddr(m), getKeyLength(m), NULL);
	  (jbHtInsert(ht_xid, ht_ht, (byte*)&(__header_ptr(m)->hashTable), sizeof(clusterHashTable_t), (byte*)&ht, sizeof(jbHashTable_t)));
	  /*	  ret = (jbHtDelete(xid, &ht) >= 0); */ /* Don't need this--jbHtDelete just frees the (stack!) pointer. */
	  Tcommit(app_state_cht->ht_xid);
	  app_state_cht->ht_xid = Tbegin();	  

	}
      } break;
      
    case TSTSET: 
      {
	printf("Unimplemented request!\n");
      } break;
    
    case GETXID: 
      {
	/*	int new_xid = Tbegin();
	if(new_xid >= 0) {
	  setKeyLength(m, 0);
	  setValLength(m, sizeof(int));
	  *((int*)getValAddr(m)) = new_xid;
	  ret = 1;
	  if(jbHtInsert(ht_xid, xid_ht, &(stateMachine->machine_id), sizeof(state_machine_id), &xid, sizeof(int)) == -1) {
	    printf("Begin failed on jbHtInsert!\n");
	  } else {
	    printf("Created local xid for global xid: %ld\n", stateMachine->machine_id);
	  }
	} else {
	  printf("Begin failed on Tbegin()!\n");

	  ret = 0;
	  }  NOOP */
      } break;
      
      /*    case COMMIT: 
      {
	ret = (Tcommit(xid) >= 0);
      } break;
      
    case ABORT: 
      {
	ret = (Tabort(xid) >= 0);
      } break;
      */
    default: 
      {
	printf("Unknown request type: %d\n", *requestType(m));
      }
    }
  
  return ret;
}

state_name commit_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  
  setup_vars;
  
  assert(xid_exists);
  Tcommit(xid);
  jbHtRemove(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid);
  Tcommit(app_state_cht->ht_xid);
  app_state_cht->ht_xid = Tbegin();
    /* }*/
  
  if(*responseType(m) == AWAIT_RESULT) {
    printf("commit_cht responding on an AWAIT_RESULT request.\n");
    assert(0);
    /*    respond_once(&((DfaSet*)dfaSet)->networkSetup, SUBORDINATE_ACKING_2PC, m, __header_ptr(m)->initiator); */
  }
  /* TODO: Check error codes, and return accordingly... */
  return 1;
}

state_name tally_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  
 // setup_vars;

  /* TODO: Make sure this is after tally forces the log. Also, need to
     make sure that it increments the (currently unimplemented)
     sequence number before flushing... */

  if(*responseType(m) == AWAIT_COMMIT_POINT && stateMachine->current_state==COORDINATOR_START_2PC) {
    state_machine_id tmp = m->from_machine_id;
    
    /* TODO:  Could the chages to from_machine_id be moved into libdfa (it does this anyway, but it does it too late.) */
    m->from_machine_id = m->initiator_machine_id; /*stateMachine->machine_id;*/
    
    printf("Coordinator responding: ? ht=? (key length %d) %d ->   to %s:%ld\n",  getKeyLength(m), *(int*)getKeyAddr(m), /*getValAddr(m),*/ m->initiator, m->initiator_machine_id );
    respond_once(&((DfaSet*)dfaSet)->networkSetup, COORDINATOR_COMMITTING_2PC, m, m->initiator);

    m->from_machine_id = tmp;
  }
  
  return 1;
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

int cHtCreate(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * new_ht) {
  size_t zero = 0;
  return _chtEval(dfaSet, CREATE, AWAIT_COMMIT_POINT, &xid, new_ht, NULL, &zero, NULL, &zero) != SUBORDINATE_VETO_2PC;
}


int cHtInsert(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t datlen) {
  return _chtEval(dfaSet, INSERT, AWAIT_COMMIT_POINT, &xid, ht, key, &keylen, dat, &datlen) != SUBORDINATE_VETO_2PC;
}

int cHtLookup(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t * datlen) {
  return _chtEval(dfaSet, LOOKUP, AWAIT_COMMIT_POINT, &xid, ht, key, &keylen, dat, datlen) != SUBORDINATE_VETO_2PC;
}

int cHtRemove(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t * datlen) {
  return _chtEval(dfaSet, REMOVE, AWAIT_COMMIT_POINT, &xid, ht, key, &keylen, dat, datlen) != SUBORDINATE_VETO_2PC;
}

int cHtDelete(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t *ht) {
  size_t zero = 0;
  return _chtEval(dfaSet, DELETE, AWAIT_COMMIT_POINT, &xid, ht, NULL, &zero, NULL, &zero) != SUBORDINATE_VETO_2PC;
}

int cHtGetXid(state_machine_id* xid, DfaSet * dfaSet) {
  size_t zero = 0;
  *xid = NULL_MACHINE;    /* Will be overwritten by
				      _chtEval... Need a large random
				      value so that the request will
				      be serviced exactly once, but
				      will not conflict with real
				      transactions or other begins.*/
 return _chtEval(dfaSet, GETXID, AWAIT_ARRIVAL, xid, NULL, NULL, &zero, NULL, &zero) != SUBORDINATE_VETO_2PC;
}

/*int cHtCommit(state_machine_id xid, DfaSet * dfaSet) {
  size_t zero = 0;
  return _chtEval(dfaSet, COMMIT, AWAIT_COMMIT_POINT, &xid, NULL, NULL, &zero, NULL, &zero);
}


int cHtAbort(state_machine_id xid, DfaSet * dfaSet) {
  size_t zero = 0;
  return _chtEval(dfaSet, ABORT, AWAIT_COMMIT_POINT, &xid, NULL, NULL, &zero, NULL, &zero);
  }*/
