#include "cht.h"
#include "cht_message.h"
#include <assert.h>
#include <string.h>
#include <netinet/in.h>

#define setup_vars                                                                          \
TwoPCAppState * app_state_2pc = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));           \
CHTAppState * app_state_cht = app_state_2pc->app_state;                                     \
recordid xid_ht = app_state_cht->xid_ht;                                             \
int ht_xid = app_state_cht->ht_xid;                                                         

int getXid(int ht_xid, recordid xid_ht, state_machine_id id) {
  byte * xid;
  int ret;
  int size = ThashLookup(ht_xid, xid_ht, (byte*)&id, sizeof(state_machine_id), &xid);
  if(size == sizeof(int)) {
    ret = *(int*)xid;
    free(xid);
  } else {
    assert(size == -1);
    ret = -1;
  }
  return ret;
}


recordid getHashTable (int ht_xid, recordid ht_ht, clusterHashTable_t hashTable) {
  byte * ht;
  recordid ret;
  int size = ThashLookup(ht_xid, ht_ht, (byte*)&hashTable, sizeof(clusterHashTable_t), &ht);
  if(size == sizeof(recordid)) {
    ret = *(recordid*)ht;
    free(ht);
  } else {
    assert(size == -1);
    ret.page =  0;
    ret.slot =  0;
    ret.size = -1;
  }
  return ret;
}


/** TODO For now, we ignore the possiblity that LLADD's functions
    return error codes.  Instead, we assume that they always
    succeed. */
static state_name do_work(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  
  setup_vars;

  recordid ht = getHashTable(ht_xid, app_state_cht->ht_ht, __header_ptr(m)->hashTable);
  int xid = getXid(ht_xid, xid_ht, stateMachine->machine_id);
  int ht_exists = (ht.size != -1);

  int ret = 1;  
  switch(*requestType(m)) 
    {  
    case CREATE: 
      {
	recordid new = ThashCreate(ht_xid, VARIABLE_LENGTH, VARIABLE_LENGTH);

	ThashInsert(ht_xid, app_state_cht->ht_ht, 
		    (byte*)&(__header_ptr(m)->hashTable), sizeof(clusterHashTable_t), 
		    (byte*)&new, sizeof(recordid));

	DEBUG("Created local slice of global hash table %d\n", (__header_ptr(m)->hashTable));
	//Tcommit(app_state_cht->ht_xid);
	//app_state_cht->ht_xid = Tbegin();	  

      } break;
      
    case INSERT: 
      {
	if(!ht_exists) {
	  printf ("Hash table %d doesn't exist!\n", __header_ptr(m)->hashTable.id); fflush(stdout); ret = 0; 
	} else {

	  ThashInsert(xid, ht, getKeyAddr(m), getKeyLength(m), getValAddr(m), getValLength(m));

	  DEBUG("Insert: %d ht=%d (key length %d) %d -> %d\n", ret, 
		 (__header_ptr(m)->hashTable), getKeyLength(m), 
		 *(int*)getKeyAddr(m), *(int*)getValAddr(m));
	  
	}
      } break;
      
    case LOOKUP:
      {
	if(!ht_exists) { 
	  printf ("Hash table doesn't exist!\n"); fflush(stdout); ret = 0; 
	} else {
	  byte * new;
	  int valueLength = ThashLookup(xid, ht, getKeyAddr(m), getKeyLength(m), &new);
	  if(valueLength != -1) {
	    assert(valueLength <= getValLength(m));
	    memcpy(getValAddr(m), new, valueLength);
	    free(new);
	    setValLength(m, valueLength);
	  } else {
	    setValLength(m, 0);
	  }
	  DEBUG("Lookup: %d ht=%d (key length %d) %d -> %d\n", ret, 
		 (__header_ptr(m)->hashTable), getKeyLength(m),
		 *(int*)getKeyAddr(m), *(int*)getValAddr(m));
	} 
      } break;
      
    case REMOVE:
      {
	if(!ht_exists) { 
	  printf ("Hash table doesn't exist!\n"); fflush(stdout); ret = 0; 
	} else {
	  /** @todo we no longer return old value on remove... */
	  int remove_ret = ThashRemove(xid, ht, getKeyAddr(m), getKeyLength(m));
	  if(remove_ret == 0) {
	    setValLength(m, 0);
	  } else {
	    setValLength(m, 1);
	  }
	}
      } break;
      
    case DELETE: 
      {
	if(!ht_exists) { printf ("Hash table doesn't exist!\n"); fflush(stdout); ret = 0; } else {
	  ThashRemove(xid, app_state_cht->ht_ht, getKeyAddr(m), getKeyLength(m));
	  //Tcommit(app_state_cht->ht_xid);
	  //app_state_cht->ht_xid = Tbegin();	  

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
    case COMMIT:  
      {
      // placeholder (2pc commits for us unless there's an error)
      } break;
    case ABORT: 
      { 
	ret = 0; // Insert an 'error' to cause 2pc to abort the transaction.
      } break;
      
    default: 
      {
	printf("Unknown request type: %d\n", *requestType(m));
      }
    }
  
  return ret;
}

/* Run by the coordinator when the request is received from the client. */
state_name init_xact_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {

  TwoPCAppState * app_state_2pc = ((TwoPCAppState*)(((DfaSet*)dfaSet)->app_setup));           
  CHTAppState * app_state_cht = app_state_2pc->app_state;                                     

  if(m->type != m->response_type) {
    printf("Bug in client!  m->type != response_type(m).\n");
  }

  if(*requestType(m) == CREATE) {
    clusterHashTable_t new_cht;
    new_cht.id = app_state_cht->next_hashTableId;
    app_state_cht->next_hashTableId++;

    memcpy(&(__header_ptr(m)->hashTable), &new_cht, sizeof(clusterHashTable_t));

    DEBUG("Allocated hashtable %d\n", new_cht.id);
  }

  //  printf("requestType: %d, responseType: %d from %s:%ld\n", *requestType(m), m->response_type, /**(int*)getKeyAddr(m),*/ m->initiator, m->initiator_machine_id);

  return 1;
}

/* Begins new transaction, does the work the transaction requests, stores the message, and returns the corresponding error code. */
state_name veto_or_prepare_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  state_name ret;
  setup_vars;

  int xid = getXid(ht_xid, app_state_cht->xid_ht, stateMachine->machine_id);
  int xid_exists = (-1 != xid);
  if(stateMachine->current_state != XACT_ACTIVE) {
    if(xid_exists) { printf("Warning:  Stale xid found!\n"); }
    assert(!xid_exists);
    
  //  printf("requestType: %d, responseType: %d from %s:%ld\n", *requestType(m), m->response_type, /**(int*)getKeyAddr(m),*/ m->initiator, m->initiator_machine_id);
    
  /* This is the start of a new transaction */
    xid = Tbegin();
    if(xid < 0) {
      printf("Tbegin failed; %d\n", xid);
    } else {
      ThashInsert(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid, sizeof(int));
      xid_exists = 1;
    }
  }
  
  ret = do_work(dfaSet, stateMachine, m, from);
  
  ret = ret ? SUBORDINATE_PREPARED_2PC : SUBORDINATE_VETO_2PC;
  
  if(ret == SUBORDINATE_VETO_2PC) {
    abort_cht(dfaSet, stateMachine, m, from);
  }
  return ret;
}
state_name eval_action_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  state_name ret = -1;
  setup_vars;

  int xid = getXid(ht_xid, app_state_cht->xid_ht, stateMachine->machine_id);
  int xid_exists = (-1 != xid);
  if(xid_exists) { 
    assert(stateMachine->current_state == XACT_ACTIVE); 
  } else {

    /* This is the start of a new transaction */
    xid = Tbegin();
    if(xid < 0) {
      printf("Tbegin failed; %d\n", xid);
      abort();
    } else {
      ThashInsert(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id), (byte*)&xid, sizeof(int));
      xid_exists = 1;
    }
    
  }

  //  printf("requestType: %d, responseType: %d from %s:%ld\n", *requestType(m), m->response_type, /**(int*)getKeyAddr(m),*/ m->initiator, m->initiator_machine_id);
  
  if(xid_exists) {
    
    ret = do_work(dfaSet, stateMachine, m, from);
    
    assert(ret);  // for now! 
 
  }

  // if(ret == SUBORDINATE_VETO_2PC) {
  //  abort_cht(dfaSet, stateMachine, m, from);
  // }
  return ret;
}

state_name abort_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  setup_vars;

  int xid = getXid(ht_xid, app_state_cht->xid_ht, stateMachine->machine_id);
  int xid_exists = (-1 != xid);

  printf("Aborting!!\n");

  assert(xid_exists);

  Tabort(xid); // !!!!

  ThashRemove(ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id));
  //  Tcommit(app_state_cht->ht_xid);
  //  app_state_cht->ht_xid = Tbegin();
  return 1;
}


state_name commit_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  
  setup_vars;

  int xid = getXid(ht_xid, app_state_cht->xid_ht, stateMachine->machine_id);
  int xid_exists = (-1 != xid);

  assert(xid_exists);
  Tcommit(xid);

  ThashRemove(app_state_cht->ht_xid, xid_ht, (byte*)&(stateMachine->machine_id), sizeof(state_machine_id));
  Tcommit(app_state_cht->ht_xid);
  app_state_cht->ht_xid = Tbegin();

  /* TODO: Check error codes, and return accordingly... */
  return 1;
}

state_name tally_cht(void * dfaSet, StateMachine * stateMachine, Message * m, char * from) {
  return 1;
}

DfaSet * cHtCoordinatorInit(char * configFile, short (*partition_function)(DfaSet *, Message *)) {
  NetworkSetup * config = readNetworkConfig(configFile, COORDINATOR);
  DfaSet * ret = cHtInit(CHT_COORDINATOR, partition_function, config);
  free(config);
  return ret;
}

DfaSet * cHtSubordinateInit(char * configFile, short (*partition_function)(DfaSet *, Message *), int subordinate_number) {
  NetworkSetup * config = readNetworkConfig(configFile, subordinate_number);
  DfaSet * ret = cHtInit(CHT_SERVER, partition_function, config);
  free (config);
  return ret;
}
void debug_print_message(Message * m) {
  printf("debug: (key length %d) %d -> %d\n", getKeyLength(m), *(int*)getKeyAddr(m), *(int*)getValAddr(m));
}
