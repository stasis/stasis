#include "cht.h"
#include "cht_message.h"
#include "../../2pc/2pc.h"
#include <netinet/in.h>
#include <assert.h>
#include <string.h>


#define REQUEST_TYPE XACT_ACK_RESULT
//#define REQUEST_TYPE AWAIT_COMMIT_POINT
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
static int _chtEval(DfaSet * dfaSet, 
	     unsigned char request_type, 
	     unsigned char response_type,
	     state_machine_id * xid,
	     clusterHashTable_t * ht, 
	     void * key,   size_t * key_size,
	     void * value, size_t * value_size) {
  
  /* Fill out a message payload. */

  Message m;
  
  if(ht != NULL) {
    DEBUG("_chtEval(request=%d, response=%d, xid=%ld, ht=%d ", request_type, response_type, *xid, ht->id);
  } else {
    DEBUG("_chtEval(request=%d, response=%d, xid=%ld, ht=NULL ", request_type, response_type, *xid);
  }
  if(key == NULL) {
    DEBUG(")\n");
  } else {
    DEBUG("key=%d)\n", *(int*)key);
  }
  * requestType(&m) = request_type;
  m.response_type = response_type;

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
  } else { 

    //memset(&(__header_ptr(&m)->hashTable), 0, sizeof(clusterHashTable_t));
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
  *value_size = getValLength(&m);
  *key_size = getKeyLength(&m);
  *xid = m.to_machine_id;

  DEBUG("+chtEval returning %d\n", m.type);

  //  return m.type;
  return 1;
}

int cHtCreate(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * new_ht) {
  size_t zero = 0;
  return _chtEval(dfaSet, CREATE, /*AWAIT_COMMIT_POINT,*/ REQUEST_TYPE, &xid, new_ht, NULL, &zero, NULL, &zero) != SUBORDINATE_VETO_2PC;
}


int cHtInsert(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t datlen) {
  return _chtEval(dfaSet, INSERT, /*AWAIT_COMMIT_POINT,*/ REQUEST_TYPE, &xid, ht, key, &keylen, dat, &datlen) != SUBORDINATE_VETO_2PC;
}

int cHtLookup(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t * datlen) {
  assert(keylen != 0);
  int ret = _chtEval(dfaSet, LOOKUP, /*AWAIT_COMMIT_POINT,*/ REQUEST_TYPE, &xid, ht, key, &keylen, dat, datlen) != SUBORDINATE_VETO_2PC;
  assert(ret);
  return (*datlen != 0);
}

int cHtRemove(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t * datlen) {
  int ret = _chtEval(dfaSet, REMOVE, /*AWAIT_COMMIT_POINT,*/ REQUEST_TYPE, &xid, ht, key, &keylen, dat, datlen) != SUBORDINATE_VETO_2PC;
  if(!ret) { return -1; }
  return *datlen;
}

int cHtDelete(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t *ht) {
  size_t zero = 0;
  return _chtEval(dfaSet, DELETE, /*AWAIT_COMMIT_POINT,*/ REQUEST_TYPE, &xid, ht, NULL, &zero, NULL, &zero) != SUBORDINATE_VETO_2PC;
}

int cHtGetXid(state_machine_id* xid, DfaSet * dfaSet) {
  size_t zero = 0;
  *xid = NULL_MACHINE;    /* Will be overwritten by
				      _chtEval... Need a large random
				      value so that the request will
				      be serviced exactly once, but
				      will not conflict with real
				      transactions or other begins.*/
  return _chtEval(dfaSet, GETXID, /*AWAIT_ARRIVAL,*/ REQUEST_TYPE, xid, NULL, NULL, &zero, NULL, &zero) != SUBORDINATE_VETO_2PC;
}

DfaSet * cHtClientInit(char * configFile) {
  NetworkSetup * config = readNetworkConfig(configFile, 0);
  assert(config->coordinator);

  DfaSet * ret = cHtInit(CHT_CLIENT, NULL, config);
  assert(config->coordinator);
  free (config);
  return ret;
}

int cHtCommit(state_machine_id xid, DfaSet * dfaSet) {
  size_t zero = 0;
  return _chtEval(dfaSet, COMMIT, XACT_COMMIT, &xid, NULL, NULL, &zero, NULL, &zero);
}


int cHtAbort(state_machine_id xid, DfaSet * dfaSet) {
  size_t zero = 0;
  return _chtEval(dfaSet,  ABORT, XACT_COMMIT, &xid, NULL, NULL, &zero, NULL, &zero);
  abort();
}
