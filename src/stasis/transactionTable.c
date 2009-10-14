/*
 * transactionTable.c
 *
 *  Created on: Oct 14, 2009
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/constants.h>
#include <stasis/transactionTable.h>
#include <assert.h>

TransactionLog stasis_transaction_table[MAX_TRANSACTIONS];
static int stasis_transaction_table_active_count = 0;
static int stasis_transaction_table_xid_count = 0;

int stasis_transaction_table_num_active() {
  return stasis_transaction_table_active_count;
}

/**
    This mutex protects stasis_transaction_table, numActiveXactions and
    xidCount.
*/
static pthread_mutex_t stasis_transaction_table_mutex;

typedef enum {
  INVALID_XTABLE_XID = INVALID_XID,
  PENDING_XTABLE_XID = -2
} stasis_transaction_table_status;

void stasis_transaction_table_init() {
  pthread_mutex_init(&stasis_transaction_table_mutex, NULL);
  stasis_transaction_table_active_count = 0;

  memset(stasis_transaction_table, INVALID_XTABLE_XID,
     sizeof(TransactionLog)*MAX_TRANSACTIONS);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_init(&stasis_transaction_table[i].mut,0);
  }
}

void stasis_transaction_table_deinit() {
  pthread_mutex_destroy(&stasis_transaction_table_mutex);
  stasis_transaction_table_active_count = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_destroy(&stasis_transaction_table[i].mut);
  }
  memset(stasis_transaction_table, INVALID_XTABLE_XID, sizeof(TransactionLog)*MAX_TRANSACTIONS);
}

void stasis_transaction_table_max_transaction_id_set(int xid) {
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  stasis_transaction_table_xid_count = xid;
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
}
void stasis_transaction_table_active_transaction_count_set(int xid) {
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  stasis_transaction_table_active_count = xid;
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
}

lsn_t stasis_transaction_table_minRecLSN() {
  lsn_t minRecLSN = LSN_T_MAX;
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(stasis_transaction_table[i].xid != INVALID_XTABLE_XID) {
      lsn_t recLSN = stasis_transaction_table[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) {
        minRecLSN = recLSN;
      }
    }
  }
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return minRecLSN;
}


int TactiveTransactionCount() {
  return stasis_transaction_table_active_count;
}

int* stasis_transaction_table_list_active() {
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  int * ret = malloc(sizeof(*ret));
  ret[0] = 0;
  int retcount = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(stasis_transaction_table[i].xid != INVALID_XTABLE_XID) {
      ret[retcount] = stasis_transaction_table[i].xid;
      retcount++;
      ret = realloc(ret, (retcount+1) * sizeof(*ret));
      ret[retcount] = 0;
    }
  }
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return ret;
}
int* TlistActiveTransactions() {
  return stasis_transaction_table_list_active();
}
int TisActiveTransaction(int xid) {
  if(xid < 0) { return 0; }
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  int ret = xid != INVALID_XTABLE_XID && stasis_transaction_table[xid%MAX_TRANSACTIONS].xid == xid;
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return ret;
}

int stasis_transaction_table_roll_forward(int xid, lsn_t lsn, lsn_t prevLSN) {
  TransactionLog * l = &stasis_transaction_table[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    // rolling forward CLRs / NTAs makes prevLSN decrease.
    assert(l->prevLSN >= prevLSN);
  } else {
    pthread_mutex_lock(&stasis_transaction_table_mutex);
    assert(l->xid == INVALID_XTABLE_XID);
    l->xid = xid;
    l->recLSN = lsn;
    stasis_transaction_table_active_count++;
    pthread_mutex_unlock(&stasis_transaction_table_mutex);
  }
  l->prevLSN = lsn;
  return 0;
}
int stasis_transaction_table_roll_forward_with_reclsn(int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN) {
  assert(stasis_transaction_table[xid%MAX_TRANSACTIONS].recLSN == recLSN);
  return stasis_transaction_table_roll_forward(xid, lsn, prevLSN);
}
TransactionLog * stasis_transaction_table_begin(int * xid) {
  int index = 0;
  int xidCount_tmp;

  pthread_mutex_lock(&stasis_transaction_table_mutex);

  if( stasis_transaction_table_active_count == MAX_TRANSACTIONS ) {
    pthread_mutex_unlock(&stasis_transaction_table_mutex);
    *xid = LLADD_EXCEED_MAX_TRANSACTIONS;
    return 0;
  } else {
    DEBUG("%s:%d activate in begin\n",__FILE__,__LINE__);
    stasis_transaction_table_active_count++;
  }
  for(int i = 0; i < MAX_TRANSACTIONS; i++ ) {
    stasis_transaction_table_xid_count++;
    if( stasis_transaction_table[stasis_transaction_table_xid_count%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
      index = stasis_transaction_table_xid_count%MAX_TRANSACTIONS;
      break;
    }
  }

  xidCount_tmp = stasis_transaction_table_xid_count;

  stasis_transaction_table[index].xid = PENDING_XTABLE_XID;

  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  *xid = xidCount_tmp;
  return &stasis_transaction_table[index];
}
TransactionLog * stasis_transaction_table_get(int xid) {
  return &stasis_transaction_table[xid % MAX_TRANSACTIONS];
}
int stasis_transaction_table_commit(int xid) {
  pthread_mutex_lock(&stasis_transaction_table_mutex);

  stasis_transaction_table[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  DEBUG("%s:%d deactivate %d\n",__FILE__,__LINE__,xid);
  stasis_transaction_table_active_count--;
  assert( stasis_transaction_table_active_count >= 0 );
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return 0;
}
int stasis_transaction_table_forget(int xid) {
  assert(xid != INVALID_XTABLE_XID);
  TransactionLog * l = &stasis_transaction_table[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    pthread_mutex_lock(&stasis_transaction_table_mutex);
    l->xid = INVALID_XTABLE_XID;
    l->prevLSN = -1;
    l->recLSN = -1;
    stasis_transaction_table_active_count--;
    assert(stasis_transaction_table_active_count >= 0);
    pthread_mutex_unlock(&stasis_transaction_table_mutex);
  } else {
    assert(l->xid == INVALID_XTABLE_XID);
  }
  return 0;
}
