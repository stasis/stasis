/*
 * transactionTable.c
 *
 *  Created on: Oct 14, 2009
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/constants.h>
#include <stasis/transactionTable.h>
#include <stasis/transactional.h>
#include <assert.h>

struct stasis_transaction_table_t {
  int active_count;
  int xid_count;
  /**
      This mutex protects the rest of the struct
      xidCount.
  */
  pthread_mutex_t mut;
  stasis_transaction_table_entry_t table[MAX_TRANSACTIONS];
};

typedef enum {
  INVALID_XTABLE_XID = INVALID_XID,
  PENDING_XTABLE_XID = -2
} stasis_transaction_table_status;

int stasis_transaction_table_num_active(stasis_transaction_table_t *tbl) {
  return tbl->active_count;
}
int stasis_transaction_table_is_active(stasis_transaction_table_t *tbl, int xid) {
  return xid >= 0 && tbl->table[xid % MAX_TRANSACTIONS].xid == xid;
}

stasis_transaction_table_t *  stasis_transaction_table_init() {
  stasis_transaction_table_t * tbl = malloc(sizeof(*tbl));
  pthread_mutex_init(&tbl->mut, NULL);
  tbl->active_count = 0;

  memset(tbl->table, INVALID_XTABLE_XID,
     sizeof(stasis_transaction_table_entry_t)*MAX_TRANSACTIONS);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_init(&tbl->table[i].mut,0);
  }
  return tbl;
}

void stasis_transaction_table_deinit(stasis_transaction_table_t *tbl) {
  pthread_mutex_destroy(&tbl->mut);
  tbl->active_count = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_destroy(&tbl->table[i].mut);
  }
  free(tbl);
}

void stasis_transaction_table_max_transaction_id_set(stasis_transaction_table_t *tbl, int xid) {
  pthread_mutex_lock(&tbl->mut);
  tbl->xid_count = xid;
  pthread_mutex_unlock(&tbl->mut);
}
void stasis_transaction_table_active_transaction_count_set(stasis_transaction_table_t *tbl, int xid) {
  pthread_mutex_lock(&tbl->mut);
  tbl->active_count = xid;
  pthread_mutex_unlock(&tbl->mut);
}

lsn_t stasis_transaction_table_minRecLSN(stasis_transaction_table_t *tbl) {
  lsn_t minRecLSN = LSN_T_MAX;
  pthread_mutex_lock(&tbl->mut);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(tbl->table[i].xid != INVALID_XTABLE_XID) {
      lsn_t recLSN = tbl->table[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) {
        minRecLSN = recLSN;
      }
    }
  }
  pthread_mutex_unlock(&tbl->mut);
  return minRecLSN;
}


int TactiveTransactionCount(stasis_transaction_table_t *tbl) {
  return tbl->active_count;
}

int* stasis_transaction_table_list_active(stasis_transaction_table_t *tbl) {
  pthread_mutex_lock(&tbl->mut);
  int * ret = malloc(sizeof(*ret));
  ret[0] = 0;
  int retcount = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(tbl->table[i].xid != INVALID_XTABLE_XID) {
      ret[retcount] = tbl->table[i].xid;
      retcount++;
      ret = realloc(ret, (retcount+1) * sizeof(*ret));
      ret[retcount] = 0;
    }
  }
  pthread_mutex_unlock(&tbl->mut);
  return ret;
}
int* TlistActiveTransactions() {
  return stasis_transaction_table_list_active(stasis_runtime_transaction_table());
}
int TisActiveTransaction(int xid) {
  return stasis_transaction_table_is_active(stasis_runtime_transaction_table(), xid);
}

int stasis_transaction_table_roll_forward(stasis_transaction_table_t *tbl, int xid, lsn_t lsn, lsn_t prevLSN) {
  stasis_transaction_table_entry_t * l = &tbl->table[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    // rolling forward CLRs / NTAs makes prevLSN decrease.
    assert(l->prevLSN >= prevLSN);
  } else {
    pthread_mutex_lock(&tbl->mut);
    assert(l->xid == INVALID_XTABLE_XID);
    l->xid = xid;
    l->recLSN = lsn;
    tbl->active_count++;
    pthread_mutex_unlock(&tbl->mut);
  }
  l->prevLSN = lsn;
  return 0;
}
int stasis_transaction_table_roll_forward_with_reclsn(stasis_transaction_table_t *tbl, int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN) {
  assert(tbl->table[xid%MAX_TRANSACTIONS].recLSN == recLSN);
  return stasis_transaction_table_roll_forward(tbl, xid, lsn, prevLSN);
}

stasis_transaction_table_entry_t * stasis_transaction_table_begin(stasis_transaction_table_t *tbl, int * xid) {
  int index = 0;
  int xidCount_tmp;

  pthread_mutex_lock(&tbl->mut);

  if( tbl->active_count == MAX_TRANSACTIONS ) {
    pthread_mutex_unlock(&tbl->mut);
    *xid = LLADD_EXCEED_MAX_TRANSACTIONS;
    return 0;
  } else {
    DEBUG("%s:%d activate in begin\n",__FILE__,__LINE__);
    tbl->active_count++;
  }
  for(int i = 0; i < MAX_TRANSACTIONS; i++ ) {
    tbl->xid_count++;
    if( tbl->table[tbl->xid_count%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
      index = tbl->xid_count%MAX_TRANSACTIONS;
      break;
    }
  }

  xidCount_tmp = tbl->xid_count;

  tbl->table[index].xid = PENDING_XTABLE_XID;

  pthread_mutex_unlock(&tbl->mut);
  *xid = xidCount_tmp;
  return &tbl->table[index];
}
stasis_transaction_table_entry_t * stasis_transaction_table_get(stasis_transaction_table_t *tbl, int xid) {
  if(tbl->table[xid % MAX_TRANSACTIONS].xid == xid) {
    return &tbl->table[xid % MAX_TRANSACTIONS];
  } else {
    return NULL;
  }
}
int stasis_transaction_table_commit(stasis_transaction_table_t *tbl, int xid) {
  pthread_mutex_lock(&tbl->mut);

  tbl->table[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  DEBUG("%s:%d deactivate %d\n",__FILE__,__LINE__,xid);
  tbl->active_count--;
  assert( tbl->active_count >= 0 );
  pthread_mutex_unlock(&tbl->mut);
  return 0;
}
int stasis_transaction_table_forget(stasis_transaction_table_t *tbl, int xid) {
  assert(xid != INVALID_XTABLE_XID);
  stasis_transaction_table_entry_t * l = &tbl->table[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    pthread_mutex_lock(&tbl->mut);
    l->xid = INVALID_XTABLE_XID;
    l->prevLSN = -1;
    l->recLSN = -1;
    tbl->active_count--;
    assert(tbl->active_count >= 0);
    pthread_mutex_unlock(&tbl->mut);
  } else {
    assert(l->xid == INVALID_XTABLE_XID);
  }
  return 0;
}
