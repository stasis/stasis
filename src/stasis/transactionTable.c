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
//  int active_count;
//  int xid_count;
  /**
      This mutex protects the rest of the struct
      xidCount.
  */
//  pthread_mutex_t mut;
  /**
      This key points to thread local state, including fast-path access to RESERVED_XIDs.
   */
//  pthread_key_t   key;
  stasis_transaction_table_entry_t table[MAX_TRANSACTIONS];
};


struct stasis_transaction_table_thread_local_state_t {
  stasis_transaction_table_entry_t ** entries;
};

typedef enum {
  /** The transaction table entry is invalid, and ready for reuse. */
  INVALID_XTABLE_XID = INVALID_XID,
  /** The transaction table entry is invalid, but some thread is in the process of initializing it.  */
  PENDING_XTABLE_XID = -2,
  /** The transaction table entry is invalid, but it is in some thread's thread local state */
  RESERVED_XTABLE_XID = -3
} stasis_transaction_table_status;

int stasis_transaction_table_num_active(stasis_transaction_table_t *tbl) {
  int ret = 0;
  printf("Looping for num_active");
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_lock(&(tbl->table[i].mut));
    if(tbl->table[i].xid >= 0) { ret++; }
    pthread_mutex_unlock(&(tbl->table[i].mut));
  }
  return ret;
}
/*
static void stasis_transaction_table_thread_destructor(void * p) {
  assert(p);

  struct stasis_transaction_table_thread_local_state_t * tls = p;
  stasis_transaction_table_entry_t * e;
  for(int i = 0; NULL != (e = tls->entries[i]); i++) {
    pthread_mutex_lock(&e->mut);
    if(e->xid == RESERVED_XTABLE_XID) {
      e->xid = INVALID_XTABLE_XID;
    }
    pthread_mutex_unlock(&e->mut);
  }
  free(tls->entries);
  free(tls);
}
*/

int stasis_transaction_table_is_active(stasis_transaction_table_t *tbl, int xid) {
  return xid >= 0 && tbl->table[xid % MAX_TRANSACTIONS].xid == xid;
}

int* stasis_transaction_table_list_active(stasis_transaction_table_t *tbl) {
  int * ret = malloc(sizeof(*ret));
  ret[0] = 0;
  int retcount = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_lock(&tbl->table[i].mut);
    if(tbl->table[i].xid >= 0) {
      ret[retcount] = tbl->table[i].xid;
      retcount++;
      ret = realloc(ret, (retcount+1) * sizeof(*ret));
      ret[retcount] = INVALID_XID;
    }
    pthread_mutex_unlock(&tbl->table[i].mut);
  }
  return ret;
}

stasis_transaction_table_t *  stasis_transaction_table_init() {
  stasis_transaction_table_t * tbl = malloc(sizeof(*tbl));
//  pthread_mutex_init(&tbl->mut, NULL);

//  memset(tbl->table, INVALID_XTABLE_XID,
//     sizeof(stasis_transaction_table_entry_t)*MAX_TRANSACTIONS);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    tbl->table[i].xid = INVALID_XTABLE_XID;
    pthread_mutex_init(&(tbl->table[i].mut),0);
  }

  printf("initted xact table!\n");

//  pthread_key_create(&tbl->key, stasis_transaction_table_thread_destructor);

  return tbl;
}

void stasis_transaction_table_deinit(stasis_transaction_table_t *tbl) {
//  pthread_mutex_destroy(&tbl->mut);

  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_destroy(&tbl->table[i].mut);
  }
//  pthread_key_delete(tbl->key);
  free(tbl);
}
lsn_t stasis_transaction_table_minRecLSN(stasis_transaction_table_t *tbl) {
  lsn_t minRecLSN = LSN_T_MAX;
  printf("Looping for minRecLSN");
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(tbl->table[i].xid >= 0) {
      pthread_mutex_lock(&tbl->table[i].mut);
      lsn_t recLSN = tbl->table[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) {
        minRecLSN = recLSN;
      }
      pthread_mutex_unlock(&tbl->table[i].mut);
    }
  }
  return minRecLSN;
}

int stasis_transaction_table_roll_forward(stasis_transaction_table_t *tbl, int xid, lsn_t lsn, lsn_t prevLSN) {
  stasis_transaction_table_entry_t * l = &tbl->table[xid%MAX_TRANSACTIONS];
  pthread_mutex_lock(&l->mut);
  if(l->xid == xid) {
    // rolling forward CLRs / NTAs makes prevLSN decrease.
    assert(l->prevLSN >= prevLSN);
  } else {
    assert(l->xid == INVALID_XTABLE_XID  || l->xid == RESERVED_XTABLE_XID);
    l->xid = xid;
    l->recLSN = lsn;
  }
  l->prevLSN = lsn;
  pthread_mutex_unlock(&l->mut);
  return 0;
}
int stasis_transaction_table_roll_forward_with_reclsn(stasis_transaction_table_t *tbl, int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN) {
  assert(tbl->table[xid%MAX_TRANSACTIONS].recLSN == recLSN);
  return stasis_transaction_table_roll_forward(tbl, xid, lsn, prevLSN);
}

stasis_transaction_table_entry_t * stasis_transaction_table_begin(stasis_transaction_table_t *tbl, int * xid) {


  stasis_transaction_table_entry_t * ret;

  int index = INVALID_XID;

  for(int i = 0; i < MAX_TRANSACTIONS; i++ ) {
    pthread_mutex_lock(&tbl->table[i].mut);
    if( tbl->table[i].xid == INVALID_XTABLE_XID ) {
      index = i;
      tbl->table[index].xid = PENDING_XTABLE_XID;
      pthread_mutex_unlock(&tbl->table[i].mut);
      break;
    } else {
      pthread_mutex_unlock(&tbl->table[i].mut);
    }
  }
  if(index == INVALID_XID) {
    *xid = LLADD_EXCEED_MAX_TRANSACTIONS;
    ret = NULL;
  } else {
    printf("begin xid %d\n", index);
    *xid = index;

    ret = &tbl->table[index];
  }

  return ret;
}
stasis_transaction_table_entry_t * stasis_transaction_table_get(stasis_transaction_table_t *tbl, int xid) {
  if(tbl->table[xid % MAX_TRANSACTIONS].xid == xid) {
    return &tbl->table[xid % MAX_TRANSACTIONS];
  } else {
    return NULL;
  }
}
int stasis_transaction_table_commit(stasis_transaction_table_t *tbl, int xid) {
  pthread_mutex_lock(&tbl->table[xid%MAX_TRANSACTIONS].mut);

  tbl->table[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  DEBUG("%s:%d deactivate %d\n",__FILE__,__LINE__,xid);

  pthread_mutex_unlock(&tbl->table[xid%MAX_TRANSACTIONS].mut);
  return 0;
}
int stasis_transaction_table_forget(stasis_transaction_table_t *tbl, int xid) {
  assert(xid != INVALID_XTABLE_XID);
  stasis_transaction_table_entry_t * l = &tbl->table[xid%MAX_TRANSACTIONS];

  pthread_mutex_lock(&tbl->table[xid%MAX_TRANSACTIONS].mut);
  if(l->xid != xid) {
    assert(l->xid < 0); // otherwise, more than one xact had this slot at once...
  } else {
    l->xid = INVALID_XTABLE_XID;
    l->prevLSN = -1;
    l->recLSN = -1;
  }
  pthread_mutex_unlock(&tbl->table[xid%MAX_TRANSACTIONS].mut);

  return 0;
}
