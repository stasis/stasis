/*
 * transactionTable.c
 *
 *  Created on: Oct 14, 2009
 *      Author: sears
 */
#include <config.h>
#include <stasis/common.h>
#include <stasis/constants.h>
#include <stasis/transactionTable.h>
#include <stasis/transactional.h>
#include <assert.h>
#include <sys/syscall.h>                // SYS_gettid

struct stasis_transaction_table_t {
  int active_count;
#ifndef HAVE_GCC_ATOMICS
  /**
      This mutex protects the rest of the struct
      xidCount.
  */
  pthread_mutex_t mut;
#endif
  /**
      This key points to thread local state, including fast-path access to RESERVED_XIDs.
   */
  pthread_key_t   key;
  stasis_transaction_table_entry_t table[MAX_TRANSACTIONS];
  stasis_transaction_table_callback_t * commitCallbacks[3];
  int commitCallbackCount[3];
};

static inline int test_and_set_entry(stasis_transaction_table_entry_t* e, int old, int new) {
#ifdef HAVE_GCC_ATOMICS
  return __sync_bool_compare_and_swap(&(e->xid), old, new);
#else
  pthread_mutex_lock(&(e->mut));
  if(e->xid == old) {
    e->xid = new;
    pthread_mutex_unlock(&(e->mut));
    return 1;
  } else {
    pthread_mutex_unlock(&(e->mut));
    return 0;
  }
#endif
}
//  May not be called in race; protects readers from incomplete reads.
static inline void set_entry(stasis_transaction_table_entry_t* e, int new) {
#ifdef HAVE_GCC_ATOMICS
  int i = __sync_fetch_and_add(&(e->xid),0);
  int succ = test_and_set_entry(e, i, new);
  assert(succ);
#else
  pthread_mutex_lock(&(e->mut));

  e->xid = new;

  pthread_mutex_unlock(&(e->mut));
#endif
}
static inline int incr_active_count(stasis_transaction_table_t* t, int d) {
#ifdef HAVE_GCC_ATOMICS
  return __sync_fetch_and_add(&(t->active_count), d);
#else
  pthread_mutex_lock(&(t->mut));
  int ret = t->active_count;
  (t->active_count) += d;
  pthread_mutex_unlock(&(t->mut));
  return ret;
#endif
}
static inline int get_entry_xid(stasis_transaction_table_entry_t* e) {
#ifdef HAVE_GCC_ATOMICS
  return __sync_fetch_and_add(&(e->xid), 0);
#else
  pthread_mutex_lock(&e->mut);
  int ret = e->xid;
  pthread_mutex_unlock(&e->mut);
  return ret;
#endif
}
struct stasis_transaction_table_thread_local_state_t {
  intptr_t last_entry;
  intptr_t num_entries;
  stasis_transaction_table_t * tbl;
  stasis_transaction_table_entry_t ** entries;
  int * indexes;
  pid_t tid;
};

typedef enum {
  /** The transaction table entry is invalid, and ready for reuse. */
  INVALID_XTABLE_XID = INVALID_XID,
  /** The transaction table entry is invalid, but some thread is in the process of initializing it.  */
  PENDING_XTABLE_XID = -2,
  /** The transaction table entry is invalid, but it is in some thread's thread local state */
  RESERVED_XTABLE_XID = -3
} stasis_transaction_table_status;

int stasis_transaction_table_num_active_threads(stasis_transaction_table_t *tbl) {
  return incr_active_count(tbl, 0);
}

static void stasis_transaction_table_thread_destructor(void * p) {
  assert(p);

  struct stasis_transaction_table_thread_local_state_t * tls = p;
  stasis_transaction_table_entry_t * e;
  for(int i = 0; i < tls->num_entries; i++) {
    e = tls->entries[i];
    // XXX can leak; is possible that our xids are still running.
    test_and_set_entry(e, RESERVED_XTABLE_XID, INVALID_XTABLE_XID);
  }
  incr_active_count(tls->tbl, -1);

  free(tls->entries);
  free(tls->indexes);
  free(tls);


}

int stasis_transaction_table_is_active(stasis_transaction_table_t *tbl, int xid) {
  assert(xid < MAX_TRANSACTIONS);
  return xid >= 0 && tbl->table[xid].xid == xid;
}

int stasis_transaction_table_register_callback(stasis_transaction_table_t *tbl,
                                                             stasis_transaction_table_callback_t cb,
                                                             stasis_transaction_table_callback_type_t type) {
  assert(type >= 0 && type < 3);
  stasis_transaction_table_callback_t **list = &tbl->commitCallbacks[type];
  int *count = &tbl->commitCallbackCount[type];

  *list = realloc(*list, (1+*count) * sizeof(*list[0]));
  (*list)[*count] = cb;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    void *** args;
    args = &tbl->table[i].commitArgs[type];
    *args = realloc(*args, (1+*count) * sizeof(*args[0]));
    (*args)[*count] = 0;
  }
  return (*count)++;
}

int stasis_transaction_table_invoke_callbacks(stasis_transaction_table_t *tbl,
                                              stasis_transaction_table_entry_t * entry,
                                              stasis_transaction_table_callback_type_t type) {
  assert(type >= 0 && type < 3);
  stasis_transaction_table_callback_t *list = tbl->commitCallbacks[type];
  int count = tbl->commitCallbackCount[type];
  void **args = entry->commitArgs[type];

  int ret = 0;
  for(int i = 0; i < count; i++) {
    if(args[i]) {
      ret = list[i](entry->xid, args[i]) || ret;
      args[i] = 0;
    }
  }
  return ret;
}
int stasis_transaction_table_set_argument(stasis_transaction_table_t *tbl, int xid, int callback_id,
                                          stasis_transaction_table_callback_type_t type, void *arg) {
  assert(type >= 0 && type < 3);
  int count = tbl->commitCallbackCount[type];
  void ** args = tbl->table[xid].commitArgs[type];
  assert(count > callback_id);
  args[callback_id] = arg;
  return 0;
}

int* stasis_transaction_table_list_active(stasis_transaction_table_t *tbl, int *count) {
  int * ret = stasis_alloc(int);
  ret[0] = INVALID_XID;
  *count = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    int e_xid = get_entry_xid(&tbl->table[i]);
    if(e_xid >= 0) {
      ret[*count] = e_xid;
      (*count)++;
      ret = realloc(ret, ((*count)+1) * sizeof(*ret));
      ret[*count] = INVALID_XID;
    }
  }
  return ret;
}

stasis_transaction_table_t *  stasis_transaction_table_init() {
  stasis_transaction_table_t * tbl = stasis_alloc(stasis_transaction_table_t);
  tbl->active_count = 0;

#ifndef HAVE_GCC_ATOMICS
  pthread_mutex_init(&tbl->mut, NULL);
#endif

  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    tbl->table[i].xid = INVALID_XTABLE_XID;
    tbl->table[i].xidWhenFree = INVALID_XTABLE_XID;
    tbl->table[i].commitArgs[0] = 0;
    tbl->table[i].commitArgs[1] = 0;
    tbl->table[i].commitArgs[2] = 0;
    tbl->table[i].tid = -1;
#ifndef HAVE_GCC_ATOMICS
    pthread_mutex_init(&(tbl->table[i].mut),0);
#endif
  }

  for(int i = 0; i < 3; i++) {
    tbl->commitCallbacks[i] = 0;
    tbl->commitCallbackCount[i] = 0;
  }

  DEBUG("initted xact table!\n");

  pthread_key_create(&tbl->key, stasis_transaction_table_thread_destructor);

  return tbl;
}

void stasis_transaction_post_recovery(stasis_transaction_table_t *tbl) {
    /*
     * Verify that all the entries are either:
     *
     *   - free in the global pool (common case)
     *   - active (recovered prepared transactions)
     *
     * In particular there should be no entries being initialized or
     * reserved by a thread
     */
    for(int i = 0; i < MAX_TRANSACTIONS; i++) {
        assert(tbl->table[i].xid == INVALID_XTABLE_XID ||
               tbl->table[i].xid >= 0);
        assert(tbl->table[i].tid == -1);
    }
}

void stasis_transaction_table_deinit(stasis_transaction_table_t *tbl) {
#ifndef HAVE_GCC_ATOMICS
  pthread_mutex_destroy(&tbl->mut);
#endif

  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
#ifndef HAVE_GCC_ATOMICS
    pthread_mutex_destroy(&tbl->table[i].mut);
#endif
    for(int j = 0; j < 3; j++) {
      if(tbl->table[i].commitArgs[j]) { free(tbl->table[i].commitArgs[j]); }
    }
  }
  for(int j = 0; j < 3; j++) {
    if(tbl->commitCallbacks[j]) { free(tbl->commitCallbacks[j]); }
  }

  pthread_key_delete(tbl->key);
  free(tbl);
}
lsn_t stasis_transaction_table_minRecLSN(stasis_transaction_table_t *tbl) {
  lsn_t minRecLSN = LSN_T_MAX;
  DEBUG("Looping for minRecLSN");
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    int e_xid = get_entry_xid(&(tbl->table[i]));
    if(e_xid >= 0) {
//      pthread_mutex_lock(&tbl->table[i].mut);
      //XXX assumes reading LSNs is an atomic memory operation.
      lsn_t recLSN = tbl->table[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) {
        minRecLSN = recLSN;
      }
//      pthread_mutex_unlock(&tbl->table[i].mut);
    }
  }
  return minRecLSN;
}

int stasis_transaction_table_roll_forward(stasis_transaction_table_t *tbl, int xid, lsn_t lsn, lsn_t prevLSN) {
  assert(xid >= 0 && xid < MAX_TRANSACTIONS);
  stasis_transaction_table_entry_t * l = &tbl->table[xid];
  if(test_and_set_entry(l, xid, xid)) {
//  if(l->xid == xid) {
    // rolling forward CLRs / NTAs makes prevLSN decrease.
    assert(l->prevLSN >= prevLSN);
  } else {
    int b2 = test_and_set_entry(l, RESERVED_XTABLE_XID, xid);
    int b1 = test_and_set_entry(l, INVALID_XTABLE_XID, xid);
    assert(b1 || b2);
    l->recLSN = lsn;
  }
  l->prevLSN = lsn;
  return 0;
}
int stasis_transaction_table_roll_forward_with_reclsn(stasis_transaction_table_t *tbl, int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN) {
  assert(xid >= 0 && xid < MAX_TRANSACTIONS);
  assert(tbl->table[xid].recLSN == recLSN);
  return stasis_transaction_table_roll_forward(tbl, xid, lsn, prevLSN);
}

stasis_transaction_table_entry_t * stasis_transaction_table_begin(stasis_transaction_table_t *tbl, int * xid) {

  stasis_transaction_table_entry_t * ret;

  // Initialize tls

  struct stasis_transaction_table_thread_local_state_t * tls = pthread_getspecific(tbl->key);

  if(tls == NULL) {
    tls = stasis_alloc(struct stasis_transaction_table_thread_local_state_t);
    tls->last_entry = 0;
    tls->num_entries = 0;
    tls->entries = NULL;
    tls->indexes = NULL;
    tls->tid = syscall(SYS_gettid);
    pthread_setspecific(tbl->key, tls);

    tls->tbl = tbl;

    incr_active_count(tbl, 1);
  }

  // Fast path - allocate from the local pool

  ret = 0;

  int index = INVALID_XID;

  for(intptr_t i = 0; i < tls->num_entries; i++) {
    intptr_t idx = (1 + i + tls->last_entry) % tls->num_entries;
    ret = tls->entries[idx];
    if(test_and_set_entry(ret, RESERVED_XTABLE_XID, PENDING_XTABLE_XID)) {
      index = tls->indexes[idx];
      break;
    }
  }

  // Slow path - allocate from the global pool

  if(index == INVALID_XID) {

    for(int i = 0; i < MAX_TRANSACTIONS; i++ ) {
      if(test_and_set_entry(&tbl->table[i], INVALID_XTABLE_XID, PENDING_XTABLE_XID)) {
        index = i;
        tls->num_entries++;
        tls->entries = realloc(tls->entries, sizeof(sizeof(ret)) * tls->num_entries);
        tls->entries[tls->num_entries-1] = &tbl->table[index];
        tls->indexes = realloc(tls->indexes, sizeof(int) * tls->num_entries);
        tls->indexes[tls->num_entries-1] = index;
        tbl->table[index].xidWhenFree = RESERVED_XTABLE_XID;
        tbl->table[index].tid = tls->tid;
        break;
      }
    }
  }
  if(index == INVALID_XID) {
    *xid = LLADD_EXCEED_MAX_TRANSACTIONS;
    ret = NULL;
  } else {
    DEBUG("begin xid %d\n", index);
    *xid = index;
    tls->last_entry = index;
    ret = &tbl->table[index];
  }

  return ret;
}
stasis_transaction_table_entry_t * stasis_transaction_table_get(stasis_transaction_table_t *tbl, int xid) {
  assert(xid >= 0 && xid < MAX_TRANSACTIONS);
  if(tbl->table[xid].xid == xid) {
    return &tbl->table[xid];
  } else {
    return NULL;
  }
}
int stasis_transaction_table_commit(stasis_transaction_table_t *tbl, int xid) {
  assert(xid >= 0 && xid < MAX_TRANSACTIONS);

  set_entry(&(tbl->table[xid]), tbl->table[xid].xidWhenFree);

  return 0;
}
int stasis_transaction_table_forget(stasis_transaction_table_t *tbl, int xid) {
  assert(xid >= 0 && xid < MAX_TRANSACTIONS);
  stasis_transaction_table_entry_t * l = &tbl->table[xid];

  if(test_and_set_entry(&tbl->table[xid], xid, tbl->table[xid].xidWhenFree)) {
    // success
  } else {
    // during recovery, we might forget something we've never heard of.
    assert(l->xid < 0); // otherwise, more than one xact had this slot at once...
  }
  return 0;
}
