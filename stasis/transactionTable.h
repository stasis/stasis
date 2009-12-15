/*
 * transactionTable.h
 *
 *  Created on: Oct 14, 2009
 *      Author: sears
 */

#ifndef TRANSACTIONTABLE_H_
#define TRANSACTIONTABLE_H_

#include <stasis/common.h>

typedef int (*stasis_transaction_table_callback_t)(int, void*);

typedef struct stasis_transaction_table_callback_list_t stasis_transaction_table_callback_list_t;
typedef struct stasis_transaction_table_entry_t stasis_transaction_table_entry_t;
typedef struct stasis_transaction_table_t stasis_transaction_table_t;

/**
   Contains the state needed by the logging layer to perform
   operations on a transaction.
 */
struct stasis_transaction_table_entry_t {
  int xid;
  lsn_t prevLSN;
  lsn_t recLSN;
  void ** commitArgs[3];
#ifndef HAVE_GCC_ATOMICS
  pthread_mutex_t mut;
#endif
};
/**
   Initialize Stasis' transaction table.  Called by Tinit() and unit
   tests that wish to test portions of Stasis in isolation.
 */
stasis_transaction_table_t* stasis_transaction_table_init();
/** Free resources associated with the transaction table */
void stasis_transaction_table_deinit(stasis_transaction_table_t*);

int stasis_transaction_table_roll_forward(stasis_transaction_table_t*,int xid, lsn_t lsn, lsn_t prevLSN);
/**
   @todo update Tprepare() to not write reclsn to log, then remove
         this function.
 */
int stasis_transaction_table_roll_forward_with_reclsn(stasis_transaction_table_t*,int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN);
/**
    This is used by log truncation.
*/
lsn_t stasis_transaction_table_minRecLSN(stasis_transaction_table_t*);

stasis_transaction_table_entry_t * stasis_transaction_table_begin(stasis_transaction_table_t*,int * xid);
stasis_transaction_table_entry_t * stasis_transaction_table_get(stasis_transaction_table_t*,int xid);
int stasis_transaction_table_commit(stasis_transaction_table_t*,int xid);
int stasis_transaction_table_forget(stasis_transaction_table_t*,int xid);

int stasis_transaction_table_num_active_threads(stasis_transaction_table_t*);
int* stasis_transaction_table_list_active(stasis_transaction_table_t*, int *count);
int stasis_transaction_table_is_active(stasis_transaction_table_t*, int xid);

typedef enum {
  PRE_COMMIT = 0,
  AT_COMMIT = 1,
  POST_COMMIT = 2
} stasis_transaction_table_callback_type_t;

int stasis_transaction_table_register_callback(stasis_transaction_table_t *tbl,
					       stasis_transaction_table_callback_t cb,
					       stasis_transaction_table_callback_type_t type);
int stasis_transaction_table_invoke_callbacks(stasis_transaction_table_t *tbl,
                                              stasis_transaction_table_entry_t *entry,
                                              stasis_transaction_table_callback_type_t type);
int stasis_transaction_table_set_argument(stasis_transaction_table_t *tbl, int xid, int callback_id,
                                          stasis_transaction_table_callback_type_t type, void *arg);

#endif /* TRANSACTIONTABLE_H_ */
