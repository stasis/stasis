/*
 * transactionTable.h
 *
 *  Created on: Oct 14, 2009
 *      Author: sears
 */

#ifndef TRANSACTIONTABLE_H_
#define TRANSACTIONTABLE_H_

#include <stasis/common.h>
typedef struct TransactionLog TransactionLog;

/**
   Contains the state needed by the logging layer to perform
   operations on a transaction.
 */
struct TransactionLog {
  int xid;
  lsn_t prevLSN;
  lsn_t recLSN;
  pthread_mutex_t mut;
};

/**
   XXX TransactionTable should be private to transactional2.c!
*/
extern TransactionLog stasis_transaction_table[MAX_TRANSACTIONS];

/**
   Initialize Stasis' transaction table.  Called by Tinit() and unit
   tests that wish to test portions of Stasis in isolation.
 */
void stasis_transaction_table_init();
/** Free resources associated with the transaction table */
void stasis_transaction_table_deinit();
/**
 *  Used by recovery to prevent reuse of old transaction ids.
 *
 *  Should not be used elsewhere.
 *
 * @param xid  The highest transaction id issued so far.
 */
void stasis_transaction_table_max_transaction_id_set(int xid);
/**
 *  Used by test cases to mess with internal transaction table state.
 *
 * @param xid  The new active transaction count.
 */
void stasis_transaction_table_active_transaction_count_set(int xid);

int stasis_transaction_table_roll_forward(int xid, lsn_t lsn, lsn_t prevLSN);
/**
   @todo update Tprepare() to not write reclsn to log, then remove
         this function.
 */
int stasis_transaction_table_roll_forward_with_reclsn(int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN);
/**
    This is used by log truncation.
*/
lsn_t stasis_transaction_table_minRecLSN();

TransactionLog * stasis_transaction_table_begin(int * xid);
TransactionLog * stasis_transaction_table_get(int xid);
int stasis_transaction_table_commit(int xid);
int stasis_transaction_table_forget(int xid);

int stasis_transaction_table_num_active();
int* stasis_transaction_table_list_active();

#endif /* TRANSACTIONTABLE_H_ */
