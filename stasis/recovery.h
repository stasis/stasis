#ifndef __LLADD_RECOVERY2_H
#define __LLADD_RECOVERY2_H

#include <stasis/transactionTable.h>
#include <stasis/logger/logger2.h>
#include <stasis/operations/alloc.h>

void  stasis_recovery_initiate(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_alloc_t * alloc);
/** This really doesn't belong in recovery.c, but there's so much code overlap, it doesn't make sense not to put it there.
 *
 *  XXX undoTrans should not take the entire transaction table as an argument.  Instead, it should place its transaction argument directly into the list of transactions that undo processes.
 * */
void  undoTrans(stasis_log_t*log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t transaction);

#endif
