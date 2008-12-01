#ifndef __LLADD_RECOVERY2_H
#define __LLADD_RECOVERY2_H

#include <stasis/logger/logger2.h>

void  stasis_recovery_initiate(stasis_log_t* log);
/** This really doesn't belong in recovery.c, but there's so much code overlap, it doesn't make sense not to put it there. */
void  undoTrans(stasis_log_t*log, TransactionLog transaction);

#endif
