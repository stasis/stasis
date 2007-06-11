
#ifndef __LLADD_RECOVERY2_H
#define __LLADD_RECOVERY2_H

void  InitiateRecovery();
/** This really doesn't belong in recovery.c, but there's so much code overlap, it doesn't make sense not to put it there. */
void  undoTrans();

#endif
