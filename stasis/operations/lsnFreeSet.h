#ifndef __LSN_FREE_SET_H
#define __LSN_FREE_SET_H
#include <stasis/logger/reorderingHandle.h>
Operation getSetLsnFree();
Operation getSetLsnFreeInverse();
int TsetLsnFree(int xid, recordid rid, const void *dat);
int TsetLsnReorderable(int xid, stasis_log_reordering_handle_t * h,
                       recordid rid, const void *dat);
#endif //__LSN_FREE_SET_H
