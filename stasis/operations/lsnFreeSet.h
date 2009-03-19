#ifndef __LSN_FREE_SET_H
#define __LSN_FREE_SET_H
#include <stasis/logger/reorderingHandle.h>
Operation getSetLsnFree();
Operation getSetLsnFreeInverse();
int TsetLsnFree(int xid, recordid rid, const void *dat);
int TsetReorderable(int xid, stasis_log_reordering_handle_t * h,
                       recordid rid, const void *dat);
int TsetWriteBack(int xid, pageid_t page, pageoff_t off, pageoff_t len,
                  const void * dat, const void * olddat);
#endif //__LSN_FREE_SET_H
