#ifndef __LSN_FREE_SET_H
#define __LSN_FREE_SET_H
Operation getSetLsnFree();
Operation getSetLsnFreeInverse();
int TsetLSNFree(int xid, recordid rid, const void *dat);
#endif //__LSN_FREE_SET_H
