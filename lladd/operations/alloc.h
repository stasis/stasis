#include <lladd/operations.h>

#ifndef __ALLOC_H
#define __ALLOC_H

Operation getAlloc();
Operation getDealloc();
recordid Talloc(int xid, size_t size);
void Tdealloc(int xid, recordid rid);

#endif
