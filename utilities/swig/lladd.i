%module lladd
%{
#include "lladd/transactional.h"
%}

extern int Tinit();
extern int Tdeinit();

extern int Tbegin();

extern void Tupdate(int xid, recordid rid, const void *dat, int op);
extern void TupdateStr(int xid, recordid rid, const char *dat, int op);
extern void TupdateRaw(int xid, recordid rid, const void *dat, int op);

extern void Tread(int xid, recordid rid, void *dat);
extern void TreadStr(int xid, recordid rid, char *dat);

extern void TreadUnlocked(int xid, recordid rid, void *dat);

extern int Tcommit(int xid);
extern int Tabort(int xid);

extern void Trevive(int xid, long lsn);

extern void TsetXIDCount(int xid);

extern int TisActiveTransaction(int xid);

extern lsn_t transactions_minRecLSN();

extern int TdurabilityLevel();

extern recordid Talloc(int xid, unsigned long size);
extern recordid TallocFromPage(int xid, long page, unsigned long size);
extern void Tdealloc(int xid, recordid rid);

%include "lladd/constants.h"


