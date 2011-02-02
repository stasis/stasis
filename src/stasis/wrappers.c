#include <stasis/transactional.h>
#include <stasis/bufferManager.h>

Page*  __real_loadPage(int xid, pageid_t pageid);
Page * __real_loadPageOfType(int xid, pageid_t pageid, pagetype_t type);
Page * __real_loadUninitializedPage(int xid, pageid_t pageid);
Page * __real_loadPageForOperation(int xid, pageid_t pageid, int op, int is_recovery);
void   __real_releasePage(Page* p);
Page * __real_getCachedPage(int xid, pageid_t pageid);

Page* __wrap_loadPage(int xid, pageid_t pageid) {
  printf("CALL loadPage(%d, %lld)\n", (int)xid, (long long)pageid);
  Page * ret = __real_loadPage(xid, pageid);
  printf("RET  %lx = loadPage(%d, %lld)\n", (intptr_t)ret, (int)xid, (long long)pageid);
  return ret;
}
Page * __wrap_loadPageOfType(int xid, pageid_t pageid, pagetype_t type) {
  printf("CALL loadPage(%d, %lld)\n", (int)xid, (long long)pageid);
  Page * ret = __real_loadPageOfType(xid, pageid, type);
  printf("RET  %lx = loadPage(%d, %lld)\n", (intptr_t)ret, (int)xid, (long long)pageid);
  return ret;
}
Page * __wrap_loadUninitializedPage(int xid, pageid_t pageid) {
  printf("CALL loadPage(%d, %lld)\n", (int)xid, (long long)pageid);
  Page * ret = __real_loadUninitializedPage(xid, pageid);
  printf("RET  %lx = loadPage(%d, %lld)\n", (intptr_t)ret, (int)xid, (long long)pageid);
  return ret;
}
Page * __wrap_loadPageForOperation(int xid, pageid_t pageid, int op, int is_recovery) {
  printf("CALL loadPage(%d, %lld)\n", (int)xid, (long long)pageid);
  Page * ret = __real_loadPageForOperation(xid, pageid, op, is_recovery);
  printf("RET  %lx = loadPage(%d, %lld)\n", (intptr_t)ret, (int)xid, (long long)pageid);
  return ret;
}
void   __wrap_releasePage(Page* p) {
  printf("CALL releasePage(%lx)\n", (intptr_t)p);
  __real_releasePage(p);
  printf("RET  void = releasePage(%lx)\n", (intptr_t)p);
}
Page * __wrap_getCachedPage(int xid, pageid_t pageid) {
  printf("CALL loadPage(%d, %lld)\n", (int)xid, (long long)pageid);
  Page * ret = __real_getCachedPage(xid, pageid);
  printf("RET  %lx = loadPage(%d, %lld)\n", (intptr_t)ret, (int)xid, (long long)pageid);
  return ret;
}
