#include <stasis/transactional.h>
#include <stasis/bufferManager.h>

Page* __real_loadPage(int xid, pageid_t pid);
Page* __wrap_loadPage(int xid, pageid_t pid) {
  printf("CALL loadPage(%d, %lld)\n", (int)xid, (long long)pid);
  Page * ret = __real_loadPage(xid, pid);
  printf("RET  %lx = loadPage(%d, %lld)\n", (intptr_t)ret, (int)xid, (long long)pid);
  return ret;
}
void __real_releasePage(Page* p);
void __wrap_releasePage(Page* p) {
  printf("CALL releasePage(%lx)\n", (intptr_t)p);
  __real_releasePage(p);
  printf("RET  void = releasePage(%lx)\n", (intptr_t)p);
}
