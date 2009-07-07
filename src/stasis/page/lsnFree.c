#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <stasis/logger/logger2.h>

void stasis_slotted_lsn_free_initialize_page(Page * p) {
  stasis_slotted_initialize_page(p);
  p->pageType = SLOTTED_LSN_FREE_PAGE;
}
// XXX still not correct; need to have an "LSN_FREE" constant.
static void lsnFreeLoaded(Page * p) {
  p->LSN = 0; //stasis_log_file->next_available_lsn(stasis_log_file);
}
static void lsnFreeFlushed(Page * p) { }

page_impl slottedLsnFreeImpl() {
  page_impl pi = slottedImpl();
  pi.page_type = SLOTTED_LSN_FREE_PAGE;
  pi.pageLoaded = lsnFreeLoaded;
  pi.pageLoaded = lsnFreeFlushed;
  return pi;
}
