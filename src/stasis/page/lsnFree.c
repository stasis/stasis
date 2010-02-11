#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <stasis/logger/logger2.h>

void stasis_slotted_lsn_free_initialize_page(Page * p) {
  stasis_page_slotted_initialize_page(p);
  p->pageType = SLOTTED_LSN_FREE_PAGE;
}

page_impl slottedLsnFreeImpl() {
  page_impl pi = stasis_page_slotted_impl();
  pi.has_header = 0;
  pi.page_type = SLOTTED_LSN_FREE_PAGE;
  pi.pageLoaded = 0;
  pi.pageFlushed = 0;
  return pi;
}
