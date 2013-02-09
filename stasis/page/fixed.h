#include "../page.h"

#ifndef __FIXED_H
#define __FIXED_H

BEGIN_C_DECLS

void stasis_page_fixed_init();
void stasis_page_fixed_deinit();

void stasis_page_fixed_initialize_page(Page * page, size_t size, int count);
recordid stasis_page_fixed_next_record(int xid, Page *p, recordid rid);

page_impl stasis_page_fixed_impl();
page_impl stasis_page_array_list_impl();

END_C_DECLS

#endif
