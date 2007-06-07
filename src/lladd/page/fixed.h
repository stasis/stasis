#include "../page.h"

#ifndef __FIXED_H 
#define __FIXED_H

#define recordsize_ptr(page)  shorts_from_end((page), 1)
#define recordcount_ptr(page) shorts_from_end((page), 2)
#define fixed_record_ptr(page, n)   bytes_from_start((page), *recordsize_ptr((page)) * (n))

void fixedPageInit();
void fixedPageDeinit();
page_impl fixedImpl();
page_impl arrayListImpl();
#endif
