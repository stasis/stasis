#include "../page.h"
#include "header.h"


void headerPageInitialize(Page * page) {
  memset(page->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(page) = LLADD_HEADER_PAGE;
  *headerFreepage_ptr(page) = 1;
  *headerFreepagelist_ptr(page) = 0;
}

void freePage(Page * freepage, long freepage_id, Page * headerpage) {
  memset(freepage->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(freepage) = LLADD_FREE_PAGE;

  *nextfreepage_ptr(freepage) = *headerFreepagelist_ptr(headerpage);
  *headerFreepagelist_ptr(headerpage) = freepage_id;
}

/**
   @param freepage Must be the head of the freepage list (right now,
   the free list is essentially treated like a stack.
*/
void unfreePage(Page * freepage, Page * headerpage) {
  *headerFreepagelist_ptr(headerpage) = *nextfreepage_ptr(freepage);
  memset(freepage->memAddr, 0, PAGE_SIZE);
}
