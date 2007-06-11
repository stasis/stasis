#include "../page.h"
#include "header.h"
#include <assert.h>

int headerPageInitialize() {
  Page * p;
  try_ret(0) { 
    p = loadPage(-1, 0);
    assert(!compensation_error());
  } end_ret(0);
  int freePage;
  if(*page_type_ptr(p) != LLADD_HEADER_PAGE) { 
    assert(*page_type_ptr(p) == 0) ;
    memset(p->memAddr, 0, PAGE_SIZE);
    *page_type_ptr(p) = LLADD_HEADER_PAGE;
    *headerFreepage_ptr(p) = 1;
    *headerFreepagelist_ptr(p) = 0;    
  }
  
  freePage = *headerFreepage_ptr(p);
  releasePage(p);
  assert(freePage);
  return freePage;
}

void freePage(Page * freepage, long freepage_id, Page * headerpage) {
  memset(freepage->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(freepage) = LLADD_FREE_PAGE;

  *nextfreepage_ptr(freepage) = *headerFreepagelist_ptr(headerpage);
  *headerFreepagelist_ptr(headerpage) = freepage_id;
}

/**
   freepage Must be the head of the freepage list (right now,
   the free list is essentially treated like a stack.
*/
void unfreePage(Page * freepage, Page * headerpage) {
  *headerFreepagelist_ptr(headerpage) = *nextfreepage_ptr(freepage);
  memset(freepage->memAddr, 0, PAGE_SIZE);
}
