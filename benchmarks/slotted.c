/*
 * slotted.c
 *
 *  Created on: Jul 15, 2009
 *      Author: sears
 */
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <stasis/transactional.h>

int main(int argc, char* argv[]) {
  unlink("storefile.txt");
  unlink("logfile.txt");
  int i = 1;
  // Should we pin the page once per run?
  int mode = 0;
  // How many operations?
  unsigned long long count = 1000000;
  while(i != argc) {
    if(!strcmp(argv[i], "--mode")) {
      i++;
      assert(i != argc);
      char * endptr;
      mode = strtoull(argv[i], &endptr, 10);
      assert(!*endptr);
      i++;
    } else if(!strcmp(argv[i], "--count")) {
      i++;
      assert(i != argc);
      char * endptr;
      count = strtoull(argv[i], &endptr, 10);
      assert(!*endptr);
      i++;
    } else {
      abort();
    }
  }
  printf("%d %lld\n", mode, count);

  Tinit();

  int xid = Tbegin();

  pageid_t page = TpageAlloc(xid);
  if(mode == 0) {
    Page * p = loadPage(xid, page);
    writelock(p->rwlatch, 0);
    stasis_page_slotted_initialize_page(p);

    recordid rid = stasis_record_alloc_begin(xid, p, sizeof(uint64_t));
    stasis_record_alloc_done(xid, p, rid);
    for(unsigned long long i = 0; i < count; i++) {
      uint64_t val = i;
      stasis_record_write(xid, p, rid, (const byte*)&val);
    }
    unlock(p->rwlatch);
    releasePage(p);
  } else if(mode == 1) {
    Page * p = loadPage(xid, page);
    writelock(p->rwlatch, 0);

    stasis_page_slotted_initialize_page(p);

    recordid rid = stasis_record_alloc_begin(xid, p, sizeof(uint64_t));
    stasis_record_alloc_done(xid, p, rid);

    unlock(p->rwlatch);

    for(unsigned long long i = 0; i < count; i++) {
      writelock(p->rwlatch, 0);
      uint64_t val = i;
      stasis_record_write(xid, p, rid, (const byte*)&val);
      unlock(p->rwlatch);
    }
    releasePage(p);
  } else if(mode == 2) {
    Page * p = loadPage(xid, page);
    writelock(p->rwlatch, 0);

    stasis_page_slotted_initialize_page(p);

    recordid rid = stasis_record_alloc_begin(xid, p, sizeof(uint64_t));
    stasis_record_alloc_done(xid, p, rid);

    unlock(p->rwlatch);
    releasePage(p);

    for(unsigned long long i = 0; i < count; i++) {
      Page * p = loadPage(xid, page);
      writelock(p->rwlatch, 0);
      uint64_t val = i;
      stasis_record_write(xid, p, rid, (const byte*)&val);
      unlock(p->rwlatch);
      releasePage(p);
    }
  } else if(mode == 3) {

    recordid rid = Talloc(xid, sizeof(uint64_t));

    for(unsigned long long i = 0; i < count; i++) {
      uint64_t val = i;
      Tset(xid, rid, &val);
    }
  } else {
    abort();
  }
  Tcommit(xid);

  Tdeinit();

  return 0;
}
