/*
 * redBlackMemoryOverhead.c
 *
 *  Created on: Nov 17, 2010
 *      Author: sears
 */

#include <config.h>
#include <stasis/common.h>
#include <stasis/util/redblack.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef struct {
  uint64_t key;
  unsigned char * value;
} entry;

int cmp(const void *ap, const void *bp, const void *ign) {
  const entry *a = ap, *b = bp;
  return (a->key < b->key) ? -1 : (a->key == b->key) ? 0 : 1;
}

int main(int argc, char * argv[]) {
  if(argc != 3) {
    printf("Usage: %s [db size-in-mb] [tuple size in bytes]\n", argv[0]);
    return 1;
  }
  int64_t contents_size = atoll(argv[1]) * (1024 * 1024);
  int32_t tuple_size = atoi(argv[2]);
  int64_t tuple_count = contents_size / tuple_size;
  int64_t tree_count = 0;
  printf("tuple count = %lld / %lld = %lld\n", (long long) contents_size, (long long) tuple_size, (long long)tuple_count); fflush(stdout);

  int64_t last_memory_usage = 0;

  entry dummy;
  dummy.key = 0;
  struct rbtree *tree = rbinit(cmp,0);
  uint64_t iter = 0;
  int quiesce_count = 0;
  while(1) {
    if(tree_count < tuple_count) {
//      printf("a1000");
//      for(int i = 0; i < 1000; i++) {
        entry * e = stasis_alloc(entry);
        e->key = ((uint64_t)random()) * (uint64_t)random();
        int sz = random() % (2 * tuple_size - sizeof(e));
        e->value = stasis_malloc(sz, unsigned char);
        for(int j = 0; j < (sz); j++) {
          e->value[j] = (unsigned char) (j & 255);
        }
        entry * f = (entry*)/*no-const*/rbsearch(e, tree);
        if(f == e) {
          tree_count++;
        } else {
          free (e->value);
          free (e);
        }
//      }
    } else {
      const entry * e = rblookup(RB_LUGTEQ, &dummy, tree);
//      printf("d");
      if(!e) {
        dummy.key = 0;
      } else {
        rbdelete(e, tree);
        free(e->value);
        free((void*)e);
        tree_count--;
      }
    }
    if(! (iter & ((1 << 20)-1))) {
//      printf("%lld:\n", (long long)iter);
      int fd = open("/proc/self/statm", O_RDONLY);
      char buf[40];
      read(fd, buf, 40);
      buf[39] = 0;
      close(fd);
      int64_t mem =  (atol(buf) * 4096) / (1024 * 1024);
      printf("# %lldmb\n", (long long) mem);  fflush(stdout);
      if(mem > 8000) {
        printf("FAIL\n"); fflush(stdout);
        break;
      }
      if(mem <= last_memory_usage) {
        quiesce_count ++;
        if(quiesce_count == 10) {
          printf("Results: %lld %lld %lld %lld (|tuple|, #tuples, actual mem, target mem)\n", (long long) tuple_size, (long long) tuple_count, (long long) mem, (long long) contents_size);  fflush(stdout);
          break;
        }
      } else {
        last_memory_usage = mem;
        quiesce_count = 0;
      }
//      system("free -m");

#if 0
      break;
#endif
    }
    iter++;

  }

  // Tear down tree (need to do this so that valgrind can check for memory leaks...)
  dummy.key = 0;
  while(1) {
    const entry * e = rblookup(RB_LUGTEQ, &dummy, tree);
    if(!e) { break; }
    rbdelete(e, tree);
    free(e->value);
    free((void*)e);
    tree_count--;
      }
  rbdestroy(tree);
  return 0;
}
