/*
 * hashtableDifferentPages.c
 *
 *  Created on: Oct 19, 2009
 *      Author: sears
 */
#include <stasis/util/concurrentHash.h>
#include <stasis/transactional.h>
#include <pthread.h>
#include <stdio.h>


char * usage = "%s numthreads numops\n";

unsigned long numops;

hashtable_t * ht;

static void* worker(void* arg) {
  pageid_t pid = *(pageid_t*)arg;
  hashtable_insert(ht, pid, &pid);
  for(unsigned long i = 0; i < numops; i++) {
    void * ptr = hashtable_lookup(ht, pid);
    assert(ptr == &pid);
    //    Page * p = loadPage(-1, pid);
//    releasePage(p);
  }

  return 0;
}

int main(int argc, char * argv[]) {
  if(argc != 3) { printf(usage, argv[0]); abort(); }
  char * endptr;
  unsigned long numthreads = strtoul(argv[1], &endptr, 10);
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }
  numops= strtoul(argv[2], &endptr, 10) / numthreads;
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }

  pthread_t workers[numthreads];
  pageid_t pids[numthreads];

  ht = hashtable_init(numthreads * 10);

  for(int i = 0; i < numthreads; i++) {
    pids[i] = i*2   ;
    pthread_create(&workers[i], 0, worker, &pids[i]);
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_join(workers[i], 0);
  }

  hashtable_deinit(ht);
}
