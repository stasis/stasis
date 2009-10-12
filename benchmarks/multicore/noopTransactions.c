/*
 * noopTransactions.c
 *
 *  Created on: Oct 12, 2009
 *      Author: sears
 */

#include <stasis/transactional.h>
#include <pthread.h>
#include <stdio.h>

char * usage = "%s numthreads numops\n";

static void* noopWorker(void* arg) {
  unsigned long numops = *(unsigned long*) arg;
  for(unsigned long i = 0; i < numops; i++) {
    int xid = Tbegin();
    TsoftCommit(xid);
  }
  return 0;
}

int main(int argc, char * argv[]) {
  if(argc != 3) { printf(usage, argv[0]); abort(); }
  char * endptr;
  unsigned long numthreads = strtoul(argv[1], &endptr, 10);
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }
  unsigned long numops= strtoul(argv[2], &endptr, 10) / numthreads;
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }

  pthread_t workers[numthreads];

  Tinit();

  for(int i = 0; i < numthreads; i++) {
    pthread_create(&workers[i], 0, noopWorker, &numops);
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_join(workers[i], 0);
  }

  Tdeinit();
}
