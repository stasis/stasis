/*
 * smallLogEntry.c
 *
 *  Created on: Oct 12, 2009
 *      Author: sears
 */

#include <stasis/logger/logger2.h>
#include <stasis/logger/safeWrites.h>
#include <stasis/logger/inMemoryLog.h>
#include <stasis/flags.h>

#include <pthread.h>
#include <stdio.h>


char * usage = "%s numthreads numops\n";

stasis_log_t * l;

static void* worker(void* arg) {
  unsigned long numops = *(unsigned long*) arg;
  LogEntry e;
  e.LSN = 0;
  e.prevLSN = 0;
  e.type = UPDATELOG;
  e.xid = 0;
  e.update.arg_size = 0;
  e.update.funcID = 0;
  e.update.page = INVALID_PAGE;

  for(unsigned long i = 0; i < numops; i++) {
    l->write_entry(l, &e);
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
  if(stasis_log_type == LOG_TO_FILE) {
    l = stasis_log_safe_writes_open(stasis_log_file_name, stasis_log_file_mode, stasis_log_file_permissions);
  } else {
    l = stasis_log_impl_in_memory_open();
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_create(&workers[i], 0, worker, &numops);
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_join(workers[i], 0);
  }
}
