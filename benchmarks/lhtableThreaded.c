#define _GNU_SOURCE
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stasis/util/lhtable.h>
#include <stasis/util/malloc.h>
#include <assert.h>

int entries;
int thread_count;
int waiting = 0;
pthread_mutex_t startAtOnce    = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t threadWoke      = PTHREAD_COND_INITIALIZER;
pthread_cond_t startAtOnceCond = PTHREAD_COND_INITIALIZER;

struct LH_ENTRY(table) * t; 

char * itoa(int i) {
  char * ret;
  int err = asprintf(&ret, "%d", i);
  assert(err != -1);
  return ret;
}

static void * worker (void * arg) {
  int thread_num = *(int*)arg;
  int entries_s = entries;
  pthread_mutex_lock(&startAtOnce);
  waiting++;
  if(thread_count == waiting) { 
    pthread_cond_signal(&threadWoke);
  }
  pthread_cond_wait(&startAtOnceCond, &startAtOnce);
  pthread_mutex_unlock(&startAtOnce);

  for(int i = 0; i < entries_s; i++) {
    long val = (thread_num * entries_s) + i;
    char * key = itoa(val);
    LH_ENTRY(insert)(t, key, strlen(key), (void*)val);
    free(key);
  }

  return 0;
}
int main(int argc, char ** argv) { 
  
  const char * usage = "Usage: %s <thread_count> <entries>\n%s\n";

  if(argc != 3) {
    printf(usage, argv[0], "Wrong number of arguments");
    return -1;
  }
  errno = 0;
  char *endptr;
  
  thread_count = strtol(argv[1], &endptr, 10);

  if(errno || *endptr) {
    printf(usage, argv[0] ,"Could not parse # threads");
    return -1;
  }

  entries = strtol(argv[2], &endptr, 10);

  if(errno || *endptr) {
    printf(usage, argv[0],"Could not parse # entries");
    return -1;
  }
    
  printf("thread_count = %d, #entries = %d\n", thread_count, entries);

  pthread_t * threads = stasis_malloc(thread_count, pthread_t);
  int* thread_args = stasis_malloc(thread_count, int);
  for(int i = 0; i < thread_count; i++) { 
    thread_args[i] = i + 1;
    pthread_create(&(threads[i]), 0, worker, &(thread_args[i]));
  }

  t = LH_ENTRY(create)(100);

  pthread_mutex_lock(&startAtOnce);
  while(waiting != thread_count) { 
    pthread_cond_wait(&threadWoke, &startAtOnce);
  }
  pthread_cond_broadcast(&startAtOnceCond);
  pthread_mutex_unlock(&startAtOnce);

  printf("Start now\n"); fflush(0);

  for(int i = 0; i < thread_count; i++) {
    void *j;
    pthread_join(threads[i], &j);
  }

  printf("End now\n"); fflush(0);

  LH_ENTRY(destroy)(t);
  free(threads);
  free(thread_args);


  
  return 0;
}
