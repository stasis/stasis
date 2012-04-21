/*
 * turbine.c
 *
 *  Created on: Aug 31, 2011
 *      Author: sears
 */
#include <assert.h>
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/latches.h>
#include <stasis/util/time.h>

int many_handles = 1;

typedef struct worker {
  int fd;
  const char * filename;
  uint64_t stride;
  uint64_t offset;
  uint64_t last;
  uint64_t write_size;
  pthread_cond_t cond;
  pthread_mutex_t mutex;
  struct worker * next;
} worker;
/**
 * Straightforward implementation:
 *  - each worker thread has a mutex and a condition variable.
 *  - we grab our successor's mutex, update it's incoming value, release the mutex, and signal them.
 */
void * func(void * argp) {
  worker * arg = argp;
  void * buf;
#ifdef HAVE_POSIX_MEMALIGN
  posix_memalign(&buf, 512, arg->write_size);
#else
  buf = malloc(2*arg->write_size);
  buf = (void*)(((intptr_t)buf) & ~((intptr_t)arg->write_size-1));
#endif
  memset(buf, 0, arg->write_size);
//  void * buf = calloc(arg->write_size, 1);
  pthread_mutex_lock(&arg->mutex);
  uint64_t offset = 0;
  if(many_handles) {
#ifdef HAVE_O_DSYNC
    arg->fd = open(arg->filename, O_WRONLY|O_DSYNC);
#else
    arg->fd = open(arg->filename, O_WRONLY|O_SYNC);
#endif
    if(arg->fd == -1) {
      perror("Couldn't open file");
      abort();
    }
  }
  while(offset < arg->last) {
    while(arg->offset == 0) {
      pthread_cond_wait(&arg->cond, &arg->mutex);
    }
    offset = arg->offset;
    arg->offset = 0;
//    if(!(offset % 10000)) printf("%lld < %lld\n", (long long)arg->offset, (long long) arg->last);
    pthread_mutex_lock(&arg->next->mutex);
    arg->next->offset = offset + 1;
    pthread_mutex_unlock(&arg->next->mutex);
    pthread_cond_signal(&arg->next->cond);
//    struct timespec slp = stasis_double_to_timespec(0.001);
    int err = pwrite(arg->fd, buf, arg->write_size, arg->stride * offset * arg->write_size);
    if(err == -1) {
      perror("Couldn't write");
      abort();
    }
    assert(err && err == arg->write_size);
  }
  pthread_mutex_unlock(&arg->mutex);
  if(many_handles) {
    close(arg->fd);
  }
  return 0;
}

int main(int argc, char * argv[]) {
  if(argc != 6) {
    printf("usage: %s filename num_workers num_ops write_size stride", argv[0]); fflush(stdout); abort();
  }
  int NUM_WORKERS = atoi(argv[2]);
  uint64_t ops = atoll(argv[3]);
  int write_size = atoi(argv[4]);
  worker * workers = malloc(sizeof(worker) * NUM_WORKERS);
  pthread_t*  threads = malloc(sizeof(pthread_t) * NUM_WORKERS);
  uint64_t stride = atoll(argv[5]);
  int fd = -1;
  if(!many_handles) {
#ifdef HAVE_O_DSYNC
    fd = open(argv[1], O_WRONLY|O_DSYNC); //|O_DIRECT);
#else
    fd = open(argv[1], O_WRONLY|O_SYNC); //|O_DIRECT);
#endif
  }

  for(int i = 0; i < NUM_WORKERS; i++) {
    workers[i].fd = fd;
    workers[i].filename = argv[1];
    workers[i].offset = 0;
    workers[i].last   = ops;
    workers[i].write_size = write_size;
    workers[i].stride = stride;
    pthread_cond_init(&workers[i].cond,0);
    pthread_mutex_init(&workers[i].mutex,0);
    workers[i].next = &workers[(i+1) % NUM_WORKERS];
  }
  for(int i = 0; i < NUM_WORKERS; i++) {
    pthread_create(&threads[i], 0, func, &workers[i]);
  }

  struct timeval start, stop;
  gettimeofday(&start, 0);
  workers[0].offset = 1;
  pthread_cond_signal(&workers[0].cond);

  for(int i = 0; i < NUM_WORKERS; i++) {
    pthread_join(threads[i], 0);
  }
  gettimeofday(&stop, 0);

  double elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop,start));
  double ops_per_sec = ((double)ops) / elapsed;
  printf("%lld ops in %f seconds = %f ops/second, %f speedup, %f mb/sec\n", (long long)ops, elapsed, ops_per_sec, ops_per_sec/ /*166.666*/(/*7200.0*/5400.0/60.0), ((double)ops*write_size)/(1024.0*1024.0*elapsed));
  if(!many_handles) {
//    fdatasync(fd);
    close(fd);
  }

  return 0;
}
