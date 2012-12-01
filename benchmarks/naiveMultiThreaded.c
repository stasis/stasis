#include <stasis/transactional.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
static pthread_mutex_t hash_mutex = PTHREAD_MUTEX_INITIALIZER;
static int count;
static recordid hash;

static void * go (void * arg_ptr) {
  pthread_mutex_lock(&hash_mutex);

  int k = *(int*)arg_ptr;
  int j;
  int xid = Tbegin();

  for(j = k * count; j < (k+1) *(count) ; j++) {
    TnaiveHashInsert(xid, hash, &j, sizeof(int), &j, sizeof(int));
    //    printf("(%d)", k);
  }
  
  Tcommit(xid);
  pthread_mutex_unlock(&hash_mutex);

  return NULL;
}


int main(int argc, char** argv) {

  assert(argc == 3);

  int thread_count = atoi(argv[1]);
  count = atoi(argv[2]);

  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt");

  pthread_t * workers = stasis_malloc(thread_count, pthread_t);

  Tinit();
  int xid = Tbegin();
  hash = TnaiveHashCreate(xid, sizeof(int), sizeof(int));
  
  Tcommit(xid);

  int k;

  for(k = 0; k < thread_count; k++) {
    int * k_copy = stasis_alloc(int);
    *k_copy = k ;
    pthread_create(&workers[k], NULL, go, k_copy);

  }

  for(k = 0; k < thread_count; k++) {
    pthread_join(workers[k],NULL);
  }

  /* Tdeinit() */
}
