#include <config.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stasis/transactional.h>
#include <unistd.h>
#include <pthread.h>

int i = 0;
int max_active = 0;

pthread_mutex_t mutex;

//static pthread_mutex_t hash_mutex = PTHREAD_MUTEX_INITIALIZER;
static int count;
static recordid hash;

static void * go (void * arg_ptr) {

  pthread_mutex_lock(&mutex);
  i++;
  if(i > max_active) {
    max_active = i;
  }
  pthread_mutex_unlock(&mutex);

  //  pthread_mutex_lock(&hash_mutex);

  int k = *(int*)arg_ptr;
  int j;
  int xid = Tbegin();

  unsigned int seed = k;

  for(j = 0; j < count ; j++) {
    unsigned int r = rand_r(&seed) % 10000;
    byte * tmp = NULL;
    ThashLookup(xid, hash, (byte*)&r, sizeof(int), &tmp);
    assert(r == *(unsigned int*)tmp);
  }
  //  for(j = k * count; j < (k+1) *(count) ; j++) {
  //   TlogicalHashInsert(xid, hash, &j, sizeof(int), &j, sizeof(int));
    //    printf("(%d)", k);
  //}
  
  Tcommit(xid);
  /*
  for(j = k * count; j < (k+1) *(count) ; j++) {
    int tmp = -100;
    TlogicalHashLookup(xid, hash, &j, sizeof(int), &tmp, sizeof(int));
    assert(j == tmp);
    } */


  //  pthread_mutex_unlock(&hash_mutex);

  pthread_mutex_lock(&mutex);
  i--;
  pthread_mutex_unlock(&mutex);


  return NULL;
}


int main(int argc, char** argv) {

  assert(argc == 3);

  int thread_count = atoi(argv[1]);
  count = atoi(argv[2]);

  /*  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt");*/

  pthread_t * workers = stasis_malloc(thread_count, pthread_t);

  Tinit();
  int xid = Tbegin();
  //  hash = ThashCreate(xid, sizeof(int), sizeof(int));
  hash = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  
  int k;

  for(k = 0; k < 20000; k++) {
    ThashInsert(xid, hash, (byte*)&k, sizeof(int), (byte*)&k, sizeof(int));
  }

  Tcommit(xid);
  
  /* threads have static thread sizes.  Ughh. */
  pthread_attr_t attr;
  pthread_attr_init(&attr);

  pthread_mutex_init(&mutex, NULL);

  pthread_attr_setstacksize (&attr, PTHREAD_STACK_MIN);

  pthread_mutex_lock(&mutex);

  for(k = 0; k < thread_count; k++) {
    int * k_copy = stasis_alloc(int);
    *k_copy = k ;
    pthread_create(&workers[k], &attr, go, k_copy);

  }

  pthread_mutex_unlock(&mutex);


  for(k = 0; k < thread_count; k++) {
    pthread_join(workers[k],NULL);
  }

  Tdeinit();

  printf("Max active at once: %d\n", max_active);

}
