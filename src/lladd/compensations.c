#include <lladd/compensations.h>

int ___compensation_count___ = 0;

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
static pthread_key_t error_key;

void compensations_init () {
  int ret = pthread_key_create(&error_key, NULL);
  assert(!ret); 
  pthread_setspecific(error_key, NULL); 
}

void compensations_deinit() {
  int ret = pthread_key_delete(error_key);
  assert(!ret);
}

int compensation_error() {
  int error = (int) pthread_getspecific(error_key);
  return error;
}

void compensation_clear_error() {
  compensation_set_error(0);
}

void compensation_set_error(int error) {
  int ret = pthread_setspecific(error_key, (void *)error);
  if(ret) {
    printf("Unhandled error: %s\n", strerror(ret));
    abort();
  }
  assert(!ret);
}
