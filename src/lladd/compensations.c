#include <lladd/compensations.h>
#include <assert.h>
pthread_key_t error_key;

void compensations_init () {
  int ret = pthread_key_create(&error_key, NULL);
  assert(!ret); 
  pthread_setspecific(error_key, NULL);
}

void compensations_deinit() {
  pthread_key_delete(error_key);
}

int compensation_error() {
  int error = (int) pthread_getspecific(error_key);
  return error;
}

void compensation_clear_error() {
  compensation_set_error(0);
}

void compensation_set_error(int error) {
  pthread_setspecific(error_key, (void *)error);
}
