#define __USE_GNU
#define _GNU_SOURCE
#include <stasis/latches.h>
#include <stasis/transactional.h>
#include <stasis/hash.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stasis/operations/noop.h>
#include <stasis/fifo.h>
#include <stasis/multiplexer.h>
#include <stasis/logger/logMemory.h>
#include <stdio.h>

int HelloWorld(){
  printf("hello");
  return 0;
}
