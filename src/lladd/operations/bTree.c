#define __USE_GNU
#define _GNU_SOURCE
#include "../latches.h"
#include <lladd/transactional.h>
#include <lladd/hash.h>
#include "../page.h"
#include "../page/slotted.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <lladd/operations/noop.h>
#include <lladd/fifo.h>
#include <lladd/multiplexer.h>
#include "../logger/logMemory.h"
#include <stdio.h>

int HelloWorld(){
  printf("hello");
  return 0;
}
