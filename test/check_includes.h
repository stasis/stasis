#include <config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "check_impl.h"

void setup (void) {
  remove("logfile.txt");
  remove("storefile.txt");
}

void teardown(void) {
#ifdef LONG_TEST
  system("echo *.txt | grep -v '*' | xargs -n1 -r ls -lh --full-time");
#endif
  setup();
}
