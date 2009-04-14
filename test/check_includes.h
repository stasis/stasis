#include <check.h>
#include <config.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef HAVE_TCASE_SET_TIMEOUT
#define tcase_set_timeout(x, y) 0
#endif

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
