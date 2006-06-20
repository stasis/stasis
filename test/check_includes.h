#include <stdio.h>
#include <stdlib.h>
#include <config.h>

#ifndef HAVE_TCASE_SET_TIMEOUT
#define tcase_set_timeout(x, y) 0
#endif

void setup (void) { 
  remove("logfile.txt");
  remove("storefile.txt");
  remove("blob0_file.txt");
  remove("blob1_file.txt");
}

void teardown(void) {
  system("ls -lh *.txt*");
  setup();
}
