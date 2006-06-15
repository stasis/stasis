#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <lladd/transactional.h>

int main(int argc, char** argv) {
  Tinit();

  recordid hash = {1, 0, 48};
  char * val;
  if(-1 != ThashLookup(-1, hash, (byte*)argv[1], strlen(argv[1]), (byte**)&val)) { 
    printf("%s\n", val);
    free(val);
  }
  Tdeinit();
  return 0;
}
