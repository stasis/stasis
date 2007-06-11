#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <stasis/transactional.h>

int main(int argc, char** argv) {
  Tinit();

  recordid hash = {1, 0, 48};
  byte * val;
  byte ** argvb = (byte**) argv;
  if(-1 != ThashLookup(-1, hash, argvb[1], strlen(argv[1]), &val)) { 
    printf("%s\n", (char*)val);
    free(val);
  }
  Tdeinit();
  return 0;
}
