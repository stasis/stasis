#include <lladd/transactional.h>

int main(void) {
  Tinit();
  Tdeinit();
  return compensation_error();
}

