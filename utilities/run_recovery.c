#include <stasis/transactional.h>

int main(void) {
  Tinit();
  Tdeinit();
  return compensation_error();
}
