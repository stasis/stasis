#include <lladd/transactional.h>
#include <lladd/truncation.h>
int main(void) {
  Tinit();
  truncateNow();
  Tdeinit();

  return compensation_error();
}

