#include <stasis/transactional.h>
#include <stasis/truncation.h>
int main(void) {
  Tinit();
  truncateNow();
  Tdeinit();

  return compensation_error();
}
