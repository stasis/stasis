#include <stasis/transactional.h>
#include <stasis/truncation.h>
int main(void) {
  Tinit();
  truncateNow(1);
  Tdeinit();

  return compensation_error();
}
