#include <stasis/transactional.h>
#include <stasis/truncation.h>
int main(void) {
  Tinit();
  stasis_truncation_truncate(stasis_log_file, 1);
  Tdeinit();

  return compensation_error();
}
