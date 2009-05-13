#include <stasis/transactional.h>
#include <stasis/truncation.h>
int main(void) {
  Tinit();
  TtruncateLog();
  Tdeinit();

  return compensation_error();
}
