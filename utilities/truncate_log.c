#include <lladd/transactional.h>
#include "../src/lladd/logger/logWriter.h"
int main(void) {
  Tinit();
  syncLog();
  lsn_t trunc_to = flushedLSN();
  Tdeinit();
  
  openLogWriter();
  truncateLog(trunc_to);

  return compensation_error();
}

