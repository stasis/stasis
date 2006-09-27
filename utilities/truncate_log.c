#include <lladd/transactional.h>
#include <lladd/logger/logger2.h>
#include <lladd/truncation.h>
int main(void) {
  Tinit();
  //  syncLog();
  //  lsn_t trunc_to = LogFlushedLSN();
  truncateNow();
  Tdeinit();
  
  //  openLogWriter();
  //  truncateLog(trunc_to);

  return compensation_error();
}

