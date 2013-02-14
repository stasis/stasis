#include <config.h>
#include <stasis/common.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <stasis/operations.h>
#include <stasis/logger/logHandle.h>
#include <stasis/logger/logger2.h>
#include <stasis/logger/safeWrites.h>
#include <stasis/flags.h>

static char * logEntryToString(const LogEntry * le) {
  char * ret = NULL;
  int err = -1;
  switch(le->type) {
  case UPDATELOG:
    {

      err = asprintf(&ret, "UPDATE\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\tpage={%8lld}\tfuncId=%3d\targSize=%9lld\n", le->LSN, le->prevLSN, le->xid,
	       le->update.page, le->update.funcID, (long long)le->update.arg_size );
    }
    break;
  case XBEGIN:
    {
      err = asprintf(&ret, "BEGIN\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case XCOMMIT:
    {
      err = asprintf(&ret, "COMMIT\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);

    }
    break;
  case XABORT:
    {
      err = asprintf(&ret, "ABORT\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);

    }
    break;
  case XPREPARE:
    {
      err = asprintf(&ret, "PREPARE\tlsn=%9lld\tprevlsn=%9lld\txid=%4d,reclsn=%9lld\n", le->LSN, le->prevLSN, le->xid, getPrepareRecLSN(le));

    }
    break;
  case XEND:
    {
      err = asprintf(&ret, "END  \tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case CLRLOG:
    {
      err = asprintf(&ret, "CLR   \tlsn=%9lld\tprevlsn=%9lld\txid=%4d\tcompensates={%8lld}\n", le->LSN, le->prevLSN, le->xid,
	       (((const LogEntry*)((const struct __raw_log_entry*)le)+1))->LSN); //((CLRLogEntry*)le)->clr.compensated_lsn);
    }
    break;
  }
  assert(err != -1);
  return ret;
}

int main(void) {
  LogHandle* lh;
  const LogEntry * le;

  stasis_operation_table_init();
  stasis_log_t* log;
  if(NULL == (log = stasis_log_safe_writes_open(stasis_log_file_name,
                                                stasis_log_file_mode,
                                                stasis_log_file_permissions,
                                                stasis_log_softcommit))){
    printf("Couldn't open log.\n");
  }

  lh = getLogHandle(log);

  while((le = nextInLog(lh))) {

    char * s = logEntryToString(le);
    if(s) {
      printf("%s", s);

      free(s);
    }
  }
  freeLogHandle(lh);

}
