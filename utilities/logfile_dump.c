#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>

#include <stasis/logger/logHandle.h>
#include <stasis/logger/logWriter.h>


static char * logEntryToString(const LogEntry * le) {
  char * ret = NULL;

  switch(le->type) {
  case UPDATELOG:
    {

      asprintf(&ret, "UPDATE\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\tpage={%8lld}\tfuncId=%3d\targSize=%9lld\n", le->LSN, le->prevLSN, le->xid, 
	       le->update.page, le->update.funcID, (long long)le->update.arg_size );
      
    }
    break;
  case XBEGIN:
    {
      asprintf(&ret, "BEGIN\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case XCOMMIT:
    {
      asprintf(&ret, "COMMIT\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);

    }
    break;
  case XABORT:
    {
      asprintf(&ret, "ABORT\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);

    }
    break;
  case XPREPARE:
    {
      asprintf(&ret, "PREPARE\tlsn=%9lld\tprevlsn=%9lld\txid=%4d,reclsn=%9lld\n", le->LSN, le->prevLSN, le->xid, getPrepareRecLSN(le));

    }
    break;
  case XEND:
    {
      asprintf(&ret, "END  \tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case CLRLOG:
    {
      asprintf(&ret, "CLR   \tlsn=%9lld\tprevlsn=%9lld\txid=%4d\tcompensates={%8lld}\n", le->LSN, le->prevLSN, le->xid, 
	       ((CLRLogEntry*)le)->clr.compensated_lsn);
    }
    break;
  }
  return ret;
}

void setupOperationsTable();

int main() {
  LogHandle lh;
  const LogEntry * le;

  setupOperationsTable();

  if(openLogWriter()) {
    printf("Couldn't open log.\n");
  }

  lh = getLogHandle(); /*LSNHandle(0); */

  while((le = nextInLog(&lh))) {

    char * s = logEntryToString(le);
    if(s) {
      printf("%s", s);

      free(s);
    }
    FreeLogEntry(le);
  }
  

}
