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
      recordid rid = le->update.rid;
      asprintf(&ret, "UPDATE\tlsn=%9lld\tprevlsn=%9lld\txid=%4d\trid={%8d %5d %5lld}\tfuncId=%3d\targSize=%9d\n", le->LSN, le->prevLSN, le->xid, 
	       rid.page, rid.slot, (long long int)rid.size, le->update.funcID, le->update.argSize );
      
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
  case XEND:
    {
      asprintf(&ret, "END  \tlsn=%9lld\tprevlsn=%9lld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case CLRLOG:
    {
      recordid rid = le->update.rid;
      asprintf(&ret, "CLR   \tlsn=%9lld\tprevlsn=%9lld\txid=%4d\trid={%8d %5d %5lld}\n", le->LSN, le->prevLSN, le->xid, 
	       rid.page, rid.slot, (long long int) rid.size );
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
