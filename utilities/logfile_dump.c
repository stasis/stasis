#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>

#include "../src/lladd/logger/logHandle.h"
#include "../src/lladd/logger/logWriter.h"


static char * logEntryToString(const LogEntry * le) {
  char * ret = NULL;

  switch(le->type) {
  case UPDATELOG:
    {
      recordid rid = le->contents.clr.rid;
      asprintf(&ret, "UPDATE\tlsn=%9ld\tprevlsn=%9ld\txid=%4d\trid={%8d %5d %5lld}\tfuncId=%3d\targSize=%9d\n", le->LSN, le->prevLSN, le->xid, 
	       rid.page, rid.slot, rid.size, le->contents.update.funcID, le->contents.update.argSize );
      
    }
    break;
  case XBEGIN:
    {
      asprintf(&ret, "BEGIN\tlsn=%9ld\tprevlsn=%9ld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case XCOMMIT:
    {
      asprintf(&ret, "COMMIT\tlsn=%9ld\tprevlsn=%9ld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);

    }
    break;
  case XABORT:
    {
      asprintf(&ret, "ABORT\tlsn=%9ld\tprevlsn=%9ld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);

    }
    break;
  case XEND:
    {
      asprintf(&ret, "END  \tlsn=%9ld\tprevlsn=%9ld\txid=%4d\n", le->LSN, le->prevLSN, le->xid);
    }
    break;
  case CLRLOG:
    {
      recordid rid = le->contents.clr.rid;
      asprintf(&ret, "CLR   \tlsn=%9ld\tprevlsn=%9ld\txid=%4d\trid={%8d %5d %5lld}\tthisUpdateLSN=%9ld\tundoNextLSN=%9ld\n", le->LSN, le->prevLSN, le->xid, 
	       rid.page, rid.slot, rid.size, (long int)le->contents.clr.thisUpdateLSN, (long int)le->contents.clr.undoNextLSN );
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
