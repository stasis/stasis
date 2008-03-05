#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "dml.h"
#include "algebra.h"
int executeInsert(int xid, recordid tables, char * insert) {
  char * linecopy = strdup(insert+strlen("insert"));
  char * strtoks;
  char * tbl = strtok_r(linecopy," ",&strtoks);
  char * tup = strtok_r(NULL,"\r\n",&strtoks);
  if((!tbl) || (!tup)) {
    printf("parse error\n");
    return 0;
  }
  char * tupcopy = strdup(tup);
  char * key = strtok_r(tupcopy,",",&strtoks);
  char * trunctup = strtok_r(NULL,"\r\n",&strtoks);
  char * table;
  if(!trunctup) {
    trunctup = "";
  }
  int sz = ThashLookup(xid, tables, (byte*)tbl, strlen(tbl)+1, (byte**)&table);
  if(sz == -1) {
    printf("Unknown table %s\n", tbl);
    return 0;
  } else {
    assert(sz == strlen((char*)table)+1);
    recordid tableRid = ridTpl(table);
    //XXX if(debug) { printf("inserted %s=%s into %s\n", key, tup, tbl); }
    ThashInsert(xid, tableRid, (byte*)key, strlen(key)+1,(byte*)trunctup,strlen(trunctup)+1);
  }
  free(tupcopy);
  free(linecopy);
  return 1;
}
int executeDelete(int xid, recordid tables, char * delete) {
  char * linecopy = strdup(delete+strlen("delete"));
  char * strtoks;
  char * tbl = strtok_r(linecopy," ",&strtoks);
  char * tup = strtok_r(NULL,"\r\n",&strtoks);
  if((!tbl) || (!tup)) {
    printf("parse error\n");
    return 0;
  }
  char * table;
  char * tupcopy = strdup(tup);
  char * key = strtok_r(tupcopy,",",&strtoks);
  int sz = ThashLookup(xid, tables, (byte*)tbl, strlen(tbl)+1, (byte**)&table);
  if(sz == -1) {
    printf("Unknown table %s\n", tbl);
    return 0;
  } else {
    assert(sz == strlen((char*)table)+1);
    recordid tableRid = ridTpl(table);
    //    if(debug) { 
      printf("deleted ->%s<- from %s\n", key, tbl);
    ThashRemove(xid, tableRid, (byte*)key, strlen(key)+1);//,(byte*)trunctup,strlen(trunctup)+1);
  }
  free(tupcopy);
  free(linecopy);
  return 1;
}
