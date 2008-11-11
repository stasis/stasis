#define _GNU_SOURCE

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <stasis/transactional.h>
#include "dml.h"
#include "algebra.h"
/*
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
*/
static recordid ReferentialDML_lookupTableRid(int xid, ReferentialAlgebra_context_t*context, char * tablename) {
  expr_list * results = 0;
  char * line;
  int err = asprintf(&line, "query {p ($1,$2,$3) {s ($0=\"%s\") TABLES} }", tablename);
  assert(err != -1);

  //XXX memory leak!
  parse(line,&results);

  assert(results);  //XXX
  assert(results->count == 1); //XXX

  lladdIterator_t * it = ReferentialAlgebra_ExecuteQuery(xid, context, results->ents[0]->u.q);
  recordid tableRid;

  if(it) {
    int first = 1;
    while(Titerator_next(xid,it)) {
      assert(first);
      first = 0;
      byte * tup;
      Titerator_value(xid, it, &tup);
      tableRid = ridTuple(*(tuple_t*)tup);
    }
    Titerator_close(xid, it);
  } else {
    abort(); //XXX
  }
  return tableRid;
}

int ReferentialDML_InsertTuple(int xid, ReferentialAlgebra_context_t*context, char * tablename, tuple_t t) {
  //XXX assumes table format is key value, starting with a string.
  tuple_t val = tupleTail(t, 1);
  size_t val_len;
  byte * val_byte = byteTuple(val,&val_len);

  recordid tableRid = ReferentialDML_lookupTableRid(xid,context,tablename);

  if(t.type[0] == string_typ) {
    ThashInsert(xid, tableRid, (byte*)t.col[0].string, 1+strlen(t.col[0].string), val_byte, val_len);
  } else if(t.type[0] == int64_typ) {
    abort(); // int keys not supported yet
    ThashInsert(xid, tableRid, (byte*)&t.col[0].int64, sizeof(int64_t), val_byte, val_len);
  } else {
    abort();
  }

  free(val_byte);
  tupleFree(val);
  return 0;
}
int ReferentialDML_DeleteTuple(int xid, ReferentialAlgebra_context_t*context, char * tablename, tuple_t t) {
  //XXX assumes table format is key value, starting with a string.
  //  tuple_t val = tupleTail(t, 1);
  //  size_t val_len;
  //  byte * val_byte = byteTuple(val,&val_len);

  recordid tableRid = ReferentialDML_lookupTableRid(xid,context,tablename);

  if(t.type[0] == string_typ) {
    ThashRemove(xid, tableRid, (byte*)t.col[0].string, 1+strlen(t.col[0].string)); //, val_byte, val_len);
  } else if(t.type[0] == int64_typ) {
    abort(); // int keys not supported yet
    ThashRemove(xid, tableRid, (byte*)&t.col[0].int64, sizeof(int64_t)); //, val_byte, val_len);
  } else {
    abort();
  }

  //free(val_byte);
  //tupleFree(val);
  return 0;
}

int ReferentialDML_ExecuteInsert(int xid, ReferentialAlgebra_context_t*context, insert *i) {
  tuple_t t = tupleVal_tuple(i->t);
  int ret = ReferentialDML_InsertTuple(xid, context, i->tbl, t);
  tupleFree(t);
  return ret;
}
int ReferentialDML_ExecuteDelete(int xid, ReferentialAlgebra_context_t*context, delete *d) {
  tuple_t t = tupleVal_tuple(d->t);
  int ret = ReferentialDML_DeleteTuple(xid, context, d->tbl, t);
  tupleFree(t);
  return ret;
}
