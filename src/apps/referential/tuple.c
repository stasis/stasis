#include <config.h>
#include <stasis/common.h>

#include <errno.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
//#include "old_algebra.h"
#include "tuple.h"
#include "lang/ast.h"

/*char * tplRid(recordid rid) {
  char * ret;
  asprintf(&ret, "%d,%d,%lld", rid.page, rid.slot, (long long)rid.size);
  return ret;
}
recordid ridTpl(char * tpl) {
  int count;
  char * freeme;
  char ** tok = split(tpl, &freeme, &count, ", ");
  recordid ret;
  errno = 0;
  assert(count == 3);
  ret.page = strtol(tok[0],NULL,10);
  ret.slot = strtol(tok[1],NULL,10);
  ret.size = strtol(tok[2],NULL,10);
  if(errno) {
    perror("Couldn't parse rid");
    abort();
  }
  free(freeme);
  free(tok);
  return ret;
}

char ** tplDup(char ** tup) {
  int i = 0;
  for(; tup[i]; i++) { } // i = num non-null entries
  char ** ret = malloc(sizeof(char**) * (i+1));
  ret[i] = 0;
  while(i) {
    i--;
    ret[i] = strdup(tup[i]);
  }
  return ret;
}
void tplFree(char ** tup) {
  for(int i = 0; tup[i]; i++) {
    free(tup[i]);
  }
  free(tup);
  }*/

tuple_t tupleRid(recordid rid) {
  tuple_t ret;
  ret.count = 3;
  ret.type = malloc(sizeof(datatype_t)*3);
  ret.col = malloc(sizeof(tuplecol_t)*3);
  ret.type[0] = int64_typ;
  ret.type[1] = int64_typ;
  ret.type[2] = int64_typ;
  ret.col[0].int64 = rid.page;
  ret.col[1].int64 = rid.slot;
  ret.col[2].int64 = rid.size;
  return ret;
}
recordid ridTuple(tuple_t tup) {
  recordid ret = { tup.col[0].int64, tup.col[1].int64, tup.col[2].int64 };
  return ret;
}
tuple_t tupleDup(tuple_t tup) {
  tuple_t ret;
  ret.count = tup.count;
  ret.type = malloc(sizeof(*ret.type)*tup.count);
  ret.col = malloc(sizeof(*ret.col)*tup.count);
  memcpy(ret.type, tup.type, sizeof(*ret.type)*tup.count);
  for(col_t i = 0; i < tup.count; i++) {
    switch(ret.type[i]) {
    case int64_typ: {
      ret.col[i].int64 = tup.col[i].int64;
    } break;
    case string_typ: {
      ret.col[i].string = strdup(tup.col[i].string);
    } break;
    default: abort();
    }
  }
  return ret;
}
void tupleFree(tuple_t tup) {
  for(col_t i = 0; i < tup.count; i++) {
    switch(tup.type[i]) {
    case int64_typ: {
      // nop
    } break;
    case string_typ: {
      free(tup.col[i].string);
    } break;
    default: abort();
    }
  }
  if(tup.col) { free(tup.col); }
  if(tup.type) { free(tup.type); }
}

tuple_t tupleAlloc(void) {
  tuple_t ret;
  ret.count = 0;
  ret.type = 0;
  ret.col = 0;
  return ret;
}

tuple_t tupleCatInt64(tuple_t t, int64_t i) {
  t.count++;
  t.type = stasis_realloc(t.type,t.count, datatype_t);
  t.col = stasis_realloc(t.col, t.count, tuplecol_t);
  t.type[t.count-1] = int64_typ;
  t.col[t.count-1].int64 = i;
  return t;
}
tuple_t tupleCatString(tuple_t t, const char * str) {
  t.count++;
  t.type = stasis_realloc(t.type,t.count,datatype_t);
  t.col = stasis_realloc(t.col,t.count,tuplecol_t);
  t.type[t.count-1] = string_typ;
  t.col[t.count-1].string = strdup(str);
  return t;
}
tuple_t tupleCatCol(tuple_t t, tuple_t src, col_t i) {
  if(src.type[i] == string_typ) {
    t = tupleCatString(t, src.col[i].string);
  } else if (src.type[i] == int64_typ) {
    t = tupleCatInt64(t, src.col[i].int64);
  }
  return t;
}
tuple_t tupleCatTuple(tuple_t t, tuple_t src) {
  for(int i = 0; i < src.count; i++) {
    t = tupleCatCol(t, src, i);
  }
  return t;
}

tuple_t tupleTail(tuple_t src, col_t start_pos) {
  tuple_t t = tupleAlloc();
  for(int i = start_pos; i < src.count; i++) {
    t = tupleCatCol(t, src, i);
  }
  return t;
}

byte * byteTuple(tuple_t t,size_t* s) {
  byte * b;
  *s = sizeof(col_t) + t.count * (sizeof(datatype_t));
  for(col_t i = 0; i<t.count; i++) {
    (*s)+= (t.type[i] == int64_typ ? sizeof(int64_t)
	                        : (strlen(t.col[i].string)+1));
  }
  b = malloc(*s);
  byte * next_b = b;
  (*(col_t*)next_b) = t.count;
  next_b += sizeof(col_t);
  size_t typlen = sizeof(datatype_t)*t.count;
  memcpy(next_b,t.type,typlen);
  next_b += typlen;
  for(col_t i = 0; i<t.count; i++) {
    if(t.type[i] == int64_typ) {
      *(int64_t*)next_b = t.col[i].int64;
      next_b += sizeof(int64_t);
    } else {
      assert(t.type[i] == string_typ);
      size_t len = strlen(t.col[i].string)+1;
      memcpy(next_b, t.col[i].string, len);
      next_b += len;
    }
  }
  return b;
}

char * stringTuple(tuple_t t) {
  char * ret = astrncat(0,"(");
  for(int i = 0; i < t.count; i++) {
    if(i) { ret = astrncat(ret, ","); }
    if(t.type[i] == int64_typ) {
      char * tok;
      int err = asprintf(&tok,"%lld",(long long)t.col[i].int64);
      assert(err != -1);
      ret = afstrncat(ret,tok);
    } else if(t.type[i] == string_typ) {
      ret = astrncat(ret,t.col[i].string);
    } else {
      abort();
    }
  }
  ret = astrncat(ret,")");
  return ret;
}

tuple_t tupleByte(byte* b) {
  tuple_t t;
  t.count = *(col_t*)b;
  //  printf("deserialized size: %d\n", t.count);
  byte * b_next = b;
  b_next += sizeof(col_t);
  size_t typelen = sizeof(datatype_t)*t.count;
  t.type = malloc(typelen);
  memcpy(t.type, b_next, typelen);
  b_next+=typelen;
  t.col = malloc(sizeof(tuplecol_t)*t.count);
  for(col_t i = 0; i < t.count; i++) {
    if(t.type[i] == int64_typ) {
      t.col[i].int64 = *(int64_t*)b_next;
      b_next += sizeof(int64_t);
    } else {
      size_t len = strlen((char*)b_next)+1;
      t.col[i].string = malloc(len);
      strcpy(t.col[i].string,(char*)b_next);
      b_next += len;
    }
  }
  return t;
}

tuple_t tupleVal_tuple(val_tuple * v) {
  tuple_t t = tupleAlloc();

  for(int i = 0; i < v->count; i++) {
    if(v->ents[i]->typ == string_typ) {
      t = tupleCatString(t, v->ents[i]->u.str);
    } else if(v->ents[i]->typ == int64_typ) {
      t = tupleCatInt64(t, v->ents[i]->u.integ);
    } else if(v->ents[i]->typ == ident_typ) {
      abort();
    } else {
      abort();
    }
  }
  return t;
}
tuple_t tupleUnion_typ(union_typ * v) {
  assert(v->count == 1);
  tuple_t t = tupleAlloc();
  typ_tuple * tt = v->ents[0];
  for(int i = 0; i < tt->count; i++) {
    assert(tt->ents[i]->typ == typ_typ);
    t = tupleCatInt64(t, tt->ents[i]->u.type);
  }
  return t;
}
