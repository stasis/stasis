#include "lang/ast.h"

#ifndef TUPLE_H
#define TUPLE_H
#include <stasis/transactional.h>
#include <ctype.h>

typedef uint8_t col_t;

typedef enum {
  star_typ,
  ident_typ,
  int64_typ,
  string_typ,
  colint_typ,
  colstr_typ,
  typ_typ,
} datatype_t;


typedef union {
  int64_t int64;
  char * string;
} tuplecol_t;
typedef struct tuple_t {
  col_t count;
  datatype_t * type;
  tuplecol_t * col;
} tuple_t;

tuple_t tupleRid(recordid rid);
recordid ridTuple(tuple_t tpl);

char * stringTuple(tuple_t);
byte * byteTuple(tuple_t,size_t*);
tuple_t tupleByte(byte*);

tuple_t tupleVal_tuple(val_tuple * t);
tuple_t tupleUnion_typ(union_typ * t);

tuple_t tupleDup(tuple_t tup);
void tupleFree(tuple_t tup);

tuple_t tupleAlloc();

tuple_t tupleCatString(tuple_t t, const char * str);
tuple_t tupleCatInt64(tuple_t t, int64_t str);
tuple_t tupleCatCol(tuple_t t, tuple_t src, col_t col);
tuple_t tupleCatTuple(tuple_t t, tuple_t src);
tuple_t tupleTail(tuple_t t, col_t start_pos);
#endif //TUPLE_H/
