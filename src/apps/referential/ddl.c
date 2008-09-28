#include "ddl.h"
#include <stdlib.h>
#include <string.h>
int ReferentialDDL_CreateTable(int xid, ReferentialAlgebra_context_t*context, create *q) {
  recordid newTable = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH); // XXX assumes first col is a string!
  tuple_t ridtpl = tupleRid(newTable);
  tuple_t union_typ_tup = tupleUnion_typ(q->schema);
  assert(union_typ_tup.count);
  assert(union_typ_tup.col[0].int64 == string_typ); // XXX

  ridtpl = tupleCatTuple(ridtpl,union_typ_tup);
  tupleFree(union_typ_tup);

  size_t tplLen;
  byte * tplBytes = byteTuple(ridtpl,&tplLen);

  ThashInsert(xid,context->hash,(byte*)q->tbl,strlen(q->tbl)+1,tplBytes,tplLen);
  free(tplBytes);
  return 1;
}
