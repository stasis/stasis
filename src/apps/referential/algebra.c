#define _GNU_SOURCE
#include <stdio.h>
#include <stasis/transactional.h>
#include <string.h>
#include <ctype.h>
#include "algebra.h"
//#include "old_algebra.h"
#include "lang/ast.h"
#include <stdlib.h>
#include <errno.h>
#define SELECT_ITERATOR (USER_DEFINED_ITERATOR+1)
#define PROJECT_ITERATOR (USER_DEFINED_ITERATOR+2)
#define KEYVAL_ITERATOR (USER_DEFINED_ITERATOR+3)
#define JOIN_ITERATOR (USER_DEFINED_ITERATOR+4)
#define KEYVALTUP_ITERATOR (USER_DEFINED_ITERATOR+3)

char ** split(char * in, char ** freeme, int* count, char * delim) {
  *freeme = strdup(in);
  *count = 0;
  char * strtoks;
  char * tok = strtok_r(*freeme, delim,&strtoks);
  char ** ret = 0;
  while(tok) {
    (*count)++;
    ret = realloc(ret, sizeof(char*) * *count);
    ret[(*count)-1] = tok;
    tok = strtok_r(NULL,delim,&strtoks);
  }
  ret = realloc(ret, sizeof(char*) * ((*count)+1));
  ret[*count]=0;
  return ret;
}

int isWhitelistChar(char c) {
  return(c == ';'
	 ||
	 c == '/'
	 ||
	 c == '?'
	 ||
	 c == ':'
	 ||
	 c == '@'
	 ||
	 c == '&'
	 ||
	 c == '='
	 ||
	 c == '+'
	 ||
	 c == '$'
	 ||
	 c == '['
	 ||
	 c == ']'
	 ||
	 c == '-'
	 ||
	 c == '_'
	 ||
	 c == '.'
	 ||
	 c == '!'
	 ||
	 c == '~'
	 ||
	 c == '*'
	 ||
	 c == '\''
	 ||
	 c == '%'
	 ||
	 c == '\\');
}



/* static void ts_close(int xid, void * it) {

}
static int ts_next(int xid, void * it) {

}
static int ts_tryNext(int xid, void * it) {

}
static int ts_key(int xid, void * it, byte ** key) {

}
static int ts_value(int xid, void * it, byte ** val) {

}
static void ts_tupleDone(int xid, void * it) {

}
static void ts_releaseLock(int xid, void *it) {

} */
/*static const lladdIterator_def_t ts_it = {
  ts_close, ts_next, ts_tnext, ts_key, ts_value, ts_tupleDone, noopTupDone
  }; */


static lladdIterator_t* ReferentialAlgebra_KeyValTupIterator(int xid, lladdIterator_t * it, datatype_t typ);

lladdIterator_t* ReferentialAlgebra_OpenTableScanner(int xid, recordid catalog,
						    char * tablename) {
  byte * table;
  size_t sz = ThashLookup(xid, catalog, (byte*)tablename, strlen(tablename)+1, (byte**)&table);
  if(sz == -1) {
    printf("Unknown table %s\n", tablename);
    return 0;
  }
  //  assert(sz == strlen(table)+1);
  /*recordid tableRid = ridTpl(table); */
  tuple_t tpl = tupleByte(table);
  recordid tableRid = ridTuple(tpl);

  assert(tpl.count >=4);
  assert(tpl.type[3] == int64_typ);

  // XXX assumes first column is a string
  lladdIterator_t * it = ReferentialAlgebra_KeyValTupIterator(xid, ThashGenericIterator(xid, tableRid), tpl.col[3].int64);

  tupleFree(tpl);
  free(table);
  return it;
}
typedef struct select_predicate {
  char ** vals;
  int count;
  void * freeme;
} select_predicate;

typedef struct select_impl {
  lladdIterator_t * it;
  union_cmp * p;
} select_impl;

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                KEY VALUE TABLE FORMAT                                      ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct kvt_impl {
  datatype_t keytype;
  tuple_t t;
  lladdIterator_t * it;
} kvt_impl;

static lladdIterator_t* ReferentialAlgebra_KeyValTupIterator(int xid, lladdIterator_t * it, datatype_t typ) {
  kvt_impl * kvt = malloc(sizeof(kvt_impl));
  kvt->keytype = typ;
  kvt->t = tupleAlloc();
  kvt->it = it;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = KEYVALTUP_ITERATOR;
  new_it->impl = kvt;
  return new_it;
}

static void kvt_close(int xid, void * it) {
  kvt_impl * kvt = it;
  Titerator_close(xid, kvt->it);
  tupleFree(kvt->t);
  free(kvt);
}
static int mkKvtTuple(int xid, kvt_impl * kvt) {
    byte * keybytes;
    byte * valbytes;
    Titerator_key(xid,kvt->it,&keybytes);
    Titerator_value(xid,kvt->it,&valbytes);
    kvt->t = tupleAlloc();
    if(kvt->keytype == string_typ) {
      kvt->t = tupleCatString(kvt->t, (char*)keybytes);
    } else if(kvt->keytype == int64_typ) {
      kvt->t = tupleCatInt64(kvt->t, *(int64_t*)keybytes);
    } else {
      abort();
    }
    tuple_t valtuple = tupleByte(valbytes);
    kvt->t = tupleCatTuple(kvt->t, valtuple);
    tupleFree(valtuple);
    return 1;
}

static int kvt_next(int xid, void * it) {
  kvt_impl * kvt = it;
  int ret;
  if((ret=Titerator_next(xid, kvt->it))) {
    tupleFree(kvt->t);
    mkKvtTuple(xid, kvt);
  }
  return ret;
}
static int kvt_tryNext(int xid, void * it) {
  kvt_impl * kvt = it;
  //  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  int ret;
  if((ret = Titerator_tryNext(xid, kvt->it))) {
    tupleFree(kvt->t);
    mkKvtTuple(xid,kvt);
  }
  return ret;
}
static int kvt_key(int xid, void * it, byte ** key) {
  kvt_impl * kvt = it;
  *key = (byte*)&kvt->t;
  return 1;
}
static int kvt_value(int xid, void * it, byte ** val) {
  kvt_impl * kvt = it;
  *val = (byte*)&kvt->t;
  return 1;
}
static void kvt_tupleDone(int xid, void * it) {
  kvt_impl * kvt = it;
  Titerator_tupleDone(xid, kvt->it);
}
static void kvt_releaseLock(int xid, void *it) {
  kvt_impl * kvt = it;
  Titerator_releaseLock(xid, kvt->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                KEY VALUE TABLE FORMAT (obsolete version)                   ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct kv_impl {
  char * catted;
  lladdIterator_t * it;
} kv_impl;

lladdIterator_t* ReferentialAlgebra_KeyValCat(int xid, lladdIterator_t * it) {
  kv_impl * kv = malloc(sizeof(kv_impl));
  kv->catted = 0;
  kv->it = it;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = KEYVAL_ITERATOR;
  new_it->impl = kv;
  return new_it;
}

static void kv_close(int xid, void * it) {
  kv_impl * kv = it;
  Titerator_close(xid, kv->it);
  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  free(kv);
}
static int kv_next(int xid, void * it) {
  kv_impl * kv = it;
  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  return Titerator_next(xid, kv->it);
}
static int kv_tryNext(int xid, void * it) {
  kv_impl * kv = it;
  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  return Titerator_tryNext(xid, kv->it);
}
static int mkCatted(int xid, kv_impl * kv) {
  byte * key = 0;
  byte * val = 0;
  Titerator_key(xid, kv->it, (byte**)&key);
  Titerator_value(xid, kv->it, (byte**)&val);

  if(!strlen((char*)val)) {
    kv->catted = strdup((char*)key);
  } else {
    kv->catted = malloc(strlen((char*)key) + 1 + strlen((char*)val) + 1);
    kv->catted[0]=0;
    strcat(strcat(strcat(kv->catted,(char*)key),","),(char*)val);
  }
  return(strlen(kv->catted));
}
static int kv_key(int xid, void * it, byte ** key) {
  kv_impl * kv = it;
  if(!kv->catted) {
    int ret = mkCatted(xid, kv);
    *key = (byte*)kv->catted;
    return ret;
  } else {
    *key = (byte*)kv->catted;
    return(strlen(kv->catted)+1);
  }
}
static int kv_value(int xid, void * it, byte ** val) {
  kv_impl * kv = it;
  if(!kv->catted) {
    int ret = mkCatted(xid, kv);
    *val = (byte*)kv->catted;
    return ret;
  } else {
    *val = (byte*)kv->catted;
    return strlen(kv->catted)+1;
  }
}
static void kv_tupleDone(int xid, void * it) {
  kv_impl * kv = it;
  Titerator_tupleDone(xid, kv->it);
}
static void kv_releaseLock(int xid, void *it) {
  kv_impl * kv = it;
  Titerator_releaseLock(xid, kv->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                SELECT                                                      ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////
lladdIterator_t* ReferentialAlgebra_Select(int xid, lladdIterator_t * it, union_cmp * pred) {
  if(!it) return 0;
  select_impl * s = malloc(sizeof(select_impl));
  s->p = pred;
  s->it = it;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = SELECT_ITERATOR;
  new_it->impl = s;
  return new_it;
}

char * strtokempt(char *str, const char * delim, char ** saveptr, int * extra) {
  char * ret;
  if(str) {
    *saveptr = str;
  }
  ret = *saveptr;

  if(!(**saveptr)) {
    if(*extra) {
      *extra = 0;
      return ret;
    } else {
      return 0;
    }
  }
  do {
    for(int i = 0; delim[i]; i++) {
      if(**saveptr == delim[i]) {
	// terminate returned string
	**saveptr = 0;
	// consume deliminator character from last time.
	(*saveptr)++;
	//printf("extra: %d\n",*extra);
	*extra = 1;
	return ret;
      }
    }
    // consume character we just looked at.
    (*saveptr)++;
  } while(**saveptr);
  //  printf("extra: %d\n",*extra);
  *extra = 0;
  return ret;
}

static int matchUnionCmp(tuple_t tup, union_cmp * cmp) {
  assert(cmp->count == 1); // not implmented

  for(col_t i = 0; i < cmp->ents[0]->count; i++) {
    assert(cmp->ents[0]->ents[i]->comparator == equal_typ); // XXX
    enum cmp_side_typ l = cmp->ents[0]->ents[i]->lhs_typ;
    enum cmp_side_typ r = cmp->ents[0]->ents[i]->rhs_typ;

    if(l == col_typ && r == val_typ) {
      col_entry * lc = cmp->ents[0]->ents[i]->lhs.colent;
      val_entry * rv = cmp->ents[0]->ents[i]->rhs.valent;

      assert(lc->typ == colint_typ); //XXX colstr_typ unimplemented

      // (0) Does the column exist?
      if(tup.count <= lc->u.colnum) { printf("XXX tuple is too short\n"); return 0; }
      // (1) No type coercion.
      if(tup.type[lc->u.colnum] != rv->typ) { return 0; }

      // (2) Enumerate over known types
      if(rv->typ == int64_typ) {
	if(tup.col[lc->u.colnum].int64 != rv->u.integ) { return 0; }
      } else if(rv->typ == string_typ) {
	if(strcmp(tup.col[lc->u.colnum].string, rv->u.str)) { return 0; }
      } else {
	abort();
      }
    } else if (l == val_typ && r == col_typ) {
      col_entry * rc = cmp->ents[0]->ents[i]->rhs.colent;
      val_entry * lv = cmp->ents[0]->ents[i]->lhs.valent;

      assert(rc->typ == colint_typ); //XXX colstr_typ unimplemented

      // (0) Does the column exist?
      if(tup.count <= rc->u.colnum) { printf("XXX tuple is too short\n"); return 0; }

      // (1) No type coercion.
      if(tup.type[rc->u.colnum] != lv->typ) { return 0; }

      // (2) Enumerate over known types
      if(lv->typ == int64_typ) {
	if(tup.col[rc->u.colnum].int64 != lv->u.integ) { return 0; }
      } else if(lv->typ == string_typ) {
	if(strcmp(tup.col[rc->u.colnum].string, lv->u.str)) { return 0; }
      } else {
	abort();
      }
    } else if (l == col_typ && r == col_typ) {
      col_entry * rc = cmp->ents[0]->ents[i]->rhs.colent;
      col_entry * lc = cmp->ents[0]->ents[i]->lhs.colent;
      assert(rc->typ == colint_typ); //XXX unimplemented
      assert(lc->typ == colint_typ); //XXX unimplemented

      // (0) Do the columns exist?
      if(tup.count <= lc->u.colnum || tup.count <= rc->u.colnum) {
	printf("XXX tuple is too short\n");
	return 0;
      }

      // (1) No type coercion
      if(tup.type[rc->u.colnum] != tup.type[lc->u.colnum]) { return 0; }

      // (2) Enumerate over types
      if(tup.type[rc->u.colnum] == int64_typ) {
	if(tup.col[rc->u.colnum].int64 != tup.col[lc->u.colnum].int64) { return 0; }
      } else if(tup.type[rc->u.colnum] == string_typ) {
	if(strcmp(tup.col[rc->u.colnum].string, tup.col[lc->u.colnum].string)) { return 0; }
      } else {
	abort();
      }
    } else if (l == val_typ && r == val_typ) {
      val_entry * rv = cmp->ents[0]->ents[i]->rhs.valent;
      val_entry * lv = cmp->ents[0]->ents[i]->lhs.valent;
      // (0) Don't need length check; not examining tuple
      // (1) No type coercion.
      if(rv->typ != lv->typ) { return 0; }
      // (2) Enumerate over types
      if(rv->typ == int64_typ) {
	if(rv->u.integ != lv->u.integ) { return 0; }
      } else if(rv->typ == string_typ) {
	if(strcmp(rv->u.str, lv->u.str)) { return 0; }
      } else {
	abort();
      }
    } else {
      abort();
    }
  }
  return 1;
}

static void s_close(int xid, void * it) {
  select_impl * impl = it;
  Titerator_close(xid, impl->it);
  //tplFree(impl->p);
  free(impl);
}
static int s_next(int xid, void * it) {
  select_impl * impl = it;
  while(Titerator_next(xid,impl->it)) {
    byte* val;
    Titerator_value(xid, impl->it, &val);
    //    printf("matching w/ %s\n", val);
    if(matchUnionCmp(*(tuple_t*)val, impl->p)) {
      return 1;
    } else {
      Titerator_tupleDone(xid, impl->it);
    }
  }
  return 0;
}
static int s_tryNext(int xid, void * it) {
  abort();
}
static int s_key(int xid, void * it, byte ** key) {
  return Titerator_key(xid, ((select_impl*)it)->it, key);
}
static int s_value(int xid, void * it, byte ** val) {
  return Titerator_value(xid, ((select_impl*)it)->it, val);
}
static void s_tupleDone(int xid, void * it) {
  Titerator_tupleDone(xid, ((select_impl*)it)->it);
}
static void s_releaseLock(int xid, void *it) {
  Titerator_releaseLock(xid, ((select_impl*)it)->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                PROJECT                                                     ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct project_impl {
  int count;
  short * cols;
  lladdIterator_t * it;
  tuple_t tup;
  int haveTup;
} project_impl;

lladdIterator_t* ReferentialAlgebra_Project(int xid, lladdIterator_t * it, col_tuple * project) {
  if(!it) return 0;
  project_impl * p = malloc(sizeof(project_impl));
  p->count = project->count;
  p->cols = malloc(sizeof(short) * p->count);
  p->it = it;
  p->haveTup = 0;
  for(int i = 0; i < p->count; i++) {
    errno = 0;
    assert(project->ents[i]->typ == colint_typ); // XXX
    p->cols[i] = project->ents[i]->u.colnum;
  }
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = PROJECT_ITERATOR;
  new_it->impl = p;
  return new_it;
}

static void p_close(int xid, void * it) {
  project_impl * impl = it;
  Titerator_close(xid, impl->it);
  if(impl->haveTup) { tupleFree(impl->tup); }
  free(impl->cols);
  free(impl);
}
static int p_next(int xid, void * it) {
  project_impl * impl = it;
  if(impl->haveTup) {
    tupleFree(impl->tup);
    impl->haveTup = 0;
  }
  return Titerator_next(xid, impl->it);

}
static int p_tryNext(int xid, void * it) {
  project_impl * impl = it;
  abort();
  return Titerator_tryNext(xid, impl->it);
}
static int p_value(int xid, void * it, byte ** val) ;
static int p_key(int xid, void * it, byte ** key) {
  return p_value(xid, it, key);
}
static int p_value(int xid, void * it, byte ** val) {
  project_impl * impl = it;
  byte * in_val;
  if(impl->haveTup) {
    *val = (byte*)&(impl->tup);
    return 1;
  }
  int ret = Titerator_value(xid,impl->it,&in_val);
  if(ret) {
    impl->tup = tupleAlloc();
    for(col_t i = 0; i < impl->count; i++) {
      impl->tup = tupleCatCol(impl->tup, *(tuple_t*)in_val, impl->cols[i]);
    }
    impl->haveTup = 1;
    *val = (byte*)&(impl->tup);
  }
  return ret;
}
static void p_tupleDone(int xid, void * it) {
  project_impl * impl = it;
  if(impl->haveTup) { tupleFree(impl->tup); impl->haveTup = 0; }
  Titerator_tupleDone(xid,impl->it);
}
static void p_releaseLock(int xid, void *it) {
  project_impl * impl = it;
  Titerator_releaseLock(xid,impl->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                JOIN (naive nested loops)                                   ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct join_impl {
  tuple_t * inner_tpls;
  uint64_t inner_count;
  lladdIterator_t * outer_it;
  lladdIterator_t * inner_it;
  cmp_tuple * pred;
  uint64_t inner_pos;
  int have_outer;
  tuple_t outer_tpl;
  tuple_t t;
} join_impl;

// return the value of a column, given a tuple and cmp->lhs_typ, cmp->lhs
static int resolveColName(/*OUT*/val_col_int *colnum, datatype_t * datatype, int64_t *val_int64, char **val_string,
			  /*IN*/ tuple_t tup, enum cmp_side_typ cmp_typ, col_entry * colent, val_entry * valent) {
  //val_col_int colnum;
  //datatype_t datatype;
  switch(cmp_typ) {
  case col_typ: {
    datatype_t lhs_colreftype = colent->typ;

    switch(lhs_colreftype) {
    case colstr_typ: {
      //      lhs_str = cmp->lhs.colent->u.colstr;
      abort(); // xxx lookup column number in schema
    } break;
    case colint_typ: {
      *colnum = colent->u.colnum;
    } break;
    default: abort(); return 0; //xxx
    }

    if(*colnum >= tup.count) { return 0; }
    *datatype = tup.type[*colnum];

    switch(*datatype) {
    case int64_typ: {
      *val_int64 =  tup.col[*colnum].int64;
    } break;
    case string_typ: {
      *val_string =  tup.col[*colnum].string;
    } break;
    default: abort(); return 0; //xxx
    }

  } break;
  case val_typ: {
    *datatype = valent->typ;
    switch(*datatype) {
    case int64_typ: {
      *val_int64 = valent->u.integ; //tup.col[*colnum].int64;
    } break;
    case string_typ: {
      *val_string = valent->u.str; //tup.col[*colnum].string;
    } break;
    default: abort(); return 0;
    }
  } break;
  default: abort(); return 0;
  }
  return 1;
}

static int matchPredicate(tuple_t tup1, tuple_t tup2, cmp_entry* cmp) {
  datatype_t lhs_datatype;
  val_col_int    lhs_colnum;
  int64_t lhs_int64;
  char* lhs_string;

  datatype_t rhs_datatype;
  val_col_int    rhs_colnum;
  int64_t rhs_int64;
  char* rhs_string;

  if(!resolveColName(&lhs_colnum, &lhs_datatype, &lhs_int64, &lhs_string, tup1, cmp->lhs_typ, cmp->lhs.colent, cmp->lhs.valent)) { printf("Tuple too short\n"); return 0; }
  if(!resolveColName(&rhs_colnum, &rhs_datatype, &rhs_int64, &rhs_string, tup2, cmp->rhs_typ, cmp->rhs.colent, cmp->rhs.valent)) { printf("Tuple too short\n"); return 0; }

  switch(cmp->comparator) {
  case equal_typ: {
    if(lhs_datatype!=rhs_datatype) { return 0; }
    switch(lhs_datatype) {
    case int64_typ: {
      if(lhs_int64==rhs_int64) { return 1; }
    } break;
    case string_typ: {
      if(!strcmp(lhs_string, rhs_string)) { return 1; }
    } break;
    default: abort();
    }
  } break;
  default: abort();
  }
  return 0;
}
static int matchComparator(tuple_t tup1,
			   tuple_t tup2,
			   cmp_tuple * pred) {
  int match = 1;
  for(int i = 0; i < pred->count; i++) {
    if(!matchPredicate(tup1,tup2,pred->ents[i])) {
      //printf("failed on pred %d\n",i);
      match = 0;
      break;
    }
  }
  return match;
}

lladdIterator_t* ReferentialAlgebra_Join(int xid,
					 lladdIterator_t * outer_it,
					 lladdIterator_t * inner_it,
					 cmp_tuple * pred) {
  if(!(outer_it && inner_it)) {
    return 0;
  }

  join_impl * j = malloc(sizeof(join_impl));

  j->inner_it = inner_it;
  j->outer_it = outer_it;
  j->have_outer = 0;
  j->pred = pred;

  j->inner_tpls = calloc(1, sizeof(tuple_t*));
  int i = 0;
  while(Titerator_next(xid, inner_it)) {
    byte * in_val;
    Titerator_value(xid, inner_it, (byte**)&in_val);
    j->inner_tpls = realloc(j->inner_tpls, sizeof(tuple_t)*(i+1));
    j->inner_tpls[i] = tupleDup(*(tuple_t*)in_val);
    Titerator_tupleDone(xid, inner_it);
    i++;
  }
  j->inner_count = i;
  j->inner_pos = 0;
  j->have_outer = 0;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = JOIN_ITERATOR;
  new_it->impl = j;
  return new_it;
}


static void j_close(int xid, void * it) {
  join_impl * j = it;
  Titerator_close(xid,j->outer_it);
  Titerator_close(xid,j->inner_it);
  for(int i = 0; i < j->inner_count; i++) {
    //    tplFree(j->inner_tpls[i]);
    //free(j->freethese[i]);
    //    free(j->inner_strs[i]);
    tupleFree(j->inner_tpls[i]);
  }
  //  tplFree(j->pred);
  free(j->inner_tpls);

  if(j->have_outer) {
    tupleFree(j->outer_tpl);
  }
  // don't free pred; that's the caller's problem.
  free(j);
}
static int j_next(int xid, void * it) {
  join_impl * j = it;
  while(1) {
    //printf("checking %d\n", j->inner_pos);
    if((j->inner_pos == j->inner_count) || (!j->have_outer)) {
      j->inner_pos = 0;
      if(j->have_outer) {
	Titerator_tupleDone(xid, j->outer_it);
	tupleFree(j->t);
      }
      if(Titerator_next(xid, j->outer_it)) {
	byte * this_tpl;
	Titerator_value(xid, j->outer_it, &this_tpl);
	j->outer_tpl = *(tuple_t*)this_tpl;
      } else {
	j->have_outer = 0;
	return 0;
      }
    }
    while(j->inner_pos != j->inner_count) {
      if(matchComparator(j->outer_tpl, j->inner_tpls[j->inner_pos], j->pred)) {
	j->have_outer = 1;
	j->t = tupleAlloc();
	j->t = tupleCatTuple(j->t, j->outer_tpl);
	j->t = tupleCatTuple(j->t, j->inner_tpls[j->inner_pos]);
	j->inner_pos++;
	return 1;
      } else {
	j->have_outer = 0;
	j->inner_pos++;
      }
    }
  }
}
static int j_tryNext(int xid, void * it) {
  abort();
}
static int j_key(int xid, void * it, byte ** key) {
  join_impl * j = it;
  *key = (byte*)&(j->t);
  return 1;
}
static int j_value(int xid, void * it, byte ** val) {
  join_impl * j = it;
  *val = (byte*)&(j->t); //inner_tpls[j->inner_pos-1]);
  return 1;
}
static void j_tupleDone(int xid, void * it) {
}
static void j_releaseLock(int xid, void *it) {
  // noop
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                AST INTERPRETER FUNCTIONS                                   ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

lladdIterator_t* ReferentialAlgebra_ExecuteQuery(int xid,
						 ReferentialAlgebra_context_t* c,
						 union_qry *q) {

  if(q->count != 1) { abort(); } // unimplemented;

  switch(q->ents[0]->typ) {
  case scan_typ: {
    return ReferentialAlgebra_OpenTableScanner(xid, c->hash, q->ents[0]->u.scn->table);
  } break;
  case select_typ: {
    return ReferentialAlgebra_Select(xid,
				     ReferentialAlgebra_ExecuteQuery(xid, c, q->ents[0]->u.sel->q),
				     q->ents[0]->u.sel->t);
  } break;
  case project_typ: {
    return ReferentialAlgebra_Project(xid,
				      ReferentialAlgebra_ExecuteQuery(xid, c, q->ents[0]->u.prj->q),
				      q->ents[0]->u.prj->t);
  } break;
  case join_typ: {
    return ReferentialAlgebra_Join(xid,
				   ReferentialAlgebra_ExecuteQuery(xid, c, q->ents[0]->u.jn->lhs),
				   ReferentialAlgebra_ExecuteQuery(xid, c, q->ents[0]->u.jn->rhs),
				   q->ents[0]->u.jn->t);
  } break;
  default: abort(); // select, project, join, etc unimplemented.
  }

}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                ININTIALIZATION                                             ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

/**
   Initialize module (Must be called before anything else in this file).
 */
void ReferentialAlgebra_init() {
  lladdIterator_def_t select_def = {
    s_close, s_next, s_tryNext, s_key, s_value, s_tupleDone, s_releaseLock
    };
  lladdIterator_register(SELECT_ITERATOR, select_def);
  lladdIterator_def_t project_def = {
    p_close, p_next, p_tryNext, p_key, p_value, p_tupleDone, p_releaseLock
  };
  lladdIterator_register(PROJECT_ITERATOR, project_def);
  lladdIterator_def_t keyval_def = {
    kv_close, kv_next, kv_tryNext, kv_key, kv_value, kv_tupleDone, kv_releaseLock
  };
  lladdIterator_register(KEYVAL_ITERATOR, keyval_def);
  lladdIterator_def_t keyvaltup_def = {
    kvt_close, kvt_next, kvt_tryNext, kvt_key, kvt_value, kvt_tupleDone, kvt_releaseLock
  };
  lladdIterator_register(KEYVALTUP_ITERATOR, keyvaltup_def);
  lladdIterator_def_t j_def = {
    j_close, j_next, j_tryNext, j_key, j_value, j_tupleDone, j_releaseLock
  };
  lladdIterator_register(JOIN_ITERATOR, j_def);
}
/**
 */
ReferentialAlgebra_context_t * ReferentialAlgebra_openContext(int xid, recordid rid) {
  ReferentialAlgebra_context_t * ret = malloc(sizeof(ReferentialAlgebra_context_t));
  ret->hash = rid;

  return ret;
}

recordid ReferentialAlgebra_allocContext(int xid) {
  recordid hash = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  tuple_t tpl = tupleRid(hash);
  tpl = tupleCatInt64(tpl,string_typ);
  tpl = tupleCatInt64(tpl,int64_typ);
  tpl = tupleCatInt64(tpl,int64_typ);
  tpl = tupleCatInt64(tpl,int64_typ);
  tpl = tupleCatInt64(tpl,star_typ);
  size_t tplLen = 0;
  byte * tplBytes = byteTuple(tpl,&tplLen);
  ThashInsert(xid,hash,(byte*)"TABLES",strlen("TABLES")+1,tplBytes,tplLen);
  free(tplBytes);
  return hash;
}
