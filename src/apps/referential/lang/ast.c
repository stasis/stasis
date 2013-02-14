#include <config.h>
#include <stasis/common.h>
#include <stdio.h>

#include <stdlib.h>
#include <string.h>
#include "ast.h"

/*
int walk(const expr_list * el) {
  return walk_expr_list(el);
}

#define WALK_LIST(type, child_type)		 \
int walk_##type(const type * x) {		 \
  int ret = 0;					 \
  for(int i = 0; i < x->count; i++) {		 \
    ret |= walk_##child_type(x->ents[i]);	 \
  }                                              \
  return ret;                                    \
}

#define WALK_LIST_DECL(type)                     \
  int walk_##type(const type * x);

#define WALK_LEAF(type)                          \
int walk_##type(const type * x) {                \
 return 0;					 \
}

#define WALK_LEAF_DECL(type)                     \
int walk_##type(const type * x);

WALK_LIST_DECL(expr_list)
WALK_LEAF_DECL(expr)

WALK_LIST(expr_list, expr)
WALK_LEAF(expr)
*/
char * astrncat(char * dest, const char * src) {
  size_t destlen = dest ? strlen(dest) : 0;
  size_t srclen = strlen(src);
  dest = realloc(dest, destlen+srclen+1);
  if(srclen && !destlen) { dest[0] = 0; }
  /*  printf("src =>%s<=",src); */
  strcat(dest, src);
  return dest;
}
char * afstrncat(char *dest, char * src) {
  dest = astrncat(dest, src);
  free(src);
  return dest;
}


char * pp_expr_list(const expr_list* el) {
  char * ret = 0;
  int i;
  for(i = 0; i < el->count; i++) {
    ret = afstrncat(ret, pp_expr(el->ents[i]));
    ret = astrncat(ret, "\n");
  }
  return ret;
}
char * pp_expr(const expr* e) {
  char * ret = 0;
  switch(e->typ) {
  case query_typ: {
    ret = astrncat(ret, "query ");
    ret = afstrncat(ret, pp_union_qry(e->u.q));
  } break;
  case insert_typ: {
    ret = astrncat(ret, "insert ");
    ret = afstrncat(ret, pp_insert(e->u.i));
  } break;
  case delete_typ: {
    ret = astrncat(ret, "delete ");
    ret = afstrncat(ret, pp_delete(e->u.d));
  } break;
  case create_typ: {
    ret = astrncat(ret, "create ");
    ret = afstrncat(ret, pp_create(e->u.c));
  } break;
  default: abort();
  }
  return ret;
}
char * pp_query(const query* q) {
  char * ret = 0;
  switch(q->typ) {
  case scan_typ: {
    ret = afstrncat(ret, pp_q_scan(q->u.scn));
  } break;
  case table_star_typ: {
    ret = astrncat(ret, "*");
  } break;
  case select_typ: {
    ret = astrncat(ret, "{s ");
    ret = afstrncat(ret, pp_union_cmp(q->u.sel->t));
    ret = astrncat(ret, " ");
    ret = afstrncat(ret, pp_union_qry(q->u.sel->q));
    ret = astrncat(ret, "}");
  } break;
  case project_typ: {
    ret = astrncat(ret, "{p ");
    ret = afstrncat(ret, pp_col_tuple(q->u.prj->t));
    ret = astrncat(ret, " ");
    ret = afstrncat(ret, pp_union_qry(q->u.prj->q));
    ret = astrncat(ret, "}");
  } break;
  case join_typ: {
    ret = astrncat(ret, "{j ");
    ret = afstrncat(ret, pp_cmp_tuple(q->u.jn->t));
    ret = astrncat(ret, " ");
    ret = afstrncat(ret, pp_union_qry(q->u.jn->lhs));
    ret = astrncat(ret, " ");
    ret = afstrncat(ret, pp_union_qry(q->u.jn->rhs));
    ret = astrncat(ret, "}");
  } break;
  default: abort();
  }
  return ret;
}
char * pp_create(const create * c){
  char * ret = 0;
  ret = astrncat(ret, c->tbl);
  ret = astrncat(ret, " ");
  ret = astrncat(ret, pp_union_typ(c->schema)); 
  return ret;
}
char * pp_q_scan(const q_scan* s) {
  char * ret = 0;
  if(-1 == asprintf(&ret, "%s", s->table))
    return 0;
  else
    return ret;
}
char * pp_insert(const insert * i) {
  char * ret = 0;
  if(-1 == asprintf(&ret, "%s ", i->tbl)) {
    return 0;
  } else {
    ret = afstrncat(ret, pp_val_tuple(i->t));
    return ret;
  }
}
char * pp_delete(const delete * d) {
  char * ret = 0;
  if(-1 == asprintf(&ret, "%s ", d->tbl)) {
    return 0;
  } else {
    ret = afstrncat(ret, pp_val_tuple(d->t));
  }
  return ret;
}
/*char * pp_pat_tuple(const pat_tuple * p) {
  int i;
  char * ret = 0;
  int first = 1;
  ret = astrncat(ret,"(");
  for(i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,", ");
    } else {
      first = 0;
    }
    ret = afstrncat(ret, pp_pat_entry(p->ents[i]));
  }
  ret = astrncat(ret,")");
  return ret;
  } */

char * pp_val_tuple(const val_tuple * p) {
  int i;
  char * ret = 0;
  int first = 1;
  ret = astrncat(ret,"(");
  for(i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,", ");
    } else {
      first = 0;
    }
    ret = afstrncat(ret, pp_val_entry(p->ents[i]));
  }
  ret = astrncat(ret,")");
  return ret;
}
char * pp_col_tuple(const col_tuple * p) {
  int i;
  char * ret = 0;
  int first = 1;
  ret = astrncat(ret,"(");
  for(i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,", ");
    } else {
      first = 0;
    }
    ret = afstrncat(ret, pp_col_entry(p->ents[i]));
  }
  ret = astrncat(ret,")");
  return ret;
}
char * pp_cmp_tuple(const cmp_tuple * p) {
  int i;
  char * ret = 0;
  int first = 1;
  ret = astrncat(ret,"(");
  for(i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,", ");
    } else {
      first = 0;
    }
    ret = afstrncat(ret, pp_cmp_entry(p->ents[i]));
  }
  ret = astrncat(ret,")");
  return ret;
}
char * pp_typ_tuple(const typ_tuple * p) {
  int i;
  char * ret = 0;
  int first = 1;
  ret = astrncat(ret,"(");
  for(i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,", ");
    } else {
      first = 0;
    }
    ret = afstrncat(ret, pp_typ_entry(p->ents[i]));
  }
  ret = astrncat(ret,")");
  return ret;
}
char * pp_union_qry(const union_qry * p) {
  char * ret = 0;
  int first = 1;
  for(int i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,"|");
    } else {
      first = 0;
    }
    ret = astrncat(ret, pp_query(p->ents[i]));
  }
  return ret;
}
char * pp_union_typ(const union_typ * p) {
  char * ret = 0;
  int first = 1;
  for(int i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,"|");
    } else {
      first = 0;
    }
    ret = astrncat(ret, pp_typ_tuple(p->ents[i]));
  }
  return ret;
}
char * pp_union_cmp(const union_cmp * p) {
  char * ret = 0;
  int first = 1;
  for(int i = 0; i < p->count; i++) {
    if(!first) {
      ret = astrncat(ret,"|");
    } else {
      first = 0;
    }
    ret = astrncat(ret, pp_cmp_tuple(p->ents[i]));
  }
  return ret;
}
/*char * pp_pat_entry(const pat_entry * p) {
  char * ret = 0;
  switch(p->typ) {
  case star_typ: {
    asprintf(&ret,"*");
  } break;
  case ident_typ: {
    asprintf(&ret,"%s",p->u.ident);
  } break;
  case int_typ: {
    asprintf(&ret,"%d",p->u.integ);
  } break;
  case string_typ: {
    asprintf(&ret,"\"%s\"",p->u.str);
  } break;
  default: abort();
  }
  return ret;
  } */
char * pp_val_entry(const val_entry * p) {
  char * ret = 0;
  int err;
  switch(p->typ) {
  case ident_typ: {
    err = asprintf(&ret,"%s",p->u.ident);
  } break;
  case int64_typ: {
    err = asprintf(&ret,"%lld",(long long int)p->u.integ);
  } break;
  case string_typ: {
    err = asprintf(&ret,"\"%s\"",p->u.str);
  } break;
  default: abort();
  }
  if(err == -1)
    return 0;
  else
    return ret;
}
char * pp_col_entry(const col_entry * p) {
  char * ret = 0;
  int err;
  switch(p->typ) {
  case colint_typ: {
    err = asprintf(&ret,"$%d",p->u.colnum);
  } break;
  case colstr_typ: {
    err = asprintf(&ret,"$%s",p->u.colstr);
  } break;
  case string_typ: {
    err = asprintf(&ret,"%s",p->u.colstr);
  } break;
  default: abort();
  }
  if(err == -1)
    return 0;
  else
    return ret;
}
char * pp_cmp_entry(const cmp_entry * p) {
  char * ret = 0;
  switch(p->lhs_typ) {
  case col_typ: {
    ret = afstrncat(ret, pp_col_entry(p->lhs.colent));
  } break;
  case val_typ: {
    ret = afstrncat(ret, pp_val_entry(p->lhs.valent));
  } break;
  default: abort();
  }

  switch(p->comparator) {
  case equal_typ: {
    ret = astrncat(ret, "=");
  } break;
  default: abort();
  }

  switch(p->rhs_typ) {
  case col_typ: {
    ret = afstrncat(ret, pp_col_entry(p->rhs.colent));
  } break;
  case string_typ: {
    ret = afstrncat(ret, pp_col_entry(p->rhs.colent));
  } break;
  case val_typ: {
    ret = afstrncat(ret, pp_val_entry(p->rhs.valent));
  } break;
  default: abort();
  }
  return ret;
}
char * pp_typ_entry(const typ_entry * p) {
  char * ret = 0;
  switch(p->typ) {
  case typ_typ: {
    switch(p->u.type) {
    case int64_typ: {
      ret = astrncat(ret, "int");
    } break;
    case string_typ: {
      ret = astrncat(ret, "string");
    } break;
    default: abort();
    }
  } break;
  default: abort();
  }
  return ret;
}
