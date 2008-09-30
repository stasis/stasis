#ifndef _REFER_AST_H
#define _REFER_AST_H
#include <stdint.h>

typedef struct expr_list expr_list;
typedef struct expr expr;
typedef struct query query;
typedef struct insert insert;
typedef struct delete delete;
typedef struct create create;

typedef struct q_scan q_scan;
typedef struct q_select q_select;
typedef struct q_project q_project;
typedef struct q_join q_join;

//typedef struct pat_tuple pat_tuple;
typedef struct cmp_tuple cmp_tuple;
typedef struct col_tuple col_tuple;
typedef struct val_tuple val_tuple;
typedef struct typ_tuple typ_tuple;

typedef struct union_typ union_typ;
typedef struct union_cmp union_cmp;
typedef struct union_qry union_qry;

//typedef struct pat_entry pat_entry;
typedef struct cmp_entry cmp_entry;
typedef struct col_entry col_entry;
typedef struct val_entry val_entry;
typedef struct typ_entry typ_entry;

typedef char* table;
typedef char* ident;

typedef int    val_ent_wildcard;
typedef char*  val_ent_ident;
typedef int64_t val_ent_int;
typedef char*  val_ent_string;

#include "../tuple.h" // for datatypes.
/*typedef enum {
  integer_data_typ,
  string_data_typ
  } val_ent_typ;*/
typedef int    val_col_int;
typedef char*  val_col_string;

struct expr_list {
  int count;
  expr     ** ents;
};
char * pp_expr_list(const expr_list*);
struct expr {
  enum expr_typ {
    query_typ,
    insert_typ,
    delete_typ,
    create_typ
  } typ;
  union {
    union_qry  * q;
    insert * i;
    delete * d;
    create * c;
  } u;
};
char * pp_expr(const expr*);

struct query {
  enum query_typ {
    scan_typ,
    table_star_typ,
    select_typ,
    project_typ,
    join_typ
  } typ;
  union {
    q_scan   * scn;
    q_select * sel;
    q_project* prj;
    q_join   * jn;
  } u;
};
char * pp_query(const query*);
struct create {
  char * tbl;
  union_typ * schema;
};
char * pp_create(const create*);

struct q_scan {
  char * table;
};
char * pp_q_scan(const q_scan*);

struct q_select {
  union_cmp * t;
  union_qry * q;
};
//char * pp_q_select(const q_select*);

struct q_project {
  col_tuple * t;
  union_qry * q;
};
//char * pp_q_project(const q_project*);

struct q_join {
  cmp_tuple * t;
  union_qry * lhs;
  union_qry * rhs;
};
//char * pp_q_join(const q_join*);

struct insert {
  table      tbl;
  val_tuple  * t;
};
char * pp_insert(const insert*);

struct delete {
  table      tbl;
  val_tuple  * t;
};
char * pp_delete(const delete*);

enum cmp_typ {
  equal_typ
};

/*struct pat_tuple {
  int count;
  pat_entry ** ents; // XXX tuple type?
};
char * pp_pat_tuple(const pat_tuple*); */

struct val_tuple {
  int count;
  val_entry ** ents;
};
char * pp_val_tuple(const val_tuple*);

struct col_tuple {
  int count;
  col_entry ** ents;
};
char * pp_col_tuple(const col_tuple*);
struct cmp_tuple {
  int count;
  cmp_entry ** ents;
};
char * pp_cmp_tuple(const cmp_tuple*);
struct typ_tuple {
  int count;
  typ_entry ** ents;
};
char * pp_typ_tuple(const typ_tuple*);
/*struct pat_entry {
  enum tup_entry_typ typ;
  union {
    val_ent_wildcard wc;
    val_ent_ident    ident;
    val_ent_int      integ;
    val_ent_string   str;
  } u;
};
char * pp_pat_entry(const pat_entry*); */
struct val_entry {
  datatype_t typ;
  union {
    val_ent_ident  ident;
    val_ent_int    integ;
    val_ent_string str;
  } u;
};
char * pp_val_entry(const val_entry*);
struct col_entry {
  datatype_t typ;
  union {
    val_col_string colstr;
    val_col_int    colnum;
  } u;
};
char * pp_col_entry(const col_entry*);
struct typ_entry {
  datatype_t typ;
  union {
    datatype_t type;
  } u;
};
char * pp_typ_entry(const typ_entry*);

struct union_typ {
  int count;
  typ_tuple ** ents;
};
char * pp_union_typ(const union_typ*);
struct union_cmp {
  int count;
  cmp_tuple ** ents;
};
char * pp_union_cmp(const union_cmp*);
struct union_qry {
  int count;
  query ** ents;
};
char * pp_union_qry(const union_qry*);

enum cmp_side_typ {
  col_typ,
  val_typ
};
struct cmp_entry {
  enum cmp_side_typ lhs_typ;
  enum cmp_side_typ rhs_typ;
  enum cmp_typ comparator;
  union {
    col_entry * colent;
    val_entry * valent;
  } lhs;
  union {
    col_entry * colent;
    val_entry * valent;
  } rhs;
};
char * pp_cmp_entry(const cmp_entry*);


//int yyparse();
extern expr_list * results;
void    parse(char *buf, expr_list **result);

char * astrncat(char *,const char *);
char * afstrncat(char *,char *);

/*struct table {
  char * name;
};
char * pp_table(table*); */

/*struct ident {
  char * name;
};
char * pp_ident(ident*); */
#endif// _REFER_AST_H
