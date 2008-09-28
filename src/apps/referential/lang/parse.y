%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ast.h"

#include "/home/sears/stasis/src/apps/referential/lang/parser_private.h"


void    parse(char *buf, expr_list **result)
    {
        parse_parm  pp;

        pp.buf = buf;
        pp.length = strlen(buf);
        pp.pos = 0;
        *result = 0;
        yylex_init(&pp.yyscanner);
        yyset_extra(&pp, pp.yyscanner);
        yyparse(&pp, pp.yyscanner);
        *result = pp.result;
        yylex_destroy(pp.yyscanner);
    }

%}

%pure_parser
%parse-param {parse_parm *parm}
%parse-param {void *scanner}
%lex-param {yyscan_t *scanner}

 //%}

%token LPAREN RPAREN LBRACKET RBRACKET
%token COMMA BAR STAR EQUAL
%token SELECT PROJECT JOIN
%token QUERY INSERT DELETE CREATE

/* The reserved words "string" and "int" */
%token STRING_TYPE
%token INT_TYPE



%union {
  val_entry * val_ent;
  col_entry * col_ent;
  cmp_entry * cmp_ent;
  typ_entry * typ_ent;
  val_tuple * val_tup;
  col_tuple * col_tup;
  cmp_tuple * cmp_tup;
  typ_tuple * typ_tup;
  union_typ * union_typ;
  union_cmp * union_cmp;
  union_qry * union_qry;
  insert    * ins_expr;
  delete    * del_expr;
  query     * qry_expr;
  create    * crt_expr;
  expr      * expr;
  expr_list * exprs;
  char * string;
  int    number;
  enum cmp_typ  comparator;
}

%token <string> TABLE
%token <string> IDENT
%token <string> COLNAME
%token <number> COLNUM
%token <string> STRING
%token <number> INT

%type <val_ent> val
%type <col_ent> col
%type <cmp_ent> cmp
%type <typ_ent> typ
%type <comparator> test

%type <val_tup> val_list val_tuple
%type <col_tup> col_list col_tuple
%type <cmp_tup> cmp_list cmp_tuple
%type <typ_tup> typ_list typ_tuple

%type <union_typ> union_typ
%type <union_cmp> union_cmp
%type <union_qry> union_qry

%type <ins_expr> insert
%type <del_expr> delete
%type <qry_expr> query
%type <crt_expr> create
%type <expr>     expr
%type <exprs>    expr_list

%%

expr_list : expr    { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; parm->result = $$; }
          | expr_list expr
	  {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $2;
	  }

expr      : QUERY union_qry {
	    $$ = malloc(sizeof(*$$));
	    $$->typ = query_typ;
	    $$->u.q = $2;
          }
          | INSERT insert {
	    $$ = malloc(sizeof(*$$));
	    $$->typ = insert_typ;
	    $$->u.i = $2;
          }
          | DELETE delete {
	    $$ = malloc(sizeof(*$$));
	    $$->typ = delete_typ;
	    $$->u.d = $2;
          }
          | CREATE create {
	    $$ = malloc(sizeof(*$$));
	    $$->typ = create_typ;
	    $$->u.c = $2;
          }
union_qry : query { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
            | union_qry BAR query
	    {
	      $$ = $1 ;
	      ($$->count)++;
	      $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	      $$->ents[($$->count)-1] = $3;
	    }

query      : TABLE
           {
	     $$ = malloc(sizeof(*$$)); $$->typ=scan_typ;
	     $$->u.scn = malloc(sizeof(*$$->u.scn));
	     $$->u.scn->table = $1;
	   }
           | STAR
	   {
	     $$ = malloc(sizeof(*$$)); $$->typ=table_star_typ;
	     $$->u.scn = 0;
	   } 
           | LBRACKET SELECT union_cmp union_qry RBRACKET
           {
	     $$ = malloc(sizeof(*$$)); $$->typ=select_typ;
	     $$->u.sel = malloc(sizeof(*$$->u.sel));
	     $$->u.sel->t = $3;
	     $$->u.sel->q = $4;
	   }
           | LBRACKET PROJECT col_tuple union_qry RBRACKET
           {
	     $$ = malloc(sizeof(*$$)); $$->typ=project_typ;
	     $$->u.prj = malloc(sizeof(*$$->u.prj));
	     $$->u.prj->t = $3;
	     $$->u.prj->q = $4;
	   }
           | LBRACKET JOIN cmp_tuple union_qry union_qry RBRACKET
           {
	     $$ = malloc(sizeof(*$$)); $$->typ=join_typ;
	     $$->u.jn = malloc(sizeof(*$$->u.jn));
	     $$->u.jn->t = $3;
	     $$->u.jn->lhs = $4;
	     $$->u.jn->rhs = $5;
	   };

insert     : TABLE val_tuple { $$ = malloc(sizeof(*$$)); $$->tbl = $1; $$->t = $2; }
delete     : TABLE val_tuple { $$ = malloc(sizeof(*$$)); $$->tbl = $1; $$->t = $2; }

create     : TABLE union_typ { $$ = malloc(sizeof(*$$)); $$->tbl = $1; $$->schema = $2; }

val_tuple : LPAREN val_list RPAREN { $$ = $2; }

val_list  : val    { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
          | val_list COMMA val
	  {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $3;
	  }

val       : IDENT  { $$ = malloc(sizeof(*$$)); $$->typ = ident_typ;  $$->u.ident = $1; }
          | INT    { $$ = malloc(sizeof(*$$)); $$->typ = int64_typ;    $$->u.integ = $1; }
          | STRING { $$ = malloc(sizeof(*$$)); $$->typ = string_typ; $$->u.str = $1; }

col_tuple : LPAREN col_list RPAREN { $$ = $2; }
          | LPAREN RPAREN { $$ = malloc(sizeof(*$$)); $$->count = 0; $$->ents = malloc(1); }

col_list  : col    { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
          | col_list COMMA col
	  {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $3;
	  }

col       : COLNUM  { $$ = malloc(sizeof(*$$)); $$->typ = colint_typ;    $$->u.colnum = $1; }
          | COLNAME { $$ = malloc(sizeof(*$$)); $$->typ = colstr_typ;    $$->u.colstr = $1; }

cmp_tuple : LPAREN cmp_list RPAREN { $$ = $2; }
          | LPAREN RPAREN { $$ = malloc(sizeof(*$$)); $$->count = 0; $$->ents = malloc(sizeof(char)); }

cmp_list  : cmp    { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
          | cmp_list COMMA cmp
	  {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $3;
	  }

cmp       : col test val { $$ = malloc(sizeof(*$$)); $$->lhs_typ = col_typ; $$->lhs.colent = $1;
                                                     $$->comparator = $2;
                                                     $$->rhs_typ = val_typ; $$->rhs.valent = $3; }
          | val test col { $$ = malloc(sizeof(*$$)); $$->lhs_typ = val_typ; $$->lhs.valent = $1;
                                                     $$->comparator = $2;
                                                     $$->rhs_typ = col_typ; $$->rhs.colent = $3; }
          | col test col { $$ = malloc(sizeof(*$$)); $$->lhs_typ = col_typ; $$->lhs.colent = $1;
                                                     $$->comparator = $2;
                                                     $$->rhs_typ = col_typ; $$->rhs.colent = $3; }
          | val test val { $$ = malloc(sizeof(*$$)); $$->lhs_typ = val_typ; $$->lhs.valent = $1;
                                                     $$->comparator = $2;
                                                     $$->rhs_typ = val_typ; $$->rhs.valent = $3; }
          | col { $$ = malloc(sizeof(*$$)); $$->lhs_typ = col_typ; $$->lhs.colent = $1;
                                                     $$->comparator = equal_typ;
                                                     $$->rhs_typ = col_typ; $$->rhs.colent = $1; }
| IDENT
  { $$ = malloc(sizeof(*$$)); $$->lhs_typ = col_typ; $$->lhs.colent = malloc(sizeof(*$$->lhs.colent)); 
    $$->lhs.colent->typ=string_typ; $$->lhs.colent->u.colstr=$1;
    $$->comparator = equal_typ;
    $$->rhs_typ = col_typ; $$->rhs.colent = malloc(sizeof(*$$->rhs.colent)); 
    $$->rhs.colent->typ=string_typ; $$->rhs.colent->u.colstr=$1; }
typ_tuple : LPAREN typ_list RPAREN { $$ = $2; }
typ_list  : typ { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
          | typ_list COMMA typ
	  {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $3;
	  }

typ       : STRING_TYPE
          {
	    $$ = malloc(sizeof(*$$));
	    $$->typ = typ_typ;
	    $$->u.type = string_typ;
          }
          | INT_TYPE
          {
	    $$ = malloc(sizeof(*$$));
	    $$->typ = typ_typ;
	    $$->u.type = int64_typ;
	  }

union_typ : typ_tuple { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
          | union_typ BAR typ_tuple
          {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $3;
	  }

union_cmp : cmp_tuple { $$ = malloc(sizeof(*$$)); $$->count = 1; $$->ents = malloc(sizeof(*($$->ents))); $$->ents[0] = $1; }
          | union_cmp BAR cmp_tuple
          {
	    $$ = $1 ;
	    ($$->count)++;
	    $$->ents = realloc($$->ents, sizeof(*($$->ents)) * ($$->count));
	    $$->ents[($$->count)-1] = $3;
	  }

test      : EQUAL { $$ = equal_typ; }

%%

int yyerror(parse_parm * param, void * yyscanner, char * errMsg) {
  printf("Error: %s\n", errMsg);
  // XXX memory leak?
  param->result = 0;
  return 0;
}
int yywrap () { return 1; }

//#include "lex.yy.c"

