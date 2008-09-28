/*
  From:

  http://www.usualcoding.eu/index.php?tag/flex

 */

typedef struct      parse_parm_s
{
  void            *yyscanner;
  char            *buf;
  int             pos;
  int             length;
  void            *result;
}                   parse_parm;

//void    parse(char *buf, expr_list **result);


//#define YYSTYPE    

#define YY_EXTRA_TYPE   parse_parm *

//int     yylex(void /*YYSTYPE*/ *, void *);
int     yylex_init(void **);
int     yylex_destroy(void *);
void    yyset_extra(YY_EXTRA_TYPE, void *);
int     yyparse(parse_parm *, void *);
//void    yyerror();
