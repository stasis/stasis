%{
#include <assert.h>
#include <string.h>
#include "ast.h"
#include "y.tab.h"
#include "parser_private.h"
//#define DBG(x) printf("%s\t",x); ECHO 
#define DBG(x)

#define PARM    yyget_extra(yyscanner)

#define YY_INPUT(buffer, res, max_size)			\
  do {							\
    if (PARM->pos >= PARM->length)			\
      res = YY_NULL;					\
    else						\
      {							\
	res = PARM->length - PARM->pos;			\
	res > (int)max_size ? res = max_size : 0;	\
	memcpy(buffer, PARM->buf + PARM->pos, res);	\
	PARM->pos += res;				\
      }							\
  } while (0)

//%option reentrant bison-bridge
//%option noyywrap
//%option nounput

%}

%option reentrant bison-bridge
%option noyywrap
%option nounput

%START INTUPLE

%%
"("                           {DBG("LPAREN"); BEGIN INTUPLE; return LPAREN;}
")"                           {DBG("RPAREN"); BEGIN 0; return RPAREN;}
"{"                           {DBG("LBRACKET"); return LBRACKET;}
"}"                           {DBG("RBRACKET"); return RBRACKET;}
","                           {DBG("COMMA"); return COMMA;}
"|"                           {DBG("BAR"); return BAR;}
"*"                           {DBG("STAR"); return STAR;}
"="                           {DBG("EQUAL"); return EQUAL;}
"insert"                      {DBG("INSERT"); return INSERT;}
"i"                           {DBG("INSERT"); return INSERT;}
"delete"                      {DBG("DELETE"); return DELETE;}
"d"                           {DBG("DELETE"); return DELETE;}
"query"                       {DBG("QUERY"); return QUERY;}
"q"                           {DBG("QUERY"); return QUERY;}
"create"                      {DBG("CREATE"); return CREATE;}
"c"                           {DBG("CREATE"); return CREATE;}
"string"                      {DBG("STRING_TYPE"); return STRING_TYPE;}
"int"                         {DBG("INT_TYPE"); return INT_TYPE;}
"s"                           {DBG("SELECT"); return SELECT;}
"select"                      {DBG("SELECT"); return SELECT;}
"p"                           {DBG("PROJECT"); return PROJECT;}
"project"                     {DBG("PROJECT"); return PROJECT;}
"j"                           {DBG("JOIN"); return JOIN;}
"join"                        {DBG("JOIN"); return JOIN;}
<INTUPLE>[A-Za-z_][A-Za-z0-9_]* {DBG("IDENT"); yylval->string = strdup(yytext);return IDENT; }
[A-Za-z_][A-Za-z0-9_]*          {DBG("TABLE"); yylval->string = strdup(yytext); return TABLE; }
[-+]?[0-9]+                   {DBG("INT"); yylval->number = atoi(yytext); /*XXX error checking*/return INT; }
$[A-Za-z_][A-Za-z0-9_]*         {DBG("COLNAME"); yylval->string = strdup(yytext+1); return COLNAME; }
$[1-9][0-9]*                  {DBG("COLNUM"); yylval->number = atoi(yytext+1); /*XXX*/return COLNUM; }
$0                            {DBG("COLNUM"); yylval->number = 0; return COLNUM; }

\"[^"]*   {DBG("STRING");
                            if (yytext[yyleng-1] == '\\') {
			      yymore();
                            } else {
                              input(yyscanner);
                              yylval->string = strdup(yytext+1);
			      return STRING;
                            }
          }
[ \t\n]                     DBG("WS");
%%
