#ifndef __ALGEBRA_H
#define __ALGEBRA_H
lladdIterator_t* ReferentialAlgebra_OpenTableScanner(int xid, recordid catalog,
						     char * tablename);
lladdIterator_t* ReferentialAlgebra_Select(int xid, lladdIterator_t* it,
					   char ** predicate);
lladdIterator_t* ReferentialAlgebra_Project(int xid, lladdIterator_t * it,
					    char ** project);
lladdIterator_t* ReferentialAlgebra_KeyValCat(int xid, lladdIterator_t * it);
char * tplRid(recordid rid);
recordid ridTpl(char * tpl);

void ReferentialAlgebra_init();

char ** executeQuery(int xid, recordid hash, char * line);
lladdIterator_t * parseExpression(int xid, recordid catalog, char **head);
char** parseTuple(char ** head);

#endif
