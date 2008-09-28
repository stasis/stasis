#ifndef __ALGEBRA_H
#define __ALGEBRA_H

#include "lang/ast.h"

typedef struct {
  recordid hash;
} ReferentialAlgebra_context_t;

lladdIterator_t* ReferentialAlgebra_ExecuteQuery(int xid,
				 ReferentialAlgebra_context_t* c, union_qry *q);

lladdIterator_t* ReferentialAlgebra_OpenTableScanner(int xid, recordid catalog,
						     char * tablename);
lladdIterator_t* ReferentialAlgebra_Select(int xid, lladdIterator_t* it,
					   union_cmp * predicate);
lladdIterator_t* ReferentialAlgebra_Project(int xid, lladdIterator_t * it,
					    col_tuple * project);
lladdIterator_t* ReferentialAlgebra_Join(int xid,
					 lladdIterator_t * outer_it,
					 lladdIterator_t * inner_it,
					 cmp_tuple * cmp);
lladdIterator_t* ReferentialAlgebra_KeyValCat(int xid, lladdIterator_t * it);

void ReferentialAlgebra_init();
ReferentialAlgebra_context_t * ReferentialAlgebra_openContext(int xid, recordid rid);
recordid ReferentialAlgebra_allocContext(int xid);

#endif
