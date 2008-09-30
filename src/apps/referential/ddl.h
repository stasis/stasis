#ifndef DDL_H
#define DDL_H
#include "algebra.h"
#include "lang/ast.h"
int ReferentialDDL_CreateTable(int xid, ReferentialAlgebra_context_t*context, create *q);
#endif //DDL_H
