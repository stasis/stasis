#include <stasis/transactional.h>
#include "algebra.h"
#include "lang/ast.h"

int ReferentialDML_ExecuteInsert(int xid, ReferentialAlgebra_context_t*context, insert *i);
int ReferentialDML_ExecuteDelete(int xid, ReferentialAlgebra_context_t*context, delete *i);
