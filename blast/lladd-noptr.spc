#include "lladd-noptr.h"

event { 
  pattern { assertIsCorrectPage($1, $2); }
  guard   { $1 == $2 }
}

event { 
  pattern { $1 = loadPage($2, $3); }
  action  { $1 = $3; }
}
