#include "lladd-delta.h"

event { 
  pattern { assertIsCorrectPage($1, $2); }
  guard   { $1 == $2 || -1 == $2}
}

event { 
  pattern { $1 = loadPage($2, $3); }
  action  { $1->id = $3; }
}

event { 
  pattern { releasePage($1); }
  action  { $1->id = -1; }
}
