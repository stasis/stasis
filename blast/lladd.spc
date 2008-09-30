#include "lladd-blast.h"

event { 
  pattern { $1 = loadPage($2, $3); }
  guard   { $1 == 0 || $1->id == -1 }
}

event { 
  pattern { releasePage($1); }
  guard   { $1 != 0 && $1->id != -1 }
  action  { $1->id = -1; }
}
