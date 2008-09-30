#include "ast.h"
#include <stdio.h>
#include <stdlib.h>

int main (int argc, char * argv[]) {
  char * in = 0;
  size_t len = 0;
  char * buf = 0;
  int read;
  while(-1!=(read = getline(&in, &len, stdin))) {
    //    printf("->%s<-",in);
    //    fflush(NULL);
    buf = astrncat(buf,in);
  }
  //  int ret = yyparse();
  expr_list * results = 0;

  parse(buf, &results);
  if(results) {
    char * str = pp_expr_list(results);
    printf("%s", str);
    free(str);
  }
  return results == 0;
}
