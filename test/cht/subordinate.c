#include <../../src/apps/cht/cht.h>

int main(int argc, char ** argv) {
  char * conf = "cluster.conf";
  
  if(argc == 3) {
    conf = argv[2];
  } else if(argc > 3 || argc < 2) {
    printf("Usage: %s subordinate_number [client.conf]\n", argv[0]);
    exit(-1);
  }
  int subordinate_number = atoi(argv[1]);
//  printf("Subordinate: %d", subordinate_number);
  Tinit();
  short (*partition_function)(DfaSet *, Message *) = NULL;
  DfaSet * cht_client = cHtSubordinateInit(conf, partition_function, subordinate_number);
  main_loop(cht_client);
  
  Tdeinit();
 // dfa_free(cht_client);
}
