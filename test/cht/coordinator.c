#include <../../src/apps/cht/cht.h>

int main(int argc, char ** argv) {
  char * conf = "cluster.conf";
  
  if(argc == 2) {
    conf = argv[1];
  } else if(argc > 2) {
    printf("Usage: %s [client.conf]\n", argv[0]);
    exit(-1);
  }
  
  Tinit();
  short (*partition_function)(DfaSet *, Message *) = multiplex_interleaved;
  DfaSet * cht_client = cHtCoordinatorInit(conf, partition_function);
  main_loop(cht_client);
  
  Tdeinit();
 // dfa_free(cht_client);
}
