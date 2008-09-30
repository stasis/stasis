#include <../../src/apps/cht/cht.h>
#include <assert.h>
int main(int argc, char ** argv) {
  char * conf = "client.conf";
  
  if(argc == 2) {
    conf = argv[1];
  } else if(argc > 2) {
    printf("Usage: %s [client.conf]\n", argv[0]);
    exit(-1);
  }
  
  Tinit();
  
  DfaSet * cht_client = cHtClientInit(conf);
  //  pthread_t main_worker_loop = 
  spawn_main_thread(cht_client);
  
  // cht_evals go here...
  
  int xid = 1;//NULL_MACHINE;  // What's the deal with this? ;)
  clusterHashTable_t new_ht;
  cHtCreate(xid, cht_client, &new_ht);
  int i;
  for(i = 0; i < 10000; i++) {
    int one = i; int two = i+1;
    cHtInsert(xid, cht_client, &new_ht, &one, sizeof(int), &two, sizeof(int));
    int newOne, newTwo;
    newOne = i;
    newTwo = 0;
    size_t newLen = sizeof(int);
    int ret = cHtLookup(xid, cht_client, &new_ht, &newOne, sizeof(int), &newTwo, &newLen);
    //    xid++;
    //printf("lookup returned %d (%d->%d)\n", ret, newOne, newTwo);
    assert(ret);
    assert(newOne == one);
    assert(newTwo == two);
    assert(newLen == sizeof(int));
  }
  cHtCommit(xid, cht_client);

  for(i = 0; i < 10000; i+=10) {
    int one = i; int two = -1;
    size_t size = sizeof(int);
    int removed = cHtRemove(xid, cht_client, &new_ht, &one, sizeof(int), &two, &size);
    assert(removed);

    size = sizeof(int);
    two = -1;
    removed = cHtRemove(xid, cht_client, &new_ht, &one, sizeof(int), &two, &size);
    assert(!removed);

    int newOne, newTwo;
    newOne = i;
    newTwo = 0;
    size_t newLen = sizeof(int);
    int ret = cHtLookup(xid, cht_client, &new_ht, &newOne, sizeof(int), &newTwo, &newLen);

    assert(!ret);

  }
  cHtAbort(xid, cht_client);
  exit(0);  // @todo this test case should continue on...
  for(i = 0; i < 10000; i++) {
    int one = i; int two = i+1;
    //    cHtInsert(xid, cht_client, new_ht, &one, sizeof(int), &two, sizeof(int));
    int newOne, newTwo;
    newOne = i;
    newTwo = 0;
    size_t newLen = sizeof(int);
    int ret = cHtLookup(xid, cht_client, &new_ht, &newOne, sizeof(int), &newTwo, &newLen);
    //    xid++;
    //printf("lookup returned %d (%d->%d)\n", ret, newOne, newTwo);
    assert(ret);
    assert(newOne == one);
    assert(newTwo == two);
    assert(newLen == sizeof(int));
  }
  cHtCommit(xid, cht_client);

  /** @todo devise a way to cleanly shut a CHT down. */
  
 // dfa_free(cht_client);
}
