#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <lladd/transactional.h>
#include <lladd/truncation.h>

int main(int argc, char** argv) { 

  Tinit();
  char * key;
  char * value;
  
  int ret;
  int xid = Tbegin();

  recordid hash = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);

  //  printf("rid = %d, %d, %lld\n", hash.page, hash.slot, hash.size);
  assert(hash.page == 1 && hash.slot == 0 && hash.size == 48);

  struct timeval start;
  struct timeval now;

  gettimeofday(&start,0);

  int count = 0;
  while(EOF != (ret=scanf("%as\t%as\n", &key, &value))) {
    if(!ret) { 
      printf("Could not parse input!\n");
      Tabort(xid);
      Tdeinit();
    }
    //printf("->%s<-\t->%s<-\n", key, value);
    ThashInsert(xid, hash, (byte*)key, strlen(key), (byte*)value, strlen(value));
    errno = 0;
    assert(errno == 0);
    free(key);
    free(value);
    count ++;
    
    if(!(count % 10000)) { 
      gettimeofday(&now,0);
      double rate = ((double)count)/((double)(now.tv_sec-start.tv_sec));
      printf("%d tuples inserted (%f per sec)\n", count, rate);
    }

  }
  Tcommit(xid);
  truncateNow();
  Tdeinit();
  return 0;
}
