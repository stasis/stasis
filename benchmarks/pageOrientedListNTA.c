#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <lladd/transactional.h>
#include <unistd.h>

//#define CHECK_RESULTS 1

int main(int argc, char** argv) {

  assert(argc == 3);

  int xact_count = atoi(argv[1]);
  int count = atoi(argv[2]);
  int k;

  /*  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt"); */
  
  Tinit();
  int xid = Tbegin();
   
  recordid hash = TpagedListAlloc(xid);
  
  Tcommit(xid);
   
   int i = 0;
   
   for(k = 0; k < xact_count; k++) {
     
     xid = Tbegin();
     
     for(;i < count *(k+1) ; i++) {
       
       TpagedListInsert(xid, hash, (byte*)&i, sizeof(int), (byte*)&i, sizeof(int));
       
     }
     
     Tcommit(xid);
     
   }

#ifdef CHECK_RESULTS

   printf("Checking results.\n");
   xid = Tbegin();
   lladd_pagedList_iterator * it = TpagedListIterator(xid, hash);

   assert(i == xact_count * count);

   int seen[count * xact_count];
   for(i = 0; i < xact_count * count; i++) {
     seen[i] = 0;
   }
   i = 0;
   int * key;
   int * val;
   int keySize;
   int valSize;
   while(TpagedListNext(xid, it, (byte**)&key, &keySize, (byte**)&val, &valSize)) {
     assert(*key == *val);
     assert(keySize == sizeof(int));
     assert(valSize == sizeof(int));
     assert(!seen[*key]);
     seen[*key]++;

     free(key);
     free(val);
     
     i++;

   }
   assert(i == xact_count * count);
   for(int i = 0; i < count * xact_count; i++) {
     assert(seen[i]==1);
   }

   Tcommit(xid);

#endif    



   Tdeinit();

}
