#include <stasis/transactional.h>
#include <stasis/bufferManager/pageArray.h>

unsigned long long recsize;
unsigned long long threadcount;
unsigned long long threadops;
recordid arrayList;

void* worker (void* arg) {
  intptr_t tid = (intptr_t)arg;
  recordid rid = arrayList;
  byte val[recsize];
  for(int j = 0; j < 10; j++) {
    for(unsigned long long i = 0; i < threadops; i++) {
      int opcode = i % 10;
      if(opcode < 0) {
	// insert (nop for now)
      } else if (opcode < 10) {
	// move
	int xid = Tbegin();
	for(int i = 0; i < recsize; i++) {
	  val[i] = ((byte*)&tid)[i&(sizeof(intptr_t)-1)];
	}
	rid.slot = (tid * threadops) + ((i * 97)%threadops); // bogus...
	Tset(xid, rid, val);
	Tcommit(xid);
      }
    }
  }
  return 0;
}

int main(int argc, char* argv[]) {
  char * e;
  recsize = strtoull(argv[1],&e,10);
  assert(recsize);
  assert(!*e);
  threadcount = strtoull(argv[2],&e,10);
  assert(threadcount);
  assert(!*e);
  threadops = strtoull(argv[3],&e,10)/(10*threadcount);
  assert(threadops);
  assert(!*e);

  stasis_log_type = LOG_TO_MEMORY;
  stasis_buffer_manager_factory = stasis_buffer_manager_mem_array_factory;

  pthread_t thread[threadcount];

  Tinit();
  int xid = Tbegin();
  arrayList = TarrayListAlloc(xid, 100, 10, recsize);
  TarrayListExtend(xid, arrayList, threadcount * threadops);
  Tcommit(xid);
  for(intptr_t i = 0; i < threadcount; i++) {
    pthread_create(&thread[i],0,worker,(void*)i);
  }
  for(intptr_t i = 0; i < threadcount; i++) {
    pthread_join(thread[i],0);
  }
  
  Tdeinit();

}
