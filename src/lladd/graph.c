#include <lladd/graph.h>
#include <alloca.h>
#include <assert.h>
#include <stdlib.h>
#include "page.h"
void naiveTraverse(int xid, recordid rid) {
  nodeHeader_t * node = alloca(rid.size);

  Tread(xid, rid, node);
  if(node->flags) { return; }
  node->flags = 1;
  Tset(xid, rid, node);

  int i = 0;
  // do 'local' nodes first.
  for(i = 0; i < node->inPage; i++) {
    short next = *(((short*)(((recordid*)(node+1))+node->outPage))+i);
    rid.slot = next;
    rid.size = TrecordSize(xid, rid);
    naiveTraverse(xid, rid);
  }

  for(i = 0; i < node->outPage; i++) {
    recordid next = ((recordid*)(node+1))[i];
    next.size = TrecordSize(xid, next);
    naiveTraverse(xid, next);
  }
}
/** @todo need to load the correct pages, since the local fifo doesn't refer to a single page!!! */
void multiTraverse(int xid, int page, lladdFifo_t * local, lladdFifo_t * global) {
  Page * p = loadPage(xid, page);
  while(Titerator_next(xid, local->iterator)) {
    recordid * rid_p;

    int rid_len = Titerator_value(xid, local->iterator, (byte**)&rid_p);
    assert(rid_len == sizeof(recordid));
    recordid rid = *rid_p;
    Titerator_tupleDone(xid, local->iterator);

    nodeHeader_t * node = malloc(rid.size);

    readRecord(xid, p, rid, node); 


    if(!node->flags) {
      node->flags = 1;

      // @todo logical operation here.
      Tset(xid, rid, node);

      // do 'local' nodes first.
      int i;
      for(i = 0; i < node->inPage; i++) {
	short slot = *(((short*)(((recordid*)(node+1))+node->outPage))+i);
	
	rid.slot = slot;
	rid.size = getRecordSize(xid, p, rid);
	Tconsumer_push(xid, local->consumer, (byte*)&(rid.page), sizeof(rid.page), (byte*)&rid, sizeof(recordid));

      }
      // now, do non-local nodes
      for(i = 0; i < node->outPage; i++) {
	recordid next = ((recordid*)(node+1))[i];

	Tconsumer_push(xid, global->consumer, (byte*)&(rid.page), sizeof(rid.page), (byte*)&next, sizeof(recordid));

      }
    }
    free(node);
    Titerator_tupleDone(xid, local->iterator);
  }

  releasePage(p);
}
