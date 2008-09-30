ADD_LIBRARY(stasis crc32.c redblack.c lhtable.c doubleLinkedList.c
                   common.c flags.c stats.c io.c bufferManager.c
                   linkedlist.c operations.c pageHandle.c pageFile.c
                   pageCache.c page.c bufferPool.c blobManager.c
                   recovery2.c truncation.c transactional2.c
                   allocationPolicy.c lockManager.c iterator.c
                   consumer.c arrayCollection.c ringbuffer.c fifo.c
                   multiplexer.c graph.c logger/logEntry.c
                   logger/logWriter.c logger/inMemoryLog.c
                   logger/logHandle.c logger/logger2.c
                   logger/logMemory.c page/raw.c page/slotted.c
                   page/fixed.c compensations.c
                   operations/pageOperations.c page/indirect.c
                   operations/decrement.c operations/increment.c
                   operations/prepare.c operations/set.c
                   operations/alloc.c operations/noop.c
                   operations/arrayList.c
                   hash.c
                   operations/naiveLinearHash.c
                   operations/nestedTopActions.c
                   operations/linearHashNTA.c
                   operations/linkedListNTA.c
                   operations/pageOrientedListNTA.c operations/bTree.c
                   operations/regions.c operations/lsmTree.c
                   io/rangeTracker.c io/memory.c io/file.c io/pfile.c
                   io/non_blocking.c io/debug.c
                   bufferManager/pageArray.c
                   bufferManager/bufferHash.c replacementPolicy/lru.c
                   replacementPolicy/lruFast.c)
