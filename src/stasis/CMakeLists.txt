ADD_LIBRARY(stasis crc32.c redblack.c lhtable.c concurrentHash.c rw.c doubleLinkedList.c
                   common.c flags.c stats.c bufferManager.c
                   linkedlist.c operations.c pageHandle.c 
                   bufferManager/concurrentBufferManager.c
                   bufferManager/legacy/pageFile.c
                   bufferManager/legacy/pageCache.c
                   bufferManager/legacy/legacyBufferManager.c
                   page.c bufferPool.c blobManager.c
                   recovery2.c truncation.c transactional2.c
                   dirtyPageTable.c transactionTable.c
                   allocationPolicy.c iterator.c
                   logger/logEntry.c
                   logger/safeWrites.c logger/logWriterUtils.c
                   logger/filePool.c
                   logger/inMemoryLog.c
                   logger/logHandle.c logger/logger2.c
                   logger/reorderingHandle.c
                   logger/groupForce.c
                   lockManager.c
                   page/uninitialized.c
                   page/slotted.c
                   page/fixed.c 
                   page/raw.c
                   page/lsnFree.c
                   page/segment.c
                   page/latchFree/lfSlotted.c
                   operations/pageOperations.c
                   operations/decrement.c operations/increment.c
                   operations/prepare.c operations/set.c
                   operations/alloc.c operations/noop.c
                   operations/arrayList.c
                   operations/lsnFreeSet.c
                   operations/segmentFile.c
                   hash.c
                   operations/naiveLinearHash.c
                   operations/linearHashNTA.c
                   operations/linkedListNTA.c
                   operations/pageOrientedListNTA.c
                   operations/regions.c
                   operations/bTree.c
                   io/rangeTracker.c io/memory.c io/file.c io/pfile.c io/raid1.c
                   io/non_blocking.c io/debug.c io/handle.c
                   bufferManager/pageArray.c
                   bufferManager/bufferHash.c replacementPolicy/lru.c
                   replacementPolicy/lruFast.c replacementPolicy/threadsafeWrapper.c replacementPolicy/concurrentWrapper.c
                   util/log2.c util/histogram.c util/hashFunctions.c util/min.c util/ringbuffer.c
                   util/multiset.c util/slab.c
                   stlredblack.cpp util/stlslab.cpp)
ADD_LIBRARY(stasis_experimental
  experimental/consumer.c
  experimental/fifo.c
  experimental/graph.c
  experimental/logMemory.c
  experimental/multiplexer.c
  experimental/ringbuffer.c
  experimental/arrayCollection.c
  experimental/lockManagerImpl.c
  experimental/group.c
  experimental/lsmTree.c
  )

INSTALL(TARGETS stasis LIBRARY DESTINATION lib)
