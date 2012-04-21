ADD_LIBRARY(stasis util/crc32.c
                   util/bloomFilter.c
                   util/redblack.c
                   util/lhtable.c
                   util/concurrentHash.c
                   util/rw.c
                   util/doubleLinkedList.c
                   util/linkedlist.c
                   util/log2.c
                   util/histogram.c
                   util/hashFunctions.c
                   util/min.c
                   util/ringbuffer.c
                   util/random.c
                   util/multiset.c
                   util/slab.c
                   util/stlredblack.cpp
                   util/stlslab.cpp
                   common.c flags.c util/latchStats.c bufferManager.c
                   operations.c pageHandle.c 
                   bufferManager/concurrentBufferManager.c
                   bufferManager/legacy/pageFile.c
                   bufferManager/legacy/pageCache.c
                   bufferManager/legacy/legacyBufferManager.c
                   page.c bufferPool.c
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
                   page/lsnFree.c
                   page/segment.c
                   experimental/latchFree/lfSlotted.c
                   operations/pageOperations.c
                   operations/decrement.c operations/increment.c
                   operations/prepare.c operations/set.c
                   operations/alloc.c operations/noop.c
                   operations/arrayList.c
                   operations/lsnFreeSet.c
                   operations/segmentFile.c
                   operations/naiveLinearHash.c
                   operations/linearHashNTA.c
                   operations/linkedListNTA.c
                   operations/pageOrientedListNTA.c
                   operations/regions.c
                   operations/bTree.c
                   operations/blobs.c
                   io/rangeTracker.c
                   io/memory.c
                   io/file.c
                   io/pfile.c
                   io/raid1.c
                   io/raid0.c
                   io/non_blocking.c
                   io/debug.c
                   io/handle.c
                   bufferManager/pageArray.c
                   bufferManager/bufferHash.c
                   replacementPolicy/lru.c
                   replacementPolicy/lruFast.c
                   replacementPolicy/threadsafeWrapper.c
                   replacementPolicy/concurrentWrapper.c
                   replacementPolicy/clock.c
		   )
IF(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
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
ENDIF(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

INSTALL(TARGETS stasis LIBRARY DESTINATION lib)
