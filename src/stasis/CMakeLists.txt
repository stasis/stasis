ADD_LIBRARY(stasis crc32.c redblack.c lhtable.c rw.c doubleLinkedList.c
                   common.c flags.c stats.c bufferManager.c
                   linkedlist.c operations.c pageHandle.c 
                   bufferManager/legacy/pageFile.c
                   bufferManager/legacy/pageCache.c
                   bufferManager/legacy/legacyBufferManager.c
                   page.c bufferPool.c blobManager.c
                   recovery2.c truncation.c transactional2.c
                   dirtyPageTable.c
                   allocationPolicy.c lockManager.c iterator.c
                   consumer.c arrayCollection.c ringbuffer.c fifo.c
                   multiplexer.c graph.c logger/logEntry.c
                   logger/safeWrites.c logger/logWriterUtils.c
                   logger/filePool.c
                   logger/inMemoryLog.c
                   logger/logHandle.c logger/logger2.c
                   logger/logMemory.c
                   logger/reorderingHandle.c
                   logger/groupForce.c
                   page/slotted.c
                   page/fixed.c 
                   page/raw.c
                   page/lsnFree.c
                   page/segment.c
                   compensations.c
                   operations/pageOperations.c
                   operations/decrement.c operations/increment.c
                   operations/prepare.c operations/set.c
                   operations/alloc.c operations/noop.c
                   operations/arrayList.c
                   operations/lsnFreeSet.c
                   operations/segmentFile.c
                   operations/group/logStructured.c
                   hash.c
                   operations/naiveLinearHash.c
                   operations/linearHashNTA.c
                   operations/linkedListNTA.c
                   operations/pageOrientedListNTA.c
                   operations/regions.c operations/lsmTree.c
                   operations/bTree.c
                   io/rangeTracker.c io/memory.c io/file.c io/pfile.c
                   io/non_blocking.c io/debug.c io/handle.c
                   bufferManager/pageArray.c
                   bufferManager/bufferHash.c replacementPolicy/lru.c
                   replacementPolicy/lruFast.c)

INSTALL(TARGETS stasis LIBRARY DESTINATION lib)
