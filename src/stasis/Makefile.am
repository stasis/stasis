nobase_pkginclude_HEADERS=$(wildcard *.h) $(wildcard */*.h)
lib_LTLIBRARIES=libstasis.la
libstasis_la_SOURCES=crc32.c redblack.c lhtable.c rw.c doubleLinkedList.c common.c flags.c \
		   stats.c linkedlist.c operations.c pageHandle.c \
		   page.c bufferPool.c blobManager.c recovery2.c truncation.c \
		   dirtyPageTable.c \
		   transactional2.c allocationPolicy.c \
		   lockManager.c iterator.c consumer.c arrayCollection.c ringbuffer.c fifo.c multiplexer.c graph.c\
	           logger/logEntry.c \
		   logger/logWriterUtils.c logger/safeWrites.c \
		   logger/filePool.c \
		   logger/inMemoryLog.c logger/logHandle.c logger/logger2.c \
		   logger/logMemory.c \
		   logger/reorderingHandle.c \
		   logger/groupForce.c \
		   page/raw.c page/slotted.c page/lsnFree.c page/fixed.c  compensations.c \
	           operations/pageOperations.c page/indirect.c operations/decrement.c  \
		   operations/increment.c  operations/prepare.c  operations/set.c \
		   operations/alloc.c operations/noop.c \
		   operations/arrayList.c hash.c \
		   operations/naiveLinearHash.c \
		   operations/linearHashNTA.c operations/linkedListNTA.c \
		   operations/pageOrientedListNTA.c \
		   operations/regions.c operations/lsmTree.c \
		   operations/lsnFreeSet.c \
		   operations/group/logStructured.c \
		   io/rangeTracker.c io/memory.c io/file.c io/pfile.c io/non_blocking.c \
		   io/debug.c io/handle.c \
		   bufferManager.c \
		   bufferManager/pageArray.c \
		   bufferManager/bufferHash.c \
                   bufferManager/legacy/pageFile.c \
		   bufferManager/legacy/pageCache.c \
		   bufferManager/legacy/legacyBufferManager.c \
	           replacementPolicy/lru.c replacementPolicy/lruFast.c
AM_CFLAGS=${GLOBAL_CFLAGS}
