#SUBDIRS=operations logger
#LDADD=$(top_builddir)/src/pbl/libpbl.a 
lib_LIBRARIES=liblladd.a
#liblladd_a_LIBADD=logger/liblogger.a operations/liboperations.a
# removed: recovery.c transactional.c logger.c logger/logparser.c logger/logstreamer.c
liblladd_a_SOURCES=crc32.c redblack.c lhtable.c doubleLinkedList.c common.c stats.c io.c bufferManager.c linkedlist.c operations.c \
                   pageHandle.c pageFile.c pageCache.c page.c bufferPool.c blobManager.c recovery2.c truncation.c \
		   transactional2.c allocationPolicy.c \
		   lockManager.c iterator.c consumer.c arrayCollection.c ringbuffer.c fifo.c multiplexer.c graph.c\
	           logger/logEntry.c logger/logWriter.c logger/inMemoryLog.c logger/logHandle.c logger/logger2.c \
		   logger/logMemory.c \
		   page/raw.c page/slotted.c page/fixed.c  compensations.c \
	           operations/pageOperations.c page/indirect.c operations/decrement.c  \
		   operations/increment.c  operations/prepare.c  operations/set.c \
		   operations/alloc.c operations/noop.c operations/instantSet.c \
		   operations/arrayList.c hash.c operations/linearHash.c \
		   operations/naiveLinearHash.c operations/nestedTopActions.c \
		   operations/linearHashNTA.c operations/linkedListNTA.c \
		   operations/pageOrientedListNTA.c operations/bTree.c \
		   operations/regions.c \
		   io/rangeTracker.c io/memory.c io/file.c io/non_blocking.c io/debug.c \
		   bufferManager/pageArray.c bufferManager/bufferHash.c \
	           replacementPolicy/lru.c replacementPolicy/lruFast.c
#		   page/header.c logger/logMemory.c \ ringbuffer.c \   asdfas
#operations/lladdhash.c 
#AM_CFLAGS= -g -Wall -pedantic -std=gnu99
AM_CFLAGS=${GLOBAL_CFLAGS}
