#SUBDIRS=operations logger
#LDADD=$(top_builddir)/src/pbl/libpbl.a 
lib_LIBRARIES=liblladd.a
#liblladd_a_LIBADD=logger/liblogger.a operations/liboperations.a
# removed: recovery.c transactional.c logger.c logger/logparser.c logger/logstreamer.c
liblladd_a_SOURCES=crc32.c common.c stats.c io.c bufferManager.c linkedlist.c operations.c \
                   pageFile.c pageCache.c page.c blobManager.c recovery2.c transactional2.c \
		   logger/logEntry.c logger/logWriter.c logger/logHandle.c logger/logger2.c \
	           operations/pageOperations.c page/indirect.c operations/decrement.c  \
		   operations/increment.c  operations/prepare.c  operations/set.c \
		   operations/alloc.c operations/noop.c operations/instantSet.c \
		   page/slotted.c operations/lladdhash.c page/header.c page/fixed.c \
		   operations/arrayList.c hash.c operations/linearHash.c operations/naiveLinearHash.c
AM_CFLAGS= -g -Wall -pedantic -std=gnu99

