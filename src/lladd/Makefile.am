#SUBDIRS=operations logger
#LDADD=$(top_builddir)/src/pbl/libpbl.a 
lib_LIBRARIES=liblladd.a
#liblladd_a_LIBADD=logger/liblogger.a operations/liboperations.a
# removed: recovery.c transactional.c logger.c logger/logparser.c logger/logstreamer.c
liblladd_a_SOURCES=common.c stats.c bufferManager.c linkedlist.c operations.c pageFile.c pageCache.c page.c blobManager.c recovery2.c transactional2.c logger/logEntry.c logger/logWriter.c logger/logHandle.c logger/logger2.c operations/decrement.c  operations/increment.c  operations/prepare.c  operations/set.c operations/alloc.c #operations/lladdhash.c 
AM_CFLAGS= -g -Wall -pedantic -std=gnu99

