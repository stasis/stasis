#SUBDIRS=operations logger
#LDADD=$(top_builddir)/src/pbl/libpbl.a 
lib_LIBRARIES=liblladd.a
#liblladd_a_LIBADD=logger/liblogger.a operations/liboperations.a
# removed: recovery.c transactional.c logger.c logger/logparser.c logger/logstreamer.c
liblladd_a_SOURCES=common.c stats.c bufferManager.c linkedlist.c operations.c pageCache.c page.c blobManager.c recovery2.c transactional2.c logger/logEntry.c logger/logWriter.c logger/logHandle.c logger/logger2.c operations/decrement.c  operations/increment.c  operations/lladdhash.c  operations/prepare.c  operations/set.c operations/alloc.c 
AM_CFLAGS= -g -Wall -pedantic -std=gnu99

