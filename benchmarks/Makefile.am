LDADD=$(top_builddir)/src/2pc/lib2pc.a $(top_builddir)/src/libdfa/libdfa.a \
      $(top_builddir)/src/lladd/liblladd.a $(top_builddir)/src/pbl/libpbl.a \
      $(top_builddir)/src/libdfa/librw.a
bin_PROGRAMS=naiveHash logicalHash readLogicalHash naiveMultiThreaded logicalMultThreaded rawSet \
	     arrayListSet logicalMultiReaders linearHashNTA linkedListNTA pageOrientedListNTA  \
	     linearHashNTAThreaded linearHashNTAMultiReader linearHashNTAWriteRequests 
AM_CFLAGS= -g -Wall -pedantic -std=gnu99

SUBDIRS=berkeleyDB
