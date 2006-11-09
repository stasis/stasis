LDADD=$(top_builddir)/src/lladd/liblladd.a  \
      $(top_builddir)/src/libdfa/librw.a
#$(top_builddir)/src/2pc/lib2pc.a $(top_builddir)/src/libdfa/libdfa.a 

bin_PROGRAMS=lhtableThreaded naiveHash logicalHash readLogicalHash naiveMultiThreaded logicalMultThreaded rawSet \
	     arrayListSet logicalMultiReaders linearHashNTA linkedListNTA pageOrientedListNTA  \
	     linearHashNTAThreaded linearHashNTAMultiReader linearHashNTAWriteRequests transitiveClosure zeroCopy
AM_CFLAGS=${GLOBAL_CFLAGS}

SUBDIRS=berkeleyDB
