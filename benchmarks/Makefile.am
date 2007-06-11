LDADD=$(top_builddir)/src/stasis/liblladd.a  \
      $(top_builddir)/src/libdfa/librw.a
#$(top_builddir)/src/2pc/lib2pc.a $(top_builddir)/src/libdfa/libdfa.a 

if BUILD_BENCHMARKS
bin_PROGRAMS=lhtableThreaded naiveHash logicalHash readLogicalHash naiveMultiThreaded logicalMultThreaded rawSet \
	     arrayListSet logicalMultiReaders linearHashNTA linkedListNTA pageOrientedListNTA  \
	     linearHashNTAThreaded linearHashNTAMultiReader linearHashNTAWriteRequests transitiveClosure zeroCopy
endif
AM_CFLAGS=${GLOBAL_CFLAGS}

SUBDIRS=berkeleyDB
