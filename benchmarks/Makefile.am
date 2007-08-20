LDADD=$(top_builddir)/src/stasis/libstasis.la  \
      $(top_builddir)/src/libdfa/librw.la

if BUILD_BENCHMARKS
noinst_PROGRAMS=lhtableThreaded naiveHash logicalHash readLogicalHash naiveMultiThreaded logicalMultThreaded rawSet \
	     arrayListSet logicalMultiReaders linearHashNTA linkedListNTA pageOrientedListNTA  \
	     linearHashNTAThreaded linearHashNTAMultiReader linearHashNTAWriteRequests transitiveClosure zeroCopy sequentialThroughput
endif
AM_CFLAGS=${GLOBAL_CFLAGS}

SUBDIRS=berkeleyDB
