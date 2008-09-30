LDADD=$(top_builddir)/src/stasis/libstasis.la  \
      $(top_builddir)/src/libdfa/librw.la


rose_SOURCES=rose.cpp
roseTable_SOURCES=roseTable.cpp

if BUILD_BENCHMARKS
noinst_PROGRAMS=lhtableThreaded naiveHash logicalHash readLogicalHash \
  naiveMultiThreaded logicalMultThreaded rawSet arrayListSet \
  logicalMultiReaders linearHashNTA linkedListNTA pageOrientedListNTA  \
  linearHashNTAThreaded linearHashNTAMultiReader linearHashNTAWriteRequests \
  transitiveClosure zeroCopy sequentialThroughput rose roseTable
endif
AM_CFLAGS=${GLOBAL_CFLAGS}
AM_CXXFLAGS=${GLOBAL_CXXFLAGS} -I ${top_builddir}/src
SUBDIRS=berkeleyDB
