LDADD=$(top_builddir)/src/stasis/libstasis.la

rose_SOURCES=rose.cpp
roseTable_SOURCES=roseTable.cpp

# logicalHash readLogicalHash logicalMultThreaded   logicalMultiReaders

if BUILD_BENCHMARKS
noinst_PROGRAMS=lhtableThreaded naiveHash \
  naiveMultiThreaded rawSet arrayListSet \
  linearHashNTA linkedListNTA pageOrientedListNTA  \
  linearHashNTAThreaded linearHashNTAMultiReader linearHashNTAWriteRequests \
  transitiveClosure zeroCopy sequentialThroughput rose roseTable
endif
AM_CFLAGS=${GLOBAL_CFLAGS}
AM_CXXFLAGS=${GLOBAL_CXXFLAGS} -I ${top_builddir}/src
SUBDIRS=berkeleyDB
