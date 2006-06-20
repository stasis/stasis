TESTS=./run
LDADD=-lconfuse $(top_builddir)/src/apps/cht/libcht.a $(top_builddir)/src/2pc/lib2pc.a $(top_builddir)/src/libdfa/libdfa.a \
      $(top_builddir)/src/lladd/liblladd.a $(top_builddir)/src/libdfa/librw.a
bin_PROGRAMS=simple cht_server client coordinator subordinate
AM_CFLAGS=${GLOBAL_CFLAGS}
