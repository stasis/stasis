TESTS=./run
LDADD=-lconfuse $(top_builddir)/src/apps/cht/libcht.la $(top_builddir)/src/2pc/lib2pc.la $(top_builddir)/src/libdfa/libdfa.la \
      $(top_builddir)/src/stasis/libstasis.la $(top_builddir)/src/libdfa/librw.la
noinst_PROGRAMS=simple cht_server client coordinator subordinate
AM_CFLAGS=${GLOBAL_CFLAGS}
