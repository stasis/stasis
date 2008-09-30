LDADD=$(top_builddir)/src/stasis/libstasis.la \
      $(top_builddir)/src/libdfa/librw.la
noinst_PROGRAMS=logfile_dump run_recovery truncate_log
AM_CFLAGS=${GLOBAL_CFLAGS}

SUBDIRS = 

if HAVE_SWIG
SUBDIRS += swig
endif
