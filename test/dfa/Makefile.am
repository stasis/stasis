LDADD= $(top_builddir)/src/libdfa/libdfa.a $(top_builddir)/src/lladd/liblladd.a $(top_builddir)/src/libdfa/librw.a -lconfuse
bin_PROGRAMS=ping_pong_dfa fork_bomb star
AM_CFLAGS=${GLOBAL_CFLAGS}
if HAVE_LIBCHECK
TESTS = check_networksetup ping_pong_dfa
else
TESTS = 
endif
noinst_PROGRAMS = $(TESTS)
CLEANFILES = check_networksetup.log
