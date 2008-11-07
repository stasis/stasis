LDADD= $(top_builddir)/src/libdfa/libdfa.la $(top_builddir)/src/stasis/libstasis.la -lconfuse
AM_CFLAGS=${GLOBAL_CFLAGS}
if HAVE_LIBCHECK
TESTS = check_networksetup ping_pong_dfa
else
TESTS = 
endif
noinst_PROGRAMS = ping_pong_dfa fork_bomb star $(TESTS)
CLEANFILES = check_networksetup.log
