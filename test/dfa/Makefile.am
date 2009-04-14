LDADD= $(top_builddir)/src/libdfa/libdfa.la $(top_builddir)/src/stasis/libstasis.la -lconfuse
AM_CFLAGS=${GLOBAL_CFLAGS}

TESTS = check_networksetup ping_pong_dfa

noinst_PROGRAMS = ping_pong_dfa fork_bomb star $(TESTS)
CLEANFILES = check_networksetup.log
