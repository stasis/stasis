LDADD= $(top_builddir)/src/libdfa/libdfa.a $(top_builddir)/src/lladd/liblladd.a $(top_builddir)/src/pbl/libpbl.a $(top_builddir)/src/libdfa/librw.a -lconfuse
bin_PROGRAMS=ping_pong_dfa fork_bomb star
AM_FLAGS= -g -Wall -pedantic -std=c99
if HAVE_LIBCHECK
TESTS = check_networksetup ping_pong_dfa
else
TESTS = 
endif
noinst_PROGRAMS = $(TESTS)
#LDADD = @CHECK_LIBS@ $(top_builddir)/src/lladd/liblladd.a $(top_builddir)/src/pbl/libpbl.a  $(top_builddir)/src/libdfa/librw.a #-lefence
CLEANFILES = check_networksetup.log
#AM_CFLAGS= -g -Wall -pedantic -std=gnu99
