SUBDIRS = lladd monotree messages dfa 2pc cht
INCLUDES = @CHECK_CFLAGS@
if HAVE_CHECK
TESTS = check_check
else
TESTS = 
endif
noinst_PROGRAMS = $(TESTS)
check_check_SOURCES = check_check.c
check_check_LDADD = @CHECK_LIBS@
CLEANFILES = check_check.log
