SUBDIRS = lladd monotree messages dfa 2pc cht pobj
#INCLUDES = @CHECK_CFLAGS@
if HAVE_LIBCHECK
TESTS = check_check
else
TESTS = 
endif
noinst_PROGRAMS = $(TESTS)
check_check_SOURCES = check_check.c
#check_check_LDADD = @CHECK_LIBS@
CLEANFILES = check_check.log
