#INCLUDES = @CHECK_CFLAGS@

SUBDIRS = stasis

if HAVE_LIBCONFUSE
SUBDIRS += monotree messages dfa 2pc cht 
endif

if BUILD_POBJ
SUBDIRS += pobj
endif 

TESTS = check_check

noinst_PROGRAMS = $(TESTS)
check_check_SOURCES = check_check.c
#check_check_LDADD = @CHECK_LIBS@
CLEANFILES = check_check.log
