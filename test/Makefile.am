#INCLUDES = @CHECK_CFLAGS@
if HAVE_LIBCHECK

SUBDIRS = lladd

if HAVE_LIBCONFUSE
SUBDIRS += monotree messages dfa 2pc cht 
endif

if BUILD_POBJ
SUBDIRS += pobj
endif 

TESTS = check_check
else
TESTS = 
endif
noinst_PROGRAMS = $(TESTS)
check_check_SOURCES = check_check.c
#check_check_LDADD = @CHECK_LIBS@
CLEANFILES = check_check.log
