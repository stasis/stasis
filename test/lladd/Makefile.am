INCLUDES = @CHECK_CFLAGS@
if HAVE_CHECK
## Had to disable check_lht because lht needs to be rewritten.
TESTS = check_logEntry check_logWriter check_page check_operations check_transactional2 check_recovery check_blobRecovery check_bufferManager check_indirect
else
TESTS = 
endif
noinst_PROGRAMS = $(TESTS)
LDADD = @CHECK_LIBS@ $(top_builddir)/src/lladd/liblladd.a $(top_builddir)/src/pbl/libpbl.a $(top_builddir)/src/libdfa/librw.a #-lefence
CLEANFILES = check_lht.log check_logEntry.log storefile.txt logfile.txt blob0_file.txt blob1_file.txt check_blobRecovery.log check_logWriter.log check_operations.log check_recovery.log check_transactional2.log check_page.log check_bufferManager.log check_indirect.log
AM_CFLAGS= -g -Wall -pedantic -std=gnu99
