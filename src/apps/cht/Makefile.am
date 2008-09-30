if HAVE_LIBCONFUSE
LDADD=$(top_builddir)/src/libdfa/libdfa.la $(top_builddir)/src/2pc/lib2pc.la
#SOURCES=cht.c 
noinst_LTLIBRARIES=libcht.la
libcht_la_SOURCES=cht.c cht_client.c cht_server.c cht_message.c
AM_CFLAGS=${GLOBAL_CFLAGS}
endif 
