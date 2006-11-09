if HAVE_LIBCONFUSE
lib_LIBRARIES=libdfa.a librw.a
else
lib_LIBRARIES=librw.a
endif

libdfa_a_SOURCES=libdfa.c monotree.c smash.c callbacks.c messages.c networksetup.c
librw_a_SOURCES=rw.c
AM_CFLAGS=${GLOBAL_CFLAGS}
