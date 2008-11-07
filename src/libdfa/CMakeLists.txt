IF(HAVE_CONFUSE)
 ADD_LIBRARY(libdfa libdfa.c monotree.c smash.c callbacks.c messages.c networksetup.c)
ENDIF(HAVE_CONFUSE)
