EXTRA_DIST = reconf
SUBDIRS = src test utilities
AM_CFLAGS = -g -Wall -pedantic

docs: 
	doxygen doc/Doxyfile-api
	doxygen doc/Doxyfile-developers

.PHONY: docs