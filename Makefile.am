EXTRA_DIST = reconf
SUBDIRS = src test utilities
AM_CFLAGS = -g -Wall -pedantic

docs: 
	doxygen doc/Doxyfile-api
	doxygen doc/Doxyfile-developers

precoverage : 
	find . -name '*.bb' | xargs rm -f
	find . -name '*.bbg' | xargs rm -f
	find . -name '*.da' | xargs rm -f

coverage: precoverage check
	echo '(make profile requires CFLAGS="-fprofile-arcs -ftest-coverage -pg -O0")'
	lcov -c --directory src/libdfa/ --directory src/lladd/ --directory test/   > tmp.info
#	lcov -c --directory .  > tmp.info
	genhtml -o doc/coverage tmp.info
	rm tmp.info

.PHONY: docs precoverage coverage