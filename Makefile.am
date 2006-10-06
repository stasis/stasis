EXTRA_DIST = reconf
SUBDIRS = src test utilities benchmarks
export GLOBAL_CFLAGS = -g -Wall -pedantic -std=gnu99 -Winline -DPBL_COMPAT
## GOAL:  Make these not warn!
#-Wextra -Wno-unused-parameter

# -feliminate-unused-debug-symbols -feliminate-unused-debug-types -funit-at-a-time -finline-functions -fno-keep-static-consts

#-funit-at-a-time => -finline-functions-called-once
#-Wconversion

docs:
	doxygen doc/Doxyfile-api
	doxygen doc/Doxyfile-developers

precoverage : 
	find . -name '*.bb' | xargs rm -f
	find . -name '*.bbg' | xargs rm -f
	find . -name '*.da' | xargs rm -f
	rm -rf doc/coverage

coverage: all precoverage
	echo '(make profile requires CFLAGS="-fprofile-arcs -ftest-coverage -pg -O0")'
	cd test/lladd
	make check || true
	cd ../..
	lcov -c --directory src/libdfa/ --directory src/lladd/ --directory test/   > tmp.info
#	lcov -c --directory .  > tmp.info
	genhtml -o doc/coverage tmp.info
	rm tmp.info

.PHONY: docs precoverage coverage
