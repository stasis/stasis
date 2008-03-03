EXTRA_DIST = reconf
SUBDIRS = stasis src test utilities benchmarks examples
export GLOBAL_CFLAGS = -g -Wall -pedantic -std=gnu99 -DPBL_COMPAT

nobase_include_HEADERS=libdfa/rw.h   # XXX move rw.h into stasis/ !!!

ACLOCAL_AMFLAGS = -I m4

## GOAL:  Make these not warn!
#-Wextra -Wno-unused-parameter -Winline

# -feliminate-unused-debug-symbols -feliminate-unused-debug-types -funit-at-a-time -finline-functions -fno-keep-static-consts

#-funit-at-a-time => -finline-functions-called-once
#-Wconversion

docs:
	doxygen doc/Doxyfile-api
	doxygen doc/Doxyfile-developers

tidy: precoverage
	find . -name '*~' -exec rm '{}' \;
	find . -name 'core' -exec rm '{}' \;
	find . -name 'core.[0-9]*' -exec rm '{}' \;
	find . -name 'vgcore.*' -exec rm '{}' \;
	find . -name '.\#*' -exec rm '{}' \;
	find . -name '*.da' -exec rm '{}' \;
	find . -name '*.gcda' -exec rm '{}' \;
	find . -name '*.gcno' -exec rm '{}' \;


precoverage : 
	find . -name '*.bb' | xargs rm -f
	find . -name '*.bbg' | xargs rm -f
	find . -name '*.da' | xargs rm -f
	rm -rf doc/coverage

coverage: precoverage
	echo '(make profile requires CFLAGS="-fprofile-arcs -ftest-coverage -pg -O0")'
#	cd test/lladd
#	make check || true
	rm -rf coverage
	mkdir coverage
	cp /home/sears/cron_stuff/CMakeCache.txt coverage
	cd coverage ; pwd
	cd coverage ; cmake ..
	cd coverage ; mv DartConfiguration.tcl DartConfiguration.tcl~
	cd coverage ; perl -ne 's/TimeOut\: 1500/TimeOut\: 3600/;print' < DartConfiguration.tcl~ > DartConfiguration.tcl
	cd coverage ; make -j 4
	cd coverage ; ctest || true
	cd coverage ; lcov -c --directory src/libdfa/ --directory src/stasis/ --directory test/ --directory src/apps/referential/ > tmp.info || true
	cd coverage ; genhtml -o ../doc/coverage tmp.info || true
	cd coverage ; rm tmp.info
#	cd ../..
#	lcov -c --directory src/libdfa/ --directory src/lladd/ --directory test/   > tmp.info
##	lcov -c --directory .  > tmp.info
#	genhtml -o doc/coverage tmp.info
#	rm tmp.info

.PHONY: docs precoverage coverage
