#! /bin/sh
make distclean
rm -rf Makefile.in  aclocal.m4  autom4te.cache  config.h.in  configure  depcomp  hello.c  install-sh  missing doc/api doc/developers doc/coverage autoscan.log configure.scan
find . | grep \~$ | xargs rm -f
find . -name '*.bb' | xargs rm -f
find . -name '*.bbg' | xargs rm -f
find . -name '*.da' | xargs rm -f
find . | perl -ne 'print if (/\/core(\.\d+)?$/)' | xargs rm -f
find . | perl -ne 'print if (/\/Makefile.in$/)' | xargs rm -f
find . | perl -ne 'print if (/\/storefile.txt$/)' | xargs rm -f
find . | perl -ne 'print if (/\/logfile.txt$/)' | xargs rm -f
find . | perl -ne 'print if (/\/blob._file.txt$/)' | xargs rm -f
rm -f test/gmon.out test/lladd/gmon.out
