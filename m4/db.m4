dnl
dnl Autoconf support for finding Berkeley DB
dnl

AC_DEFUN([AC_DB_HELP], [
cat <<EOF

Configure error with Berkeley DB...

If your installed version is not one of [$dbversions], you may
have to specify it with --with-dbver.

If your installation is in a non-standard path, you can specify
it with --with-db=DIR.

To download the latest version, go to http://www.sleepycat.com
To build and install to /usr/local/BerkeleyDB-<version>:

# cd <db_download_dir>/build_unix
# ../dist/configure --enable-cxx
# make
# make install

EOF

])

dnl
dnl Main macro for finding a usable db installation 
dnl
AC_DEFUN([AC_CONFIG_DB], [
    ac_dbvers='4.3 4.2'
    ac_dbdir='yes'

    AC_ARG_WITH(db,
        AC_HELP_STRING([--with-db=DIR],
    		   [location of a Berkeley DB installation (default system)]),
        ac_dbdir=$withval) 

    AC_ARG_WITH(dbver,
        AC_HELP_STRING([--with-dbver=VERSION],
    		   Berkeley DB versions to try (default 4.3 or 4.2)),
        ac_dbvers=$withval)

    dnl
    dnl First make sure we even want it
    dnl
    if test x$ac_dbdir = xno ; then
    DB_ENABLED=0
    else
    DB_ENABLED=1

    dnl
    dnl Now check if we have a cached value, and if not, find it.
    dnl
    if test ! x$dtn_cv_path_db_h = x ; then
        echo "checking for Berkeley DB installation... (cached) $dtn_cv_path_db_h/db_cxx.h, $dtn_cv_path_db_lib -l$dtn_cv_lib_db"
    else
        AC_FIND_DB
    fi

    if test ! $dtn_cv_path_db_h = /usr/include ; then
        CPPFLAGS="$CPPFLAGS -I$dtn_cv_path_db_h"
    fi

    if test ! $dtn_cv_path_db_lib = /usr/lib ; then
        LDFLAGS="$LDFLAGS -L$dtn_cv_path_db_lib"
    fi

    LIBS="$LIBS -l$dtn_cv_lib_db"

    fi # DB_ENABLED

    AC_SUBST(DB_ENABLED)
])

dnl
dnl Find db
dnl
AC_DEFUN([AC_FIND_DB], [
    dtn_cv_path_db_h=
    dtn_cv_path_db_lib=
    dtn_cv_lib_db=

    ac_save_CPPFLAGS="$CPPFLAGS"
    ac_save_LDFLAGS="$LDFLAGS"
    ac_save_LIBS="$LIBS"

    AC_LANG_PUSH(C++)

    for ac_dbver in $ac_dbvers ; do

    ac_dbver_major=`echo $ac_dbver | cut -d . -f1`
    ac_dbver_minor=`echo $ac_dbver | cut -d . -f2`

    if test ! x"$ac_dbdir" = x"yes" ; then
        ac_dbincdirs=$ac_dbdir/include
    else
        ac_dbincdirs="/usr/include /usr/local/include/db4 /usr/local/include/db42"
        ac_dbincdirs="$ac_dbincdirs /usr/include/db$ac_dbver"
        ac_dbincdirs="$ac_dbincdirs /usr/local/BerkeleyDB.$ac_dbver/include"
    fi

    if test ! x"$ac_dbdir" = x"yes" ; then
        ac_dblibdirs="$ac_dbdir/lib"
    else
        ac_dblibdirs="/usr/lib /usr/local/lib /usr/local/lib/db42"
        ac_dblibdirs="$ac_dblibdirs /usr/local/BerkeleyDB.$ac_dbver/lib"
    fi

    for ac_dbincdir in $ac_dbincdirs; do
	CPPFLAGS="$ac_save_CPPFLAGS -I$ac_dbincdir"
	LDFLAGS="$ac_save_LDFLAGS"
	LIBS="$ac_save_LIBS"

	dnl
	dnl First check the version in the header file. If there's a match, 
	dnl fall through to the other check to make sure it links.
	dnl If not, then we can break out of the two inner loops.
	dnl
        AC_MSG_CHECKING([for Berkeley DB header (version $ac_dbver) in $ac_dbincdir])
	AC_LINK_IFELSE(
	  AC_LANG_PROGRAM(
	    [
                #include <db_cxx.h>
           
                #if (DB_VERSION_MAJOR != ${ac_dbver_major}) || \
                    (DB_VERSION_MINOR != ${ac_dbver_minor})
                #error "incorrect version"
                #endif
            ],
            
            [
            ]),
          [ 
	      AC_MSG_RESULT([yes])
          ],
          [
              AC_MSG_RESULT([no])
	      continue
          ])

      for ac_dblibdir in $ac_dblibdirs; do
      for ac_dblib    in db_cxx-$ac_dbver; do

	LDFLAGS="$ac_save_LDFLAGS -L$ac_dblibdir"
	LIBS="$ac_save_LIBS -l$ac_dblib"

        AC_MSG_CHECKING([for Berkeley DB library in $ac_dblibdir, -l$ac_dblib])
	AC_LINK_IFELSE(
	  AC_LANG_PROGRAM(
	    [
                #include <db_cxx.h>
            ],
            
            [
	        DB *db;
                db_create(&db, NULL, 0);
            ]),

          [
              AC_MSG_RESULT([yes])
              dtn_cv_path_db_h=$ac_dbincdir
              dtn_cv_path_db_lib=$ac_dblibdir
              dtn_cv_lib_db=$ac_dblib
              break 4
          ],
          [
              AC_MSG_RESULT([no])
          ])
    done
    done
    done
    done

    AC_LANG_POP(C++)

    CPPFLAGS="$ac_save_CPPFLAGS"
    LDFLAGS="$ac_save_LDFLAGS"
    LIBS="$ac_save_LIBS"

    if test x$dtn_cv_path_db_h = x ; then
        AC_DB_HELP
        AC_MSG_ERROR([can't find usable Berkeley DB installation])
    fi
])
