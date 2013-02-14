#ifndef STASIS_CONFIG_H
#define STASIS_CONFIG_H
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#cmakedefine HAVE_POSIX_FALLOCATE
#cmakedefine HAVE_POSIX_MEMALIGN
#cmakedefine HAVE_POSIX_FADVISE
#cmakedefine HAVE_FDATASYNC
#cmakedefine HAVE_SYNC_FILE_RANGE
#cmakedefine HAVE_O_NOATIME
#cmakedefine HAVE_O_DIRECT
#cmakedefine HAVE_O_DSYNC
#cmakedefine HAVE_GCC_ATOMICS
#cmakedefine HAVE_PTHREAD_STACK_MIN
#cmakedefine HAVE_ALLOCA_H
#cmakedefine HAVE_TDESTROY
#cmakedefine HAVE_POWL
#cmakedefine DBUG
#cmakedefine ON_LINUX
#cmakedefine ON_MACOS
#endif