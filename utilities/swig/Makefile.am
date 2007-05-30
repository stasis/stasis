
BUILT_SOURCES = lladd_wrap.cc

all: $(BUILD_SOURCES)
pkgpython_PYTHON = lladd.py
pkgpyexec_LTLIBRARIES = _lladd.la
_lladd_la_SOURCES = lladd_wrap.cc
_lladd_la_CPPFLAGS = $(SWIG_PYTHON_CPPFLAGS) -I$(top_srcdir) -I$(top_srcdir)/src
_lladd_la_LDFLAGS = -module
_lladd_la_LIBADD = $(top_srcdir)/src/lladd/liblladd.a $(top_srcdir)/src/libdfa/librw.a

lladd_wrap.cc : lladd.i
	$(SWIG) $(SWIG_PYTHON_OPT) -I$(top_srcdir)/src -I$(top_srcdir) -o $@ $<
