
BUILT_SOURCES = stasis_wrap.cc

all: $(BUILD_SOURCES)
python_PYTHON = stasis.py
pyexec_LTLIBRARIES = _stasis.la
_stasis_la_SOURCES = stasis_wrap.cc
_stasis_la_CPPFLAGS = $(SWIG_PYTHON_CPPFLAGS) -I$(top_srcdir) -I$(top_srcdir)/src
_stasis_la_LDFLAGS = -module
_stasis_la_LIBADD = $(top_srcdir)/src/stasis/libstasis.la $(top_srcdir)/src/libdfa/librw.la

stasis_wrap.cc : lladd.i
	$(SWIG) $(SWIG_PYTHON_OPT) -I$(top_srcdir)/src -I$(top_srcdir) -o $@ $<
