
## .SECONDARY means that the files will be deleted if their generation
## is interrupted, but will not be automatically deleted when make
## exits normally.

.SECONDARY :
# %.ci-raw %.def


# This lets us keep the .def files in their own directory, but treat
# them as though they're in the current (per run) directory.
#VPATH=../defs/

VPATH=../..:../../berkeleyDB

GRAPHS=BULK_LOAD
# BULK_LOAD_RAW LLADD_HASH_TPS TPS
BINARIES=linearHashNTA logicalHash bdbHash

all-binaries = $(patsubst %, %.exe, ${BINARIES})
all-r-files  = $(patsubst %, %.R,   $(GRAPHS)) 
all-defs     = $(patsubst %, %.def, $(GRAPHS)) 

MAKEFLAGS=--no-print-directory
RFLAGS=--no-save --no-load

all: all-graphs all-results ${all-binaries}

all-graphs   : all-png-graphs all-pdf-graphs all-ps-graphs

all-results    : $(patsubst %, %.results, $(GRAPHS)) 
all-png-graphs : $(patsubst %, %.png, $(GRAPHS)) 
all-pdf-graphs : $(patsubst %, %.pdf, $(GRAPHS)) 
all-ps-graphs  : $(patsubst %, %.ps,  $(GRAPHS))

# XXX the "|| true" is a hack; grep -v returns failure if there 
# are no matching lines....  Really, we should check to see if it returned 2.
%.results : %.def  ${all-binaries} 
	$(MAKE) $(MAKEFLAGS) `grep Data-Series\: $< | \
              perl -ne 's/[\"\s]+/\t/g;print;print"\n"' | \
              cut -f2 | \
              perl -ne 'chomp;print"$*-";print;print".dat\n"'`
	touch $*.results

# \
#	| grep -v 'is up to date.' || true

%.png: %.results %.def
	../plot --format png $* | R $(RFLAGS) > /dev/null
%.pdf: %.results %.def
	../plot --format pdf $* | R $(RFLAGS) > /dev/null
%.ps:  %.results %.def
	../plot --format ps  $* | R $(RFLAGS) > /dev/null

%.def : defs/%.def 
	ln -s $< .


%.ci-raw : ${all-binaries} ${all-defs} 
#	echo "Foo" > $*.ci-raw
	../generateGraphData $*

%.dat: %.ci-raw ${all-defs}
	../ci-parser $*.def < $*.ci-raw > $*.dat

%.R : %.def
	plotting.pl $* > $*.R

clean :
	rm -f *.def *.R *.png *.pdf *.ps
veryclean : clean
	rm -f *.exe *.dat *.ci-raw *.results binaries

binaries:
	touch binaries
# "binaries" is an empty file that's newer than the executables in
# this directory.  It is used as an empty target, and records the time
# at which this run was initiated.  (It is 'built' from the
# executables by copying them into the current directory, and touching
# 'binaries'.  This prevents new binaries from inadvertantly being
# copied into archived benchmark directories.

# Note that this makefile recursively calls itself... there must be a
# better way.  The problem is that we need to prevent make from
# realizing that we build the .exe from the original binary.
# Otherwise, it would copy over the archived binary when we recompile...
%.exe : binaries
	$(MAKE) $(MAKEFLAGS) $*.exe.tmp
	mv $*.exe.tmp $*.exe

%.exe.tmp : %
	cp $< $*.exe.tmp

benchmarks : binaries $(patsubst %.exe, %.dat, $(wildcard *.exe))
	touch benchmarks

