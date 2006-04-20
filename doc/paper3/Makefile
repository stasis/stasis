# This is a simple, general makefile for LaTeX documents.

.SECONDARY:

all:

%: %.tex
	latex $<
	bibtex $*
	latex $<
	latex $<

%.dvi: %.tex
	latex $<
	bibtex $*
	latex $<
	latex $<

%.ps: %.dvi %.tex
	dvips -t letter -o $@ $< 

%.pdf: %.ps %.dvi %.tex
	ps2pdf $< $@

clean:
	rm -f *.aux *.log *.dvi *.bbl *.blg *~ *.ent
