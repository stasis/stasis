# This is a simple, general makefile for LaTeX documents.

.SECONDARY:

all: LLADD.pdf

#%: %.tex %.bib
#	latex $<
#	bibtex $*
#	latex $<
#	latex $<

%.dvi: %.tex %.bib
	touch $*.ent
	latex $*.tex
	bibtex $*
	latex $*.tex
	latex $*.tex

%.ps: %.dvi %.tex
	dvips -t letter -o $@ $< 

%.pdf: %.tex %.bib
	touch $*.ent
	pdflatex $*.tex
	bibtex $*
	pdflatex $*.tex
	pdflatex $*.tex
#	ps2pdf $< $@

%.html: %.tex %.bib
	htlatex $*.tex

clean:
	rm -f *.aux *.log *.dvi *.bbl *.blg *~ *.ent *.4ct *.xref *.4tc *.lg *.idv LLADD*x.png *.tmp

