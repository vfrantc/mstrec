
report.pdf: *.tex *.bib
	latexmk -pdf -synctex=1 -interaction=nonstopmode report

.PHONY: clean

clean:
	rm -f *.aux *.log *.out *.snm *.toc *.vrb *.nav *.synctex.gz *.blg *.bbl *.fdb_latexmk *.fls *.ind *.idx *.ilg *.bcf *.run.xml
