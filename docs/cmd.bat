REM pandoc -s doc.rst -o doc_simple.html
pandoc -s --toc -H header.html -B top.html -A footer.html doc.rst -o doc.html