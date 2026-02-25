import camelot as cm
tables=cm.read_pdf(r"file:///C:/Users/tradelist/Desktop/DATA/BENG/EG232-ENGINNEERING%20MEASUREMENT%20SYSTEMS/EG%20232/PAPERS/TEST%20ONE/eg232%20papers.pdf",pages="11",flavor="stream")
print(tables)
