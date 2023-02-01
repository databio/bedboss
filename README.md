# bedboss

bedboss is a command-line tool that has standardizes and calculates statistics for genomic interval data.
It is divided into 3 parts:
1) bedmaker - pipeline to convert supported file types* into BED format and bigBed format. Currently supported formats:
   - bedGraph
   - bigBed
   - bigWig
   - wig
2) bedqc - Flag bed files for further evaluation to determine whether they should be included in the downstream analysis. 
Currently, it flags bed files that are larger than 2G, has over 5 milliom regions, and/or has mean region width less than 10 bp.
This threshold can be changed in bedqc function arguments.
3) bedstat - for obtaining statistics about bed files.

User can run all 3 pipelines together using combined bedboss script or separately.


More detailed information about each of this pipelines can be found here: [bedboss Readme](./docs/README.md)
