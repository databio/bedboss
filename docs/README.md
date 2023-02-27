# BedBoss
bedboss is a command-line pipeline that standardizes and calculates statistics for genomic interval data, and enters the results into a BEDbase database. It has 3 components: 1) bedmaker (`bedboss make`); 2) bedqc (`bedboss qc`); and 3) bedstat `bedboss stat`. You may run all 3 pipelines together,  or separately.

Mainly pipelines are intended to be run from command line but nevertheless, 
they are also available as a python function, so that user can implement them to his own code.
----
## BedBoss consist of 3 main pipelines:

### bedmaker
bedmaker - pipeline to convert supported file types* into BED format and bigBed format. Currently supported formats:
   - bedGraph
   - bigBed
   - bigWig
   - wig

### bedqc
flag bed files for further evaluation to determine whether they should be included in the downstream analysis. 
Currently, it flags bed files that are larger than 2G, has over 5 milliom regions, and/or has mean region width less than 10 bp.
This threshold can be changed in bedqc function arguments.

### bedstat

pipeline for obtaining statistics about bed files

# Additional information

## bedmaker

### Additional dependencies

- bedToBigBed: http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/bedToBigBed
- bigBedToBed: http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/bigBedToBed
- bigWigToBedGraph: http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/bigWigToBedGraph
- wigToBigWig: http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/wigToBigWig

## bedstat

### Additional dependencies
regionstat.R script is used to calculate the bed file statistics, so the pipeline also depends on several R packages:

- BiocManager
- optparse
- devtools
- GenomicRanges
- GenomicDistributions
- BSgenome.<organim>.UCSC.<genome> depending on the genome used
- LOLA
- you can use installRdeps.R helper script to easily install the required packages:

- Rscript scripts/installRdeps.R
