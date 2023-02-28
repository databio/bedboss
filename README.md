# bedboss

---
![Run pytests](https://github.com/bedbase/bedboss/workflows/Run%20pytests/badge.svg)
[![docs-badge](https://readthedocs.org/projects/bedboss/badge/?version=latest)](https://bedboss.databio.org/en/latest/)
[![pypi-badge](https://img.shields.io/pypi/v/bedboss)](https://pypi.org/project/bedboss)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

bedboss is a command-line pipeline that standardizes and calculates statistics for genomic interval data, and enters the results into a BEDbase database. It has 3 components: 1) bedmaker (`bedboss make`); 2) bedqc (`bedboss qc`); and 3) bedstat `bedboss stat`. You may run all 3 pipelines together,  or separately.

## bedmaker

1) bedmaker - pipeline to convert supported file types* into BED format and bigBed format. Currently supported formats:
   - bedGraph
   - bigBed
   - bigWig
   - wig

## bedqc

2) bedqc - Flag bed files for further evaluation to determine whether they should be included in the downstream analysis. 
Currently, it flags bed files that are larger than 2G, has over 5 milliom regions, and/or has mean region width less than 10 bp.
This threshold can be changed in bedqc function arguments.

## bedstat

3) bedstat - for obtaining statistics about bed files.

# Documentation

Detailed information about each pipeline can be found in the [bedboss Readme](./docs/README.md).
