# bedboss

---
![Run pytests](https://github.com/bedbase/bedboss/workflows/Run%20instalation%20test/badge.svg)
[![docs-badge](https://readthedocs.org/projects/bedboss/badge/?version=latest)](https://bedboss.databio.org/en/latest/)
[![pypi-badge](https://img.shields.io/pypi/v/bedboss)](https://pypi.org/project/bedboss)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

bedboss is a command-line pipeline that standardizes and calculates statistics for genomic interval data, and enters the results into a BEDbase database. It has 3 components: 1) bedmaker (`bedboss make`); 2) bedqc (`bedboss qc`); and 3) bedstat `bedboss stat`. You may run all 3 pipelines separately, together (`bedbase all`).

## 1) bedmaker

Converts supported file types into BED and bigBed format. Currently supported formats:
   - bedGraph
   - bigBed
   - bigWig
   - wig

## 2) bedqc

Assess QC of BED files and flag potential problems for further evaluation so you can determine whether they should be included in downstream analysis. 
Currently, it flags BED files that are larger than 2 GB, have over 5 milliom regions, or have mean region width less than 10 bp.
These thresholds can be changed with pipeline arguments.

## bedstat

Calculates statistics about BED files.

# Documentation

Detailed information about each pipeline can be found in the [bedboss Readme](./docs/README.md).


Set up environment variables like this:

```
source environment/production.env
```