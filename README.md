# bedboss

[![PEP compatible](https://pepkit.github.io/img/PEP-compatible-green.svg)](https://pep.databio.org/)
![Run pytests](https://github.com/bedbase/bedboss/workflows/Run%20instalation%20test/badge.svg)
[![pypi-badge](https://img.shields.io/pypi/v/bedboss?color=%2334D058)](https://pypi.org/project/bedboss)
[![pypi-version](https://img.shields.io/pypi/pyversions/bedboss.svg?color=%2334D058)](https://pypi.org/project/bedboss)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Github badge](https://img.shields.io/badge/source-github-354a75?logo=github)](https://github.com/databio/bedboss)

---

**Documentation**: <a href="https://docs.bedbase.org/bedboss" target="_blank">https://docs.bedbase.org/bedboss</a>

**Source Code**: <a href="https://github.com/databio/bedboss" target="_blank">https://github.com/databio/bedboss</a>

---

bedboss is a command-line pipeline that filters, standardizes, and calculates statistics for genomic interval data, 
and enters the results into a BEDbase database. 

## Installation
To install `bedboss` use this command: 
```
pip install bedboss
```
or install the latest version from the GitHub repository:
```
pip install git+https://github.com/databio/bedboss.git
```

## Testing

### Requirements test:

To test requirements, install bedboss and run: 

```
bedboss requirements-check
```

### Smoke tests:

Use this docs:
- [./test/README.md](./test/README.md)


## How to generate usage documentation:

Run this command in the root of the repository:
```
cd scripts
bash update_usage_docs.sh
```