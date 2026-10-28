<h1 align="center">bedboss</h1>

<div align="center">
  
[![PEP compatible](https://pepkit.github.io/img/PEP-compatible-green.svg)](https://pep.databio.org/)

[//]: # (![Run pytests]&#40;https://github.com/bedbase/bedboss/workflows/Run%20instalation%20test/badge.svg&#41;)
[![pypi-badge](https://img.shields.io/pypi/v/bedboss?color=%2334D058)](https://pypi.org/project/bedboss)
[![pypi-version](https://img.shields.io/pypi/pyversions/bedboss.svg?color=%2334D058)](https://pypi.org/project/bedboss)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Github badge](https://img.shields.io/badge/source-github-354a75?logo=github)](https://github.com/databio/bedboss)

</div>

---

**Documentation**: <a href="https://docs.bedbase.org/bedboss" target="_blank">https://docs.bedbase.org/bedboss</a>

**Source Code**: <a href="https://github.com/databio/bedboss" target="_blank">https://github.com/databio/bedboss</a>

---

BEDboss is a command-line management tool for BEDbase. It contains pipelines that filters, standardizes, and calculates statistics for genomic interval data, 
functions that enters the results into a BEDbase database, deletes bed and bedsets from the database, and indexes the data to qdrant.

## Installation
To install `bedboss` use this command: 
```
pip install bedboss
```
or install the latest version from the GitHub repository:
```
pip install git+https://github.com/databio/bedboss.git
```

Some commands need optional extras:

- `--standardize-pep` (bedMS standardization) needs `pip install "bedboss[standardize]"` (pulls in torch).
- Commands that embed BED files or text (`bedboss reindex`, `bedboss reindex-text`, and the Qdrant
  upload commands in `bedboss/qdrant_index/`) need in-process models (`pip install "bbconf[local]"`)
  or a bedbase config whose `embedding:` block points at embedding servers (see the bbconf README).
  Bulk reindexing should use in-process models.
- The HPC vectorize step (`bedboss/qdrant_index/vectorize.py`) always runs models in-process, and the
  UMAP script (`bedboss/scripts/make_umap.py`) fits a UMAP model, so they need `pip install "bbconf[local,umap]"`.

## Development
For development, you should install all the dependencies, create a virtual environment, and work on the local database.
The workflow is described in the [development documentation](https://docs.bedbase.org/bedboss/development).


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
