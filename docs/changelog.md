# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.


# [0.14.0] - 2026-11-12
## Changed:
- Reference genomes are now sourced from a single canonical registry (`hg38`, `hg19`, `mm10`, one seqcol digest each). bigBed generation reuses the chrom sizes already loaded by the reference-genome validator (passed directly to `RegionSet.to_bigbed` as a dict — no temporary chrom.sizes file) instead of looking them up through `refgenconf`.
- Migrated genome-FASTA retrieval (used for bedstat GC content) to the new `refgenie` 1.0 package. The FASTA is pulled on demand for the canonical statistics genomes only; other genomes skip GC content. The `refgenie` client configures itself from the environment (`REFGENIE_HOME_PATH`) and initializes on first use — no manual `refgenie init` required.

## Removed:
- Dropped the `refgenconf` dependency (replaced by `refgenie>=1.0.0a1`).
- Removed the `--rfg-config` CLI option and the `rfg_config` parameter throughout the pipeline. Reference genome configuration is now handled by refgenie's own environment configuration.
# [0.13.0] - 2026-11-21
## Changed:
- `bedms` is now an optional extra: `pip install "bedboss[standardize]"`. It is imported only when a PEP is standardized, so a base install has no torch from bedMS.
- `matplotlib` and `seaborn` (used by bedstat) are now listed as dependencies; they used to arrive through `bbconf` -> `geniml[ml]`.
- Requires `bbconf>=0.17.0`. Embeddings can come from servers set in the config's new `embedding_server:` block; models are still named in `path:`. `save_example_bedbase_config` writes both blocks. In-process models need `bbconf[local]`.
- Short genome names that bedboss does not recognize are now stored as `undefined`. Names longer than 10 characters, such as genome digests, are kept as given.

## Fixed:
- The initial QC of GEO files reads them over https instead of ftp. Each check took about a minute over ftp and now takes under a second, which made the daily GEO upload many times slower than it needed to be.

# [0.12.2] - 2026-10-20
## Fixed:
- `bedboss geo upload-all` and `upload-gse` now stop with an error on a database or S3 error instead of logging it and moving on. Before, a broken database schema made every insert fail while the job still exited 0.

# [0.12.1] - 2026-10-16
## Fixed:
- `bedboss snapshot new` crashed on the bedsets export under SQLAlchemy 2.1, which reports JSON columns as `object` instead of `dict`. JSON columns are now detected by type.
- `bedboss snapshot new` turns off `statement_timeout` on its export connection, so long table scans are not cancelled by the app role's limit.

# [0.12.0] - 2026-08-18
## Changes:
- All references to `upload_pephub` have been removed from function signatures, docstrings, and internal logic across the codebase, including in `bedboss.py`, `bedbuncher.py`, `bbuploader/main.py`, `bedboss_hpc.py`, and the CLI (`cli.py`). This means users can no longer upload to PEPHub using any part of the pipeline. 
- Removed unused logging setup from `bedboss/bbuploader/__init__.py` and added `logmuse` import to `cli.py` for consistent logging across the CLI. 
- Fixed a bug in `_read_bed_file` in `bedclassifier.py` to only drop columns that are entirely NA, instead of any column with any NA.
- Added import for `snapshot_app` in `cli.py` from `bedboss.scripts.snapshot`.
- Added Analysis files functionality.

# [0.11.1] - 2026-04-22
## Changed:
- Migrated to new yacman version 1.0.0


# [0.11.0] - 2026-04-17
## Added:
- HPC module for running bedboss on HPC
- CLI tool for bulk uploading files to Qdrant

## Fixed:
- Bed classifier
- DB connection


# [0.10.0] - 2026-04-05
## Added:
- Creation of parquet file for umap with more metadata inside

## Changed:
- Testing version of new peppy
- New project style
- Modernized docstrings and typing


# [0.9.6] - 2026-02-18
## Added:
- Added scripts for downloading and analyzing ENCODE metadata

## Changed:
- Updated pep processing in GEO uploader and bedboss
- Updated UMAP creation


# [0.9.5] - 2026-02-16
## Changed:
- Improved metadata extractor with expanded cell line and assay extraction
- Updated UMAP creation scripts
- Updated requirements


# [0.9.4] - 2026-02-06
## Fixed:
- Fixed large downloads of embeddings from qdrant


# [0.9.3] - 2026-02-04
## Changed:
- Improved genome predictor
- Updated bbconf version requirement

## Fixed:
- Added file check for GEO files that don't have size in their soft data


# [0.9.2] - 2026-01-31
## Added:
- Added file size check for project-level files in GEO uploader


# [0.9.1] - 2026-01-31
## Changed:
- Removed unpredictable cell lines from metadata extractor


# [0.9.0] - 2026-01-31
## Added:
- Processing of gse series bed files #143
- Added metadata extraction from description if cell line or assay is not avaliable

## Changed:
- Improved reference genome predictor

## Fixed:
- Fix reference genome update and upload in bedbase db #144

# [0.8.2] - 2025-09-22
## Fixed:
- Downloading Refgenie chrom sizes (due to endpoint change)


# [0.8.1] - 2025-09-12
## Fixed:
- Creation of UMAP 

# [0.8.0] - 2025-09-11
## Added:

- Added CLI for automatic UMAP generation from qdrant
- Added "generate UMAP" cli command

## Changed:
- Changed data source of chrom sizes in bed reference validator to new refgenie.

## Fixed:
- Fixed bedboss logger
- Improved vector db reindexing
- Improved efficiency of bdd Reference validator function


# [0.7.3] - 2025-06-28
## Added:
- Added filter for rerunning unprocessed bed files based on genome


# [0.7.2] - 2025-06-21
## Changed:
- Updated path to the bigbed output folder
- Added update of metadata of the bed file
- Updated docker file 
- Remove bedqc module


# [0.7.1] - 2025-04-22
## Fixed
- Minor bug fixes

# [0.7.0] - 2025-04-21

## Added
- Added dockerfile
- Added R processing as separate service
- Added initial qc for GEO data (QC, without loading full data)
- Refactored R code
- Added summary for bedsets #112 
- Added original submission date of geo to the table #95 

## Changed:
- Improved logging
- Fixed python3.12 warnings
- Improved bedmaker. (identifier, bigbed, qc)
- Removed bedqc module - moved it to handle everything by RegionSet object
- Improved bedclasifier #101
- Fixed #97 
- Fixed #110
- Fixed multiple bugs in geo uploader
- Switched RegionSet from geniml to RegionSet from Gtars

# [0.6.0] - 2025-01-17

## Added:
- Added open_chromatin plot back into processing.
- Added gtrs dependency, that calculates gc content.
- Added skipper that automatically skips samples in pep that were already processed.
- Added lite functionality to main functions that allows to run uploading without using any heavy processing.
- Added function that will reprocess files, if they were unprocessed in the bedbase.
- Added function that predicts genome if genome wasn't provided

## Changed:
- Important speed improvements.



# [0.5.0] - 2025-01-16

## Added

- Added open_chromatin plot back into processing.
- Added gtrs dependency, that calculates gc content.
- Added skipper that automatically skips samples in pep that were already processed.
- Added lite functionality to main functions that allows to run uploading without using any heavy processing.
- Added function that will reprocess files, if they were unprocessed in the bedbase.
- Added function that predicts genome if genome wasn't provided.

## Fixes
- Important speed improvements.
- Improved requirements checker.
- Minor bug fixes.

# [0.4.1] - 2024-09-20
## Added
- Standardization of peps using bedbase bedms schema
- Reference validator module

## Fixed
- Pipeline failures (due to pipeline manager)
- Failure in cleaning temp files


# [0.4.0] - 2024-08-26
## Added
- Added bbuploader (GEO uploader)

# [0.3.0] - 2024-08-21
## Added
- Added classifier
- Added create universe uploader
- Added tokenization and token uploader

## Changes
- Updated efficiency of CLI help


# [0.2.1] - 2024-04-09
## Changed
- small naming tweaks

## Added
- added requirement check to cli


# [0.2.0] - 2024-04-08
## Changed
- moved all uploading functionality to the `bbconf` package

## Added
- added commands for indexing bedfiles
- added commands for deleting bedfiles and bedsets


## [0.1.0] - 2024-01-26
### Added
- Initial alpha release
