# Usage reference

BEDboss is command-line tool-warehouse of 3 pipelines for genomic interval files

BEDboss include: bedmaker, bedqc, bedstat. This pipelines can be run using next positional arguments:

- `bedbase all`:  Runs all pipelines one in order: bedmaker -> bedqc -> bedstat

- `bedbase make`:  Creates Bed and BigBed files from  other type of genomic interval files [bigwig|bedgraph|bed|bigbed|wig]

- `bedbase qc`: Runs Quality control for bed file (Works only with bed files)

- `bedbase stat`: Runs statistics for bed and bigbed files.

Here you can see the command-line usage instructions for the main bedboss command and for each subcommand:

## `bedboss --help`
```console
version: 0.1.0a3
usage: bedboss [-h] [--version] [--silent] [--verbosity V] [--logdev]
               {all,all-pep,make,qc,stat} ...

Warehouse of pipelines for BED-like files: bedmaker, bedstat, and bedqc.

positional arguments:
  {all,all-pep,make,qc,stat}
    all                 Run all bedboss pipelines and insert data into bedbase
    all-pep             Run all bedboss pipelines using one PEP and insert
                        data into bedbase
    make                A pipeline to convert bed, bigbed, bigwig or bedgraph
                        files into bed and bigbed formats
    qc                  Run quality control on bed file (bedqc)
    stat                A pipeline to read a file in BED format and produce
                        metadata in JSON format.

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --silent              Silence logging. Overrides verbosity.
  --verbosity V         Set logging level (1-5 or logging module level name)
  --logdev              Expand content of logging message format.
```

## `bedboss all --help`
```console
usage: bedboss all [-h] --outfolder OUTFOLDER -s SAMPLE_NAME -f INPUT_FILE -t
                   INPUT_TYPE -g GENOME [-r RFG_CONFIG]
                   [--chrom-sizes CHROM_SIZES] [-n] [--standard-chrom]
                   [--check-qc] [--open-signal-matrix OPEN_SIGNAL_MATRIX]
                   [--ensdb ENSDB] --bedbase-config BEDBASE_CONFIG
                   [-y SAMPLE_YAML] [--no-db-commit] [--just-db-commit]

options:
  -h, --help            show this help message and exit
  --outfolder OUTFOLDER
                        Pipeline output folder [Required]
  -s SAMPLE_NAME, --sample-name SAMPLE_NAME
                        name of the sample used to systematically build the
                        output name [Required]
  -f INPUT_FILE, --input-file INPUT_FILE
                        Input file [Required]
  -t INPUT_TYPE, --input-type INPUT_TYPE
                        Input type [Required] options:
                        (bigwig|bedgraph|bed|bigbed|wig)
  -g GENOME, --genome GENOME
                        reference genome (assembly) [Required]
  -r RFG_CONFIG, --rfg-config RFG_CONFIG
                        file path to the genome config file(refgenie)
  --chrom-sizes CHROM_SIZES
                        a full path to the chrom.sizes required for the
                        bedtobigbed conversion
  -n, --narrowpeak      whether it's a narrowpeak file
  --standard-chrom      Standardize chromosome names. Default: False
  --check-qc            Check quality control before processing data. Default:
                        True
  --open-signal-matrix OPEN_SIGNAL_MATRIX
                        a full path to the openSignalMatrix required for the
                        tissue specificity plots
  --ensdb ENSDB         A full path to the ensdb gtf file required for genomes
                        not in GDdata
  --bedbase-config BEDBASE_CONFIG
                        a path to the bedbase configuration file [Required]
  -y SAMPLE_YAML, --sample-yaml SAMPLE_YAML
                        a yaml config file with sample attributes to pass on
                        more metadata into the database
  --no-db-commit        skip the JSON commit to the database
  --just-db-commit      just commit the JSON to the database
```

## `bedboss all-pep --help`
```console
usage: bedboss all-pep [-h] --pep_config PEP_CONFIG

options:
  -h, --help            show this help message and exit
  --pep_config PEP_CONFIG
                        Path to the pep configuration file [Required] Required
                        fields in PEP are: sample_name, input_file,
                        input_type,outfolder, genome, bedbase_config. Optional
                        fields in PEP are: rfg_config, narrowpeak, check_qc,
                        standard_chrom, chrom_sizes, open_signal_matrix,
                        ensdb, sample_yaml, no_db_commit, just_db_commit,
                        no_db_commit, force_overwrite, skip_qdrant
```

## `bedboss make --help`
```console
usage: bedboss make [-h] -f INPUT_FILE --outfolder OUTFOLDER [-n] -t
                    INPUT_TYPE -g GENOME [-r RFG_CONFIG] -o OUTPUT_BED
                    --output-bigbed OUTPUT_BIGBED -s SAMPLE_NAME
                    [--chrom-sizes CHROM_SIZES] [--standard-chrom]

options:
  -h, --help            show this help message and exit
  -f INPUT_FILE, --input-file INPUT_FILE
                        path to the input file [Required]
  --outfolder OUTFOLDER
                        Pipeline output folder [Required]
  -n, --narrowpeak      whether it's a narrowpeak file
  -t INPUT_TYPE, --input-type INPUT_TYPE
                        input file format (supported formats: bedGraph,
                        bigBed, bigWig, wig) [Required]
  -g GENOME, --genome GENOME
                        reference genome [Required]
  -r RFG_CONFIG, --rfg-config RFG_CONFIG
                        file path to the genome config file
  -o OUTPUT_BED, --output-bed OUTPUT_BED
                        path to the output BED files [Required]
  --output-bigbed OUTPUT_BIGBED
                        path to the folder of output bigBed files [Required]
  -s SAMPLE_NAME, --sample-name SAMPLE_NAME
                        name of the sample used to systematically build the
                        output name [Required]
  --chrom-sizes CHROM_SIZES
                        whether standardize chromosome names. If ture,
                        bedmaker will remove the regions on ChrUn chromosomes,
                        such as chrN_random and chrUn_random. [Default: False]
  --standard-chrom      Standardize chromosome names. Default: False
```

## `bedboss qc --help`
```console
usage: bedboss qc [-h] --bedfile BEDFILE --outfolder OUTFOLDER

options:
  -h, --help            show this help message and exit
  --bedfile BEDFILE     a full path to bed file to process [Required]
  --outfolder OUTFOLDER
                        a full path to output log folder. [Required]
```

## `bedboss stat --help`
```console
usage: bedboss stat [-h] --bedfile BEDFILE --outfolder OUTFOLDER
                    [--open-signal-matrix OPEN_SIGNAL_MATRIX] [--ensdb ENSDB]
                    [--bigbed BIGBED] --bedbase-config BEDBASE_CONFIG
                    [-y SAMPLE_YAML] --genome GENOME [--no-db-commit]
                    [--just-db-commit]

options:
  -h, --help            show this help message and exit
  --bedfile BEDFILE     a full path to bed file to process [Required]
  --outfolder OUTFOLDER
                        Pipeline output folder [Required]
  --open-signal-matrix OPEN_SIGNAL_MATRIX
                        a full path to the openSignalMatrix required for the
                        tissue specificity plots
  --ensdb ENSDB         a full path to the ensdb gtf file required for genomes
                        not in GDdata
  --bigbed BIGBED       a full path to the bigbed files
  --bedbase-config BEDBASE_CONFIG
                        a path to the bedbase configuration file [Required]
  -y SAMPLE_YAML, --sample-yaml SAMPLE_YAML
                        a yaml config file with sample attributes to pass on
                        more metadata into the database
  --genome GENOME       genome assembly of the sample [Required]
  --no-db-commit        whether the JSON commit to the database should be
                        skipped
  --just-db-commit      whether just to commit the JSON to the database
```
