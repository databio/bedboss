# Bedboss
Mainly pipelines are intended to be run from command line but nevertheless, 
they are also available as a python function, so that user can implement them to his own code.

## CLI
To see your CLI options, invoke bedboss -h:
```
usage: bedboss <command> [<args>]

The commands used in bedmaker are:
    boss        Run all bedboss pipelines and insert data into bedbase.
    make        Make bed and bigBed file from other formats (bedmaker)
    qc          Run quality control on bed file (bedqc)
    stat        Run statistic calculation (bedstat)
bedboss: error: the following arguments are required: command
```
To run all pipelines together use boss as first argument:
`bedboss boss -h`

```
usage: bedboss [-h] -s SAMPLE_NAME -f INPUT_FILE -t INPUT_TYPE -o OUTPUT_FOLDER -g GENOME [-r RFG_CONFIG] [--chrom-sizes CHROM_SIZES] [-n NARROWPEAK] [--standard-chrom] [--check-qc] [--open-signal-matrix OPEN_SIGNAL_MATRIX]
               [--ensdb ENSDB] --bedbase-config BEDBASE_CONFIG [-y SAMPLE_YAML] [--no-db-commit] [--just-db-commit]

Run bedmaker, bedqc and bedstat in one pipeline.And upload all data to the bedbase

options:
  -h, --help            show this help message and exit
  -s SAMPLE_NAME, --sample-name SAMPLE_NAME
                        name of the sample used to systematically build the output name
  -f INPUT_FILE, --input-file INPUT_FILE
                        Input file
  -t INPUT_TYPE, --input-type INPUT_TYPE
                        Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
  -o OUTPUT_FOLDER, --output_folder OUTPUT_FOLDER
                        Output folder
  -g GENOME, --genome GENOME
                        reference genome (assembly)
  -r RFG_CONFIG, --rfg-config RFG_CONFIG
                        file path to the genome config file(refgenie)
  --chrom-sizes CHROM_SIZES
                        a full path to the chrom.sizes required for the bedtobigbed conversion
  -n NARROWPEAK, --narrowpeak NARROWPEAK
                        whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks)
  --standard-chrom      Standardize chromosome names. Default: False
  --check-qc            Check quality control before processing data. Default: True
  --open-signal-matrix OPEN_SIGNAL_MATRIX
                        a full path to the openSignalMatrix required for the tissue specificity plots
  --ensdb ENSDB         A full path to the ensdb gtf file required for genomes not in GDdata
  --bedbase-config BEDBASE_CONFIG
                        a path to the bedbase configuration file
  -y SAMPLE_YAML, --sample-yaml SAMPLE_YAML
                        a yaml config file with sample attributes to pass on more metadata into the database
  --no-db-commit        skip the JSON commit to the database
  --just-db-commit      just commit the JSON to the database

```

## bedmaker
`bedboss make -h`
```
usage: bedboss [-h] -f INPUT_FILE [-n NARROWPEAK] -t INPUT_TYPE -g GENOME -r RFG_CONFIG -o OUTPUT_BED --output-bigbed OUTPUT_BIGBED -s SAMPLE_NAME [--chrom-sizes CHROM_SIZES] [--standard-chrom] [-R] [-N] [-D] [-F] [-T] [--silent]
               [--verbosity V] [--logdev] [-C CONFIG_FILE] [-O PARENT_OUTPUT_FOLDER] [-M MEMORY_LIMIT] [-P NUMBER_OF_CORES]

A pipeline to convert bed, bigbed, bigwig or bedgraph files into bed and bigbed formats

options:
  -h, --help            show this help message and exit
  -f INPUT_FILE, --input-file INPUT_FILE
                        path to the input file
  -n NARROWPEAK, --narrowpeak NARROWPEAK
                        whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks)
  -t INPUT_TYPE, --input-type INPUT_TYPE
                        a bigwig or a bedgraph file that will be converted into BED format
  -g GENOME, --genome GENOME
                        reference genome
  -r RFG_CONFIG, --rfg-config RFG_CONFIG
                        file path to the genome config file
  -o OUTPUT_BED, --output-bed OUTPUT_BED
                        path to the output BED files
  --output-bigbed OUTPUT_BIGBED
                        path to the folder of output bigBed files
  -s SAMPLE_NAME, --sample-name SAMPLE_NAME
                        name of the sample used to systematically build the output name
  --chrom-sizes CHROM_SIZES
                        a full path to the chrom.sizes required for the bedtobigbed conversion
  --standard-chrom      Standardize chromosome names. Default: False
  -R, --recover         Overwrite locks to recover from previous failed run
  -N, --new-start       Overwrite all results to start a fresh run
  -D, --dirty           Don't auto-delete intermediate files
  -F, --force-follow    Always run 'follow' commands
  -T, --testmode        Only print commands, don't run
  --silent              Silence logging. Overrides verbosity.
  --verbosity V         Set logging level (1-5 or logging module level name)
  --logdev              Expand content of logging message format.
  -C CONFIG_FILE, --config CONFIG_FILE
                        Pipeline configuration file (YAML). Relative paths are with respect to the pipeline script.
  -O PARENT_OUTPUT_FOLDER, --output-parent PARENT_OUTPUT_FOLDER
                        Parent output directory of project
  -M MEMORY_LIMIT, --mem MEMORY_LIMIT
                        Memory limit for processes accepting such. Default units are megabytes unless specified using the suffix [K|M|G|T].
  -P NUMBER_OF_CORES, --cores NUMBER_OF_CORES
                        Number of cores for parallelized processes
```

## bedqc
`bedboss qc -h`

```
usage: bedboss [-h] --bedfile BEDFILE --outfolder OUTFOLDER

A pipeline for bed file QC.

options:
  -h, --help            show this help message and exit
  --bedfile BEDFILE     a full path to bed file to process
  --outfolder OUTFOLDER
                        a full path to output log folder.
```

## bedstat

`bedboss stat -h`
```
usage: bedboss [-h] --bedfile BEDFILE [--open-signal-matrix OPEN_SIGNAL_MATRIX] [--ensdb ENSDB] [--bigbed BIGBED] [--bedbase-config BEDBASE_CONFIG] [-y SAMPLE_YAML] --genome GENOME_ASSEMBLY [--no-db-commit] [--just-db-commit]

A pipeline to read a file in BED format and produce metadata in JSON format.

options:
  -h, --help            show this help message and exit
  --bedfile BEDFILE     a full path to bed file to process
  --open-signal-matrix OPEN_SIGNAL_MATRIX
                        a full path to the openSignalMatrix required for the tissue specificity plots
  --ensdb ENSDB         a full path to the ensdb gtf file required for genomes not in GDdata
  --bigbed BIGBED       a full path to the bigbed files
  --bedbase-config BEDBASE_CONFIG
                        a path to the bedbase configuration file
  -y SAMPLE_YAML, --sample-yaml SAMPLE_YAML
                        a yaml config file with sample attributes to pass on more metadata into the database
  --genome GENOME_ASSEMBLY
                        genome assembly of the sample
  --no-db-commit        whether the JSON commit to the database should be skipped
  --just-db-commit      whether just to commit the JSON to the database

```

# Additional information
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
