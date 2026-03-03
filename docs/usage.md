# Usage reference

BEDboss is command-line tool-manager and a set of tools for working with BED files and BEDbase. Main components of BEDboss are:
1) Pipeline for processing BED files: bedmaker, bedqc, and bedstats.
2) Indexing of the Bed files in bedbase
3) Managing bed and bedsets in the database

Here you can see the command-line usage instructions for the main bedboss command and for each subcommand:

## `bedboss --help`
```console
                                                                                
 Usage: bedboss [OPTIONS] COMMAND [ARGS]...                                     
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --version             -v        App version                                  │
│ --install-completion            Install completion for the current shell.    │
│ --show-completion               Show completion for the current shell, to    │
│                                 copy it or customize the installation.       │
│ --help                          Show this message and exit.                  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ run-all           Run all the bedboss pipeline for a single bed file         │
│ run-pep           Run the all bedboss pipeline for a bed files in a PEP      │
│ reprocess-all     Run unprocessed files or reprocess them. Currently, only   │
│                   hg38, hg19, and mm10 genomes are supported.                │
│ reprocess-one     Run unprocessed file, or reprocess it [Only 1 file]        │
│ reprocess-bedset  Reprocess a bedset                                         │
│ make-bed          Create a bed files form a  file                            │
│ make-bigbed       Create a bigbed files form a bed file                      │
│ run-stats         Create the statistics for a single bed file.               │
│ reindex           Reindex the bedbase database and insert all files to the   │
│                   qdrant database.                                           │
│ reindex-text      Reindex semantic (text) search.                            │
│ make-bedset       Create a bedset from a pep file, and insert it to the      │
│                   bedbase database.                                          │
│ init-config       Initialize the new, sample configuration file              │
│ delete-bed        Delete bed from the bedbase database                       │
│ delete-bedset     Delete BedSet from the bedbase database                    │
│ tokenize-bed      Tokenize a bedfile                                         │
│ delete-tokenized  Delete tokenized bed file                                  │
│ convert-universe  Convert bed file to universe                               │
│ update-genomes    Update reference genomes in the database                   │
│ download-umap     Download UMAP                                              │
│ prep              Download and pre-compile reference files for a genome      │
│ verify-config     Verify configuration file                                  │
│ geo               Automatic BEDbase uploader for GEO data                    │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss run-all --help`
```console
                                                                                
 Usage: bedboss run-all [OPTIONS]                                               
                                                                                
 Run all the bedboss pipeline for a single bed file                             
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --input-file                                TEXT     Path to the input    │
│                                                         file                 │
│                                                         [required]           │
│ *  --input-type                                TEXT     Type of the input    │
│                                                         file. Options are:   │
│                                                         bigwig, bedgraph,    │
│                                                         bed, bigbed, wig     │
│                                                         [required]           │
│ *  --outfolder                                 TEXT     Path to the output   │
│                                                         folder               │
│                                                         [required]           │
│ *  --genome                                    TEXT     Genome name.         │
│                                                         Example: 'hg38'      │
│                                                         [required]           │
│ *  --bedbase-config                            TEXT     Path to the bedbase  │
│                                                         config file          │
│                                                         [required]           │
│    --license-id                                TEXT     License ID. If not   │
│                                                         provided for in      │
│                                                         PEPfor each bed      │
│                                                         file, this license   │
│                                                         will be used         │
│                                                         [default:            │
│                                                         DUO:0000042]         │
│    --rfg-config                                TEXT     Path to the rfg      │
│                                                         config file          │
│    --narrowpeak          --no-narrowpeak                Is the input file a  │
│                                                         narrowpeak file?     │
│                                                         [default:            │
│                                                         no-narrowpeak]       │
│    --check-qc            --no-check-qc                  Check the quality of │
│                                                         the input file?      │
│                                                         [default: check-qc]  │
│    --chrom-sizes                               TEXT     Path to the chrom    │
│                                                         sizes file           │
│    --ensdb                                     TEXT     Path to GTF file or  │
│                                                         pre-compiled .bin    │
│                                                         (run 'gtars prep     │
│                                                         --gtf' first for     │
│                                                         faster batch         │
│                                                         processing)          │
│    --open-signal-mat…                          TEXT     Path to open signal  │
│                                                         matrix file or       │
│                                                         pre-compiled .bin    │
│    --just-db-commit      --no-just-db-comm…             Just commit to the   │
│                                                         database?            │
│                                                         [default:            │
│                                                         no-just-db-commit]   │
│    --force-overwrite     --no-force-overwr…             Force overwrite the  │
│                                                         output files         │
│                                                         [default:            │
│                                                         no-force-overwrite]  │
│    --update              --no-update                    Update the bedbase   │
│                                                         database with the    │
│                                                         new record if it     │
│                                                         exists. This         │
│                                                         overwrites           │
│                                                         'force_overwrite'    │
│                                                         option               │
│                                                         [default: no-update] │
│    --lite                --no-lite                      Run the pipeline in  │
│                                                         lite mode. [Default: │
│                                                         False]               │
│                                                         [default: no-lite]   │
│    --upload-qdrant       --no-upload-qdrant             Upload to Qdrant     │
│                                                         [default:            │
│                                                         no-upload-qdrant]    │
│    --upload-pephub       --no-upload-pephub             Upload to PEPHub     │
│                                                         [default:            │
│                                                         no-upload-pephub]    │
│    --precision                                 INTEGER  Decimal places for   │
│                                                         rounding float       │
│                                                         statistics (use -1   │
│                                                         to disable)          │
│                                                         [default: 3]         │
│    --universe            --no-universe                  Create a universe    │
│                                                         [default:            │
│                                                         no-universe]         │
│    --universe-method                           TEXT     Method used to       │
│                                                         create the universe  │
│    --universe-bedset                           TEXT     Bedset used used to  │
│                                                         create the universe  │
│    --multi               --no-multi                     Run multiple samples │
│                                                         [default: no-multi]  │
│    --recover             --no-recover                   Recover from         │
│                                                         previous run         │
│                                                         [default: recover]   │
│    --dirty               --no-dirty                     Run without removing │
│                                                         existing files       │
│                                                         [default: no-dirty]  │
│    --help                                               Show this message    │
│                                                         and exit.            │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss run-pep --help`
```console
                                                                                
 Usage: bedboss run-pep [OPTIONS]                                               
                                                                                
 Run the all bedboss pipeline for a bed files in a PEP                          
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --pep                                       TEXT     PEP file. Local or   │
│                                                         remote path          │
│                                                         [required]           │
│ *  --outfolder                                 TEXT     Path to the output   │
│                                                         folder               │
│                                                         [required]           │
│ *  --bedbase-config                            TEXT     Path to the bedbase  │
│                                                         config file          │
│                                                         [required]           │
│    --create-bedset      --no-create-bedset              Create a new bedset  │
│                                                         [default:            │
│                                                         create-bedset]       │
│    --bedset-id                                 TEXT     Bedset ID            │
│    --rfg-config                                TEXT     Path to the rfg      │
│                                                         config file          │
│    --check-qc           --no-check-qc                   Check the quality of │
│                                                         the input file?      │
│                                                         [default: check-qc]  │
│    --ensdb                                     TEXT     Path to the EnsDb    │
│                                                         database file        │
│    --just-db-commit     --no-just-db-commit             Just commit to the   │
│                                                         database?            │
│                                                         [default:            │
│                                                         no-just-db-commit]   │
│    --force-overwrite    --no-force-overwri…             Force overwrite the  │
│                                                         output files         │
│                                                         [default:            │
│                                                         no-force-overwrite]  │
│    --update             --no-update                     Update the bedbase   │
│                                                         database with the    │
│                                                         new record if it     │
│                                                         exists. This         │
│                                                         overwrites           │
│                                                         'force_overwrite'    │
│                                                         option               │
│                                                         [default: no-update] │
│    --upload-qdrant      --no-upload-qdrant              Upload to Qdrant     │
│                                                         [default:            │
│                                                         upload-qdrant]       │
│    --upload-pephub      --no-upload-pephub              Upload to PEPHub     │
│                                                         [default:            │
│                                                         upload-pephub]       │
│    --no-fail            --no-no-fail                    Do not fail on error │
│                                                         [default:            │
│                                                         no-no-fail]          │
│    --license-id                                TEXT     License ID           │
│                                                         [default:            │
│                                                         DUO:0000042]         │
│    --standardize-pep    --no-standardize-p…             Standardize the PEP  │
│                                                         using bedMS          │
│                                                         [default:            │
│                                                         no-standardize-pep]  │
│    --lite               --no-lite                       Run the pipeline in  │
│                                                         lite mode. [Default: │
│                                                         False]               │
│                                                         [default: no-lite]   │
│    --rerun              --no-rerun                      Rerun already        │
│                                                         processed samples    │
│                                                         [default: no-rerun]  │
│    --precision                                 INTEGER  Decimal places for   │
│                                                         rounding float       │
│                                                         statistics (use -1   │
│                                                         to disable)          │
│                                                         [default: 3]         │
│    --multi              --no-multi                      Run multiple samples │
│                                                         [default: no-multi]  │
│    --recover            --no-recover                    Recover from         │
│                                                         previous run         │
│                                                         [default: recover]   │
│    --dirty              --no-dirty                      Run without removing │
│                                                         existing files       │
│                                                         [default: no-dirty]  │
│    --help                                               Show this message    │
│                                                         and exit.            │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss reprocess-all --help`
```console
                                                                                
 Usage: bedboss reprocess-all [OPTIONS]                                         
                                                                                
 Run unprocessed files or reprocess them. Currently, only hg38, hg19, and mm10  
 genomes are supported.                                                         
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                    TEXT     Path to the bedbase config   │
│                                                 file                         │
│                                                 [required]                   │
│ *  --outfolder                         TEXT     Path to the output folder    │
│                                                 [required]                   │
│    --limit                             INTEGER  Limit the number of files to │
│                                                 reprocess                    │
│                                                 [default: 100]               │
│    --no-fail           --no-no-fail             Do not fail on error         │
│                                                 [default: no-fail]           │
│    --help                                       Show this message and exit.  │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss reprocess-one --help`
```console
                                                                                
 Usage: bedboss reprocess-one [OPTIONS]                                         
                                                                                
 Run unprocessed file, or reprocess it [Only 1 file]                            
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                    TEXT  Path to the bedbase config file │
│                                              [required]                      │
│ *  --outfolder                         TEXT  Path to the output folder       │
│                                              [required]                      │
│ *  --identifier                        TEXT  Identifier of the bed file      │
│                                              [required]                      │
│    --multi             --no-multi            Run multiple samples            │
│                                              [default: multi]                │
│    --recover           --no-recover          Recover from previous run       │
│                                              [default: recover]              │
│    --dirty             --no-dirty            Run without removing existing   │
│                                              files                           │
│                                              [default: no-dirty]             │
│    --help                                    Show this message and exit.     │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss reprocess-bedset --help`
```console
                                                                                
 Usage: bedboss reprocess-bedset [OPTIONS]                                      
                                                                                
 Reprocess a bedset                                                             
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                    TEXT  Path to the bedbase config file │
│                                              [required]                      │
│ *  --outfolder                         TEXT  Path to the output folder       │
│                                              [required]                      │
│ *  --identifier                        TEXT  Bedset ID [required]            │
│    --no-fail           --no-no-fail          Do not fail on error            │
│                                              [default: no-fail]              │
│    --help                                    Show this message and exit.     │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss make-bed --help`
```console
                                                                                
 Usage: bedboss make-bed [OPTIONS]                                              
                                                                                
 Create a bed files form a  file                                                
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --input-file                        TEXT  Path to the input file          │
│                                              [required]                      │
│ *  --input-type                        TEXT  Type of the input file. Options │
│                                              are: bigwig, bedgraph, bed,     │
│                                              bigbed, wig                     │
│                                              [required]                      │
│ *  --outfolder                         TEXT  Path to the output folder       │
│                                              [required]                      │
│ *  --genome                            TEXT  Genome name. Example: 'hg38'    │
│                                              [required]                      │
│    --rfg-config                        TEXT  Path to the rfg config file     │
│    --narrowpeak     --no-narrowpeak          Is the input file a narrowpeak  │
│                                              file?                           │
│                                              [default: no-narrowpeak]        │
│    --chrom-sizes                       TEXT  Path to the chrom sizes file    │
│    --multi          --no-multi               Run multiple samples            │
│                                              [default: no-multi]             │
│    --recover        --no-recover             Recover from previous run       │
│                                              [default: recover]              │
│    --dirty          --no-dirty               Run without removing existing   │
│                                              files                           │
│                                              [default: no-dirty]             │
│    --help                                    Show this message and exit.     │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss make-bigbed --help`
```console
                                                                                
 Usage: bedboss make-bigbed [OPTIONS]                                           
                                                                                
 Create a bigbed files form a bed file                                          
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-file          TEXT  Path to the input file [required]               │
│ *  --outfolder         TEXT  Path to the output folder [required]            │
│ *  --genome            TEXT  Genome name. Example: 'hg38' [required]         │
│    --rfg-config        TEXT  Path to the rfg config file                     │
│    --help                    Show this message and exit.                     │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss run-stats --help`
```console
                                                                                
 Usage: bedboss run-stats [OPTIONS]                                             
                                                                                
 Create the statistics for a single bed file.                                   
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-file                                     TEXT  Path to the bed file │
│                                                         [required]           │
│ *  --genome                                       TEXT  Genome name.         │
│                                                         Example: 'hg38'      │
│                                                         [required]           │
│ *  --outfolder                                    TEXT  Path to the output   │
│                                                         folder               │
│                                                         [required]           │
│    --ensdb                                        TEXT  Path to GTF file or  │
│                                                         pre-compiled .bin    │
│                                                         (run 'gtars prep     │
│                                                         --gtf' first for     │
│                                                         faster batch         │
│                                                         processing)          │
│    --chrom-sizes                                  TEXT  Path to the chrom    │
│                                                         sizes file           │
│    --open-signal-matrix                           TEXT  Path to open signal  │
│                                                         matrix file or       │
│                                                         pre-compiled .bin    │
│    --just-db-commit        --no-just-db-commit          Just commit to the   │
│                                                         database?            │
│                                                         [default:            │
│                                                         no-just-db-commit]   │
│    --multi                 --no-multi                   Run multiple samples │
│                                                         [default: no-multi]  │
│    --recover               --no-recover                 Recover from         │
│                                                         previous run         │
│                                                         [default: recover]   │
│    --dirty                 --no-dirty                   Run without removing │
│                                                         existing files       │
│                                                         [default: no-dirty]  │
│    --help                                               Show this message    │
│                                                         and exit.            │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss reindex --help`
```console
                                                                                
 Usage: bedboss reindex [OPTIONS]                                               
                                                                                
 Reindex the bedbase database and insert all files to the qdrant database.      
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                  TEXT     Path to the bedbase config     │
│                                               file                           │
│                                               [required]                     │
│    --purge             --no-purge             Purge existing index before    │
│                                               reindexing                     │
│                                               [default: no-purge]            │
│    --batch                           INTEGER  Number of items to upload in   │
│                                               one batch                      │
│                                               [default: 1000]                │
│    --help                                     Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss reindex-text --help`
```console
                                                                                
 Usage: bedboss reindex-text [OPTIONS]                                          
                                                                                
 Reindex semantic (text) search.                                                
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                  TEXT     Path to the bedbase config     │
│                                               file                           │
│                                               [required]                     │
│    --purge             --no-purge             Purge existing index before    │
│                                               reindexing                     │
│                                               [default: no-purge]            │
│    --batch                           INTEGER  Number of items to upload in   │
│                                               one batch                      │
│                                               [default: 1000]                │
│    --help                                     Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss make-bedset --help`
```console
                                                                                
 Usage: bedboss make-bedset [OPTIONS]                                           
                                                                                
 Create a bedset from a pep file, and insert it to the bedbase database.        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --pep                                        TEXT  PEP file. Local or     │
│                                                       remote path            │
│                                                       [required]             │
│ *  --outfolder                                  TEXT  Path to the output     │
│                                                       folder                 │
│                                                       [required]             │
│ *  --bedbase-config                             TEXT  Path to the bedbase    │
│                                                       config file            │
│                                                       [required]             │
│ *  --bedset-name                                TEXT  Name of the bedset     │
│                                                       [required]             │
│    --force-overwrite    --no-force-overwrite          Force overwrite the    │
│                                                       output files           │
│                                                       [default:              │
│                                                       no-force-overwrite]    │
│    --upload-pephub      --no-upload-pephub            Upload to PEPHub       │
│                                                       [default:              │
│                                                       no-upload-pephub]      │
│    --no-fail            --no-no-fail                  Do not fail on error   │
│                                                       [default: no-no-fail]  │
│    --help                                             Show this message and  │
│                                                       exit.                  │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss init-config --help`
```console
                                                                                
 Usage: bedboss init-config [OPTIONS]                                           
                                                                                
 Initialize the new, sample configuration file                                  
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --outfolder        TEXT  Path to the output folder [required]             │
│    --help                   Show this message and exit.                      │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss delete-bed --help`
```console
                                                                                
 Usage: bedboss delete-bed [OPTIONS]                                            
                                                                                
 Delete bed from the bedbase database                                           
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --sample-id        TEXT  Sample ID [required]                             │
│ *  --config           TEXT  Path to the bedbase config file [required]       │
│    --help                   Show this message and exit.                      │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss delete-bedset --help`
```console
                                                                                
 Usage: bedboss delete-bedset [OPTIONS]                                         
                                                                                
 Delete BedSet from the bedbase database                                        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --identifier        TEXT  BedSet ID [required]                            │
│ *  --config            TEXT  Path to the bedbase config file [required]      │
│    --help                    Show this message and exit.                     │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss tokenize-bed --help`
```console
                                                                                
 Usage: bedboss tokenize-bed [OPTIONS]                                          
                                                                                
 Tokenize a bedfile                                                             
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-id                              TEXT  Path to the bed file          │
│                                                [required]                    │
│ *  --universe-id                         TEXT  Universe ID [required]        │
│    --cache-folder                        TEXT  Path to the cache folder      │
│    --add-to-db         --no-add-to-db          Add the tokenized bed file to │
│                                                the bedbase database          │
│                                                [default: no-add-to-db]       │
│    --bedbase-config                      TEXT  Path to the bedbase config    │
│                                                file                          │
│    --overwrite         --no-overwrite          Overwrite the existing        │
│                                                tokenized bed file            │
│                                                [default: no-overwrite]       │
│    --help                                      Show this message and exit.   │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss delete-tokenized --help`
```console
                                                                                
 Usage: bedboss delete-tokenized [OPTIONS]                                      
                                                                                
 Delete tokenized bed file                                                      
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --universe-id        TEXT  Universe ID [required]                         │
│ *  --bed-id             TEXT  Bed ID [required]                              │
│    --config             TEXT  Path to the bedbase config file                │
│    --help                     Show this message and exit.                    │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss convert-universe --help`
```console
                                                                                
 Usage: bedboss convert-universe [OPTIONS]                                      
                                                                                
 Convert bed file to universe                                                   
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-id        TEXT  Path to the bed file [required]                     │
│ *  --config        TEXT  Path to the bedbase config file [required]          │
│    --method        TEXT  Method used to create the universe                  │
│    --bedset        TEXT  Bedset used to create the universe                  │
│    --help                Show this message and exit.                         │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss update-genomes --help`
```console
                                                                                
 Usage: bedboss update-genomes [OPTIONS]                                        
                                                                                
 Update reference genomes in the database                                       
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --config        TEXT  Path to the bedbase config file [required]          │
│    --help                Show this message and exit.                         │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss download-umap --help`
```console
                                                                                
 Usage: bedboss download-umap [OPTIONS]                                         
                                                                                
 Download UMAP                                                                  
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --config                TEXT     Path to the bedbase config file          │
│                                     [required]                               │
│ *  --output-file           TEXT     Path to the output json file where UMAP  │
│                                     embeddings will be saved. (*Python       │
│                                     version will be added to the filename)   │
│                                     [required]                               │
│    --n-components          INTEGER  Number of UMAP components [default: 2]   │
│    --plot-name             TEXT     Name of the plot file                    │
│    --plot-label            TEXT     Label for the plot                       │
│    --top-assays            INTEGER  Number of top assays to include          │
│                                     [default: 15]                            │
│    --top-cell-lines        INTEGER  Number of top cell lines to include      │
│                                     [default: 15]                            │
│    --method                TEXT     Dimensionality reduction method to use.  │
│                                     Options: 'umap', 'pca', or 'tsne'. To    │
│                                     use UMAP, 'umap-learn' package must be   │
│                                     installed.                               │
│                                     [default: umap]                          │
│    --help                           Show this message and exit.              │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss prep --help`
```console

 Usage: bedboss prep [OPTIONS]

 Download and pre-compile reference files for a genome

╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --genome                                TEXT  Genome name (e.g. hg38, mm10,  │
│                                               danio_rerio). Downloads GTF    │
│                                               and signal matrix, then        │
│                                               pre-compiles to .bin           │
│ --gtf                                   TEXT  Path to a local GTF/GTF.gz     │
│                                               file to pre-compile            │
│ --signal-matrix                         TEXT  Path to a local signal matrix  │
│                                               TSV/TSV.gz file to pre-compile │
│ --output          -o                    TEXT  Output path for                │
│                                               --gtf/--signal-matrix          │
│                                               (default: input path with .bin │
│                                               extension)                     │
│ --upload-s3           --no-upload-s3          Upload .bin files to S3 after  │
│                                               prepping (requires             │
│                                               --bedbase-config)              │
│                                               [default: no-upload-s3]        │
│ --bedbase-config                        TEXT  Path to bedbase config file    │
│                                               (required for --upload-s3)     │
│ --help                                        Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss verify-config --help`
```console
                                                                                
 Usage: bedboss verify-config [OPTIONS] CONFIG                                  
                                                                                
 Verify configuration file                                                      
                                                                                
╭─ Arguments ──────────────────────────────────────────────────────────────────╮
│ *    config      TEXT  Path to the bedbase config file [required]            │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss geo --help`
```console
                                                                                
 Usage: bedboss geo [OPTIONS] COMMAND [ARGS]...                                 
                                                                                
 Automatic BEDbase uploader for GEO data                                        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --version  -v        App version                                             │
│ --help               Show this message and exit.                             │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ upload-all  Run bedboss uploading pipeline for specified genome in specified │
│             period of time.                                                  │
│ upload-gse  Run bedboss uploading pipeline for GSE.                          │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss geo upload-all --help`
```console
                                                                                
 Usage: bedboss geo upload-all [OPTIONS]                                        
                                                                                
 Run bedboss uploading pipeline for specified genome in specified period of     
 time.                                                                          
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                             TEXT     Path to bedbase     │
│                                                          config file         │
│                                                          [required]          │
│ *  --outfolder                                  TEXT     Path to output      │
│                                                          folder              │
│                                                          [required]          │
│    --geo-tag                                    TEXT     GEO tag to use:     │
│                                                          'samples' or        │
│                                                          'series' [Default:  │
│                                                          samples]            │
│                                                          [default: samples]  │
│    --start-date                                 TEXT     The earliest date   │
│                                                          when opep was       │
│                                                          updated [Default:   │
│                                                          2000/01/01]         │
│    --end-date                                   TEXT     The latest date     │
│                                                          when opep was       │
│                                                          updated [Default:   │
│                                                          today's date]       │
│    --search-limit                               INTEGER  Limit of projects   │
│                                                          to be searched.     │
│                                                          [Default: 50]       │
│                                                          [default: 50]       │
│    --search-offset                              INTEGER  Limit of projects   │
│                                                          to be searched.     │
│                                                          [Default: 0]        │
│                                                          [default: 0]        │
│    --download-limit                             INTEGER  Limit of projects   │
│                                                          to be downloaded    │
│                                                          [Default: 100]      │
│                                                          [default: 100]      │
│    --genome                                     TEXT     Reference genome    │
│                                                          [Default: None]     │
│                                                          (e.g. hg38) - if    │
│                                                          None, all genomes   │
│                                                          will be processed   │
│    --preload             --no-preload                    Download bedfile    │
│                                                          before caching it.  │
│                                                          [Default: True]     │
│                                                          [default: preload]  │
│    --create-bedset       --no-create-bedset              Create bedset from  │
│                                                          bed files.          │
│                                                          [Default: True]     │
│                                                          [default:           │
│                                                          create-bedset]      │
│    --overwrite           --no-overwrite                  Overwrite existing  │
│                                                          bedfiles. [Default: │
│                                                          False]              │
│                                                          [default:           │
│                                                          no-overwrite]       │
│    --overwrite-bedset    --no-overwrite-bed…             Overwrite existing  │
│                                                          bedset. [Default:   │
│                                                          False]              │
│                                                          [default:           │
│                                                          overwrite-bedset]   │
│    --rerun               --no-rerun                      Re-run all the      │
│                                                          samples. [Default:  │
│                                                          False]              │
│                                                          [default: no-rerun] │
│    --run-skipped         --no-run-skipped                Run skipped         │
│                                                          projects. [Default: │
│                                                          False]              │
│                                                          [default:           │
│                                                          run-skipped]        │
│    --run-failed          --no-run-failed                 Run failed          │
│                                                          projects. [Default: │
│                                                          False]              │
│                                                          [default:           │
│                                                          run-failed]         │
│    --standardize-pep     --no-standardize-p…             Standardize pep     │
│                                                          with BEDMESS.       │
│                                                          [Default: False]    │
│                                                          [default:           │
│                                                          no-standardize-pep] │
│    --use-skipper         --no-use-skipper                Use skipper to skip │
│                                                          projects if they    │
│                                                          were processed      │
│                                                          locally [Default:   │
│                                                          False]              │
│                                                          [default:           │
│                                                          no-use-skipper]     │
│    --reinit-skipper      --no-reinit-skipper             Reinitialize        │
│                                                          skipper. [Default:  │
│                                                          False]              │
│                                                          [default:           │
│                                                          no-reinit-skipper]  │
│    --lite                --no-lite                       Run the pipeline in │
│                                                          lite mode.          │
│                                                          [Default: False]    │
│                                                          [default: no-lite]  │
│    --help                                                Show this message   │
│                                                          and exit.           │
╰──────────────────────────────────────────────────────────────────────────────╯


```

## `bedboss geo upload-gse --help`
```console
                                                                                
 Usage: bedboss geo upload-gse [OPTIONS]                                        
                                                                                
 Run bedboss uploading pipeline for GSE.                                        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                              TEXT  Path to bedbase       │
│                                                        config file           │
│                                                        [required]            │
│ *  --outfolder                                   TEXT  Path to output folder │
│                                                        [required]            │
│ *  --gse                                         TEXT  GSE number that can   │
│                                                        be found in pephub.   │
│                                                        eg. GSE123456         │
│                                                        [required]            │
│    --geo-tag                                     TEXT  GEO tag to use:       │
│                                                        'samples' or 'series' │
│                                                        [Default: samples]    │
│                                                        [default: samples]    │
│    --create-bedset       --no-create-bedset            Create bedset from    │
│                                                        bed files. [Default:  │
│                                                        True]                 │
│                                                        [default:             │
│                                                        create-bedset]        │
│    --genome                                      TEXT  reference genome to   │
│                                                        upload to database.   │
│                                                        If None, all genomes  │
│                                                        will be processed     │
│    --preload             --no-preload                  Download bedfile      │
│                                                        before caching it.    │
│                                                        [Default: True]       │
│                                                        [default: preload]    │
│    --rerun               --no-rerun                    Re-run all the        │
│                                                        samples. [Default:    │
│                                                        False]                │
│                                                        [default: rerun]      │
│    --run-skipped         --no-run-skipped              Run skipped projects. │
│                                                        [Default: False]      │
│                                                        [default:             │
│                                                        run-skipped]          │
│    --run-failed          --no-run-failed               Run failed projects.  │
│                                                        [Default: False]      │
│                                                        [default: run-failed] │
│    --overwrite           --no-overwrite                Overwrite existing    │
│                                                        bedfiles. [Default:   │
│                                                        False]                │
│                                                        [default:             │
│                                                        no-overwrite]         │
│    --overwrite-bedset    --no-overwrite-beds…          Overwrite existing    │
│                                                        bedset. [Default:     │
│                                                        False]                │
│                                                        [default:             │
│                                                        overwrite-bedset]     │
│    --standardize-pep     --no-standardize-pep          Standardize pep with  │
│                                                        BEDMESS. [Default:    │
│                                                        False]                │
│                                                        [default:             │
│                                                        no-standardize-pep]   │
│    --use-skipper         --no-use-skipper              Use local skipper to  │
│                                                        skip projects if they │
│                                                        were processed        │
│                                                        locally [Default:     │
│                                                        False]                │
│                                                        [default:             │
│                                                        no-use-skipper]       │
│    --reinit-skipper      --no-reinit-skipper           Reinitialize skipper. │
│                                                        [Default: False]      │
│                                                        [default:             │
│                                                        no-reinit-skipper]    │
│    --lite                --no-lite                     Run the pipeline in   │
│                                                        lite mode. [Default:  │
│                                                        False]                │
│                                                        [default: no-lite]    │
│    --help                                              Show this message and │
│                                                        exit.                 │
╰──────────────────────────────────────────────────────────────────────────────╯


```
