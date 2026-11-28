# Command line interface reference

BEDboss is command-line tool-manager and a set of tools for working with BED files and BEDbase. Main components of BEDboss are: </br>
1) Pipeline for processing BED files: bedmaker, bedqc, and bedstats.</br>
2) Indexing of the Bed files in bedbase</br>
3) Managing bed and bedsets in the database</br>

Here you can see the command-line usage instructions for the main bedboss command and for each subcommand:

## `bedboss --help`
```console
                                                                                
 Usage: bedboss [OPTIONS] COMMAND [ARGS]...                                     
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --version             -v             App version                             │
│ --verbosity                   <int>  Set logging level: 1 (CRITICAL) to 5    │
│                                      (DEBUG)                                 │
│ --logdev                             Use developer logging format            │
│ --silent                             Silence logging                         │
│ --install-completion                 Install completion for the current      │
│                                      shell.                                  │
│ --show-completion                    Show completion for the current shell,  │
│                                      to copy it or customize the             │
│                                      installation.                           │
│ --help                               Show this message and exit.             │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ run-all               Run all the bedboss pipeline for a single bed file     │
│ run-pep               Run the all bedboss pipeline for a bed files in a PEP  │
│ run-pep-hpc           Split a large PEP into N chunks and submit each as a   │
│                       SLURM job. Idempotent: re-run to resume failed/pending │
│                       chunks.                                                │
│ run-pep-hpc-status    Show status of a run-pep-hpc workdir.                  │
│ reprocess-all         Run unprocessed files or reprocess them. Currently,    │
│                       only hg38, hg19, and mm10 genomes are supported.       │
│ reprocess-one         Run unprocessed file, or reprocess it [Only 1 file]    │
│ reprocess-bedset      Reprocess a bedset                                     │
│ make-bed              Create a bed files from a  file                        │
│ make-bigbed           Create a bigbed files form a bed file                  │
│ run-stats             Create the statistics for a single bed file.           │
│ reindex               Reindex the bedbase database and insert all files to   │
│                       the qdrant database.                                   │
│ reindex-text          Reindex semantic (text) search.                        │
│ make-bedset           Create a bedset from a pep file, and insert it to the  │
│                       bedbase database.                                      │
│ init-config           Initialize the new, sample configuration file          │
│ delete-bed            Delete bed from the bedbase database                   │
│ delete-bedset         Delete BedSet from the bedbase database                │
│ tokenize-bed          Tokenize a bedfile                                     │
│ delete-tokenized      Delete tokenized bed file                              │
│ convert-universe      Convert bed file to universe                           │
│ update-genomes        Update reference genomes in the database               │
│ download-umap         Download UMAP                                          │
│ update-umap-metadata  Update UMAP parquet metadata without regenerating      │
│                       geometry                                               │
│ check-requirements    Check installed R packages                             │
│ install-requirements  Install R dependencies                                 │
│ verify-config         Verify configuration file                              │
│ geo                   Automatic BEDbase uploader for GEO data                │
│ qdrant                Qdrant reindexing commands for HPC.                    │
│ snapshot              Create and manage bulk metadata snapshots of the       │
│                       bedbase database.                                      │
│ files                 Upload and manage standalone analysis files            │
│                       (openSignalMatrix, models, other analysis inputs).     │
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
│ *  --bedbase-config                             <str>  Path to bedbase       │
│                                                        config file           │
│                                                        [required]            │
│ *  --outfolder                                  <str>  Path to output folder │
│                                                        [required]            │
│    --geo-tag                                    <str>  GEO tag to use:       │
│                                                        'samples' or 'series' │
│                                                        [Default: samples]    │
│                                                        [default: samples]    │
│    --start-date                                 <str>  The earliest date     │
│                                                        when opep was updated │
│                                                        [Default: 2000/01/01] │
│    --end-date                                   <str>  The latest date when  │
│                                                        opep was updated      │
│                                                        [Default: today's     │
│                                                        date]                 │
│    --search-limit                               <int>  Limit of projects to  │
│                                                        be searched.          │
│                                                        [Default: 50]         │
│                                                        [default: 50]         │
│    --search-offset                              <int>  Limit of projects to  │
│                                                        be searched.          │
│                                                        [Default: 0]          │
│                                                        [default: 0]          │
│    --download-limit                             <int>  Limit of projects to  │
│                                                        be downloaded         │
│                                                        [Default: 100]        │
│                                                        [default: 100]        │
│    --genome                                     <str>  Reference genome      │
│                                                        [Default: None] (e.g. │
│                                                        hg38) - if None, all  │
│                                                        genomes will be       │
│                                                        processed             │
│    --preload             --no-preload                  Download bedfile      │
│                                                        before caching it.    │
│                                                        [Default: True]       │
│                                                        [default: preload]    │
│    --create-bedset       --no-create-bedset            Create bedset from    │
│                                                        bed files. [Default:  │
│                                                        True]                 │
│                                                        [default:             │
│                                                        create-bedset]        │
│    --overwrite           --no-overwrite                Overwrite existing    │
│                                                        bedfiles. [Default:   │
│                                                        False]                │
│                                                        [default:             │
│                                                        no-overwrite]         │
│    --overwrite-bedset    --no-overwrite-bed…           Overwrite existing    │
│                                                        bedset. [Default:     │
│                                                        False]                │
│                                                        [default:             │
│                                                        overwrite-bedset]     │
│    --rerun               --no-rerun                    Re-run all the        │
│                                                        samples. [Default:    │
│                                                        False]                │
│                                                        [default: no-rerun]   │
│    --run-skipped         --no-run-skipped              Run skipped projects. │
│                                                        [Default: False]      │
│                                                        [default:             │
│                                                        run-skipped]          │
│    --run-failed          --no-run-failed               Run failed projects.  │
│                                                        [Default: False]      │
│                                                        [default: run-failed] │
│    --standardize-pep     --no-standardize-p…           Standardize pep with  │
│                                                        BEDMESS. [Default:    │
│                                                        False]                │
│                                                        [default:             │
│                                                        no-standardize-pep]   │
│    --use-skipper         --no-use-skipper              Use skipper to skip   │
│                                                        projects if they were │
│                                                        processed locally     │
│                                                        [Default: False]      │
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

## `bedboss geo upload-gse --help`
```console
                                                                                
 Usage: bedboss geo upload-gse [OPTIONS]                                        
                                                                                
 Run bedboss uploading pipeline for GSE.                                        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                             <str>  Path to bedbase       │
│                                                        config file           │
│                                                        [required]            │
│ *  --outfolder                                  <str>  Path to output folder │
│                                                        [required]            │
│ *  --gse                                        <str>  GSE number that can   │
│                                                        be found in pephub.   │
│                                                        eg. GSE123456         │
│                                                        [required]            │
│    --geo-tag                                    <str>  GEO tag to use:       │
│                                                        'samples' or 'series' │
│                                                        [Default: samples]    │
│                                                        [default: samples]    │
│    --create-bedset       --no-create-bedset            Create bedset from    │
│                                                        bed files. [Default:  │
│                                                        True]                 │
│                                                        [default:             │
│                                                        create-bedset]        │
│    --genome                                     <str>  reference genome to   │
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
│    --overwrite-bedset    --no-overwrite-bed…           Overwrite existing    │
│                                                        bedset. [Default:     │
│                                                        False]                │
│                                                        [default:             │
│                                                        overwrite-bedset]     │
│    --standardize-pep     --no-standardize-p…           Standardize pep with  │
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

## `bedboss run-all --help`
```console
                                                                                
 Usage: bedboss run-all [OPTIONS]                                               
                                                                                
 Run all the bedboss pipeline for a single bed file                             
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --input-file                                  <str>  Path to the input    │
│                                                         file                 │
│                                                         [required]           │
│ *  --input-type                                  <str>  Type of the input    │
│                                                         file. Options are:   │
│                                                         bigwig, bedgraph,    │
│                                                         bed, bigbed, wig     │
│                                                         [required]           │
│ *  --outfolder                                   <str>  Path to the output   │
│                                                         folder               │
│                                                         [required]           │
│ *  --genome                                      <str>  Genome name.         │
│                                                         Example: 'hg38'      │
│                                                         [required]           │
│ *  --bedbase-config                              <str>  Path to the bedbase  │
│                                                         config file          │
│                                                         [required]           │
│    --license-id                                  <str>  License ID. If not   │
│                                                         provided for in      │
│                                                         PEPfor each bed      │
│                                                         file, this license   │
│                                                         will be used         │
│                                                         [default:            │
│                                                         DUO:0000042]         │
│    --narrowpeak           --no-narrowpeak               Is the input file a  │
│                                                         narrowpeak file?     │
│                                                         [default:            │
│                                                         no-narrowpeak]       │
│    --check-qc             --no-check-qc                 Check the quality of │
│                                                         the input file?      │
│                                                         [default: check-qc]  │
│    --chrom-sizes                                 <str>  Path to the chrom    │
│                                                         sizes file           │
│    --open-signal-matr…                           <str>  Path to the open     │
│                                                         signal matrix file   │
│    --ensdb                                       <str>  Path to the EnsDb    │
│                                                         database file        │
│    --just-db-commit       --no-just-db-commit           Just commit to the   │
│                                                         database?            │
│                                                         [default:            │
│                                                         no-just-db-commit]   │
│    --force-overwrite      --no-force-overwri…           Force overwrite the  │
│                                                         output files         │
│                                                         [default:            │
│                                                         no-force-overwrite]  │
│    --update               --no-update                   Update the bedbase   │
│                                                         database with the    │
│                                                         new record if it     │
│                                                         exists. This         │
│                                                         overwrites           │
│                                                         'force_overwrite'    │
│                                                         option               │
│                                                         [default: no-update] │
│    --lite                 --no-lite                     Run the pipeline in  │
│                                                         lite mode. [Default: │
│                                                         False]               │
│                                                         [default: no-lite]   │
│    --upload-qdrant        --no-upload-qdrant            Upload to Qdrant     │
│                                                         [default:            │
│                                                         no-upload-qdrant]    │
│    --upload-s3            --no-upload-s3                Upload to S3         │
│                                                         [default:            │
│                                                         no-upload-s3]        │
│    --universe             --no-universe                 Create a universe    │
│                                                         [default:            │
│                                                         no-universe]         │
│    --universe-method                             <str>  Method used to       │
│                                                         create the universe  │
│    --universe-bedset                             <str>  Bedset used used to  │
│                                                         create the universe  │
│    --multi                --no-multi                    Run multiple samples │
│                                                         [default: no-multi]  │
│    --recover              --no-recover                  Recover from         │
│                                                         previous run         │
│                                                         [default: recover]   │
│    --dirty                --no-dirty                    Run without removing │
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
│ *  --pep                                        <str>  PEP file. Local or    │
│                                                        remote path           │
│                                                        [required]            │
│ *  --outfolder                                  <str>  Path to the output    │
│                                                        folder                │
│                                                        [required]            │
│ *  --bedbase-config                             <str>  Path to the bedbase   │
│                                                        config file           │
│                                                        [required]            │
│    --create-bedset      --no-create-bedset             Create a new bedset   │
│                                                        [default:             │
│                                                        create-bedset]        │
│    --bedset-heavy       --no-bedset-heavy              Run the heavy version │
│                                                        of the bedbuncher     │
│                                                        pipeline              │
│                                                        [default:             │
│                                                        no-bedset-heavy]      │
│    --bedset-id                                  <str>  Bedset ID             │
│    --check-qc           --no-check-qc                  Check the quality of  │
│                                                        the input file?       │
│                                                        [default: check-qc]   │
│    --ensdb                                      <str>  Path to the EnsDb     │
│                                                        database file         │
│    --just-db-commit     --no-just-db-commit            Just commit to the    │
│                                                        database?             │
│                                                        [default:             │
│                                                        no-just-db-commit]    │
│    --force-overwrite    --no-force-overwrite           Force overwrite the   │
│                                                        output files          │
│                                                        [default:             │
│                                                        no-force-overwrite]   │
│    --update             --no-update                    Update the bedbase    │
│                                                        database with the new │
│                                                        record if it exists.  │
│                                                        This overwrites       │
│                                                        'force_overwrite'     │
│                                                        option                │
│                                                        [default: no-update]  │
│    --upload-qdrant      --no-upload-qdrant             Upload to Qdrant      │
│                                                        [default:             │
│                                                        upload-qdrant]        │
│    --upload-s3          --no-upload-s3                 Upload to S3          │
│                                                        [default: upload-s3]  │
│    --no-fail            --no-no-fail                   Do not fail on error  │
│                                                        [default: no-no-fail] │
│    --license-id                                 <str>  License ID            │
│                                                        [default:             │
│                                                        DUO:0000042]          │
│    --standardize-pep    --no-standardize-pep           Standardize the PEP   │
│                                                        using bedMS           │
│                                                        [default:             │
│                                                        no-standardize-pep]   │
│    --lite               --no-lite                      Run the pipeline in   │
│                                                        lite mode. [Default:  │
│                                                        False]                │
│                                                        [default: no-lite]    │
│    --rerun              --no-rerun                     Rerun already         │
│                                                        processed samples     │
│                                                        [default: no-rerun]   │
│    --multi              --no-multi                     Run multiple samples  │
│                                                        [default: no-multi]   │
│    --recover            --no-recover                   Recover from previous │
│                                                        run                   │
│                                                        [default: recover]    │
│    --dirty              --no-dirty                     Run without removing  │
│                                                        existing files        │
│                                                        [default: no-dirty]   │
│    --help                                              Show this message and │
│                                                        exit.                 │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss run-pep-hpc --help`
```console
                                                                                
 Usage: bedboss run-pep-hpc [OPTIONS]                                           
                                                                                
 Split a large PEP into N chunks and submit each as a SLURM job. Idempotent:    
 re-run to resume failed/pending chunks.                                        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --pep                                        <str>  PEP file. Local path  │
│                                                        or PEPhub registry    │
│                                                        path.                 │
│                                                        [required]            │
│ *  --workdir                                    <str>  Working directory for │
│                                                        chunks, sbatch files, │
│                                                        manifest, and state.  │
│                                                        [required]            │
│ *  --n-chunks                                   <int>  Number of chunks to   │
│                                                        split the PEP into.   │
│                                                        [required]            │
│ *  --outfolder                                  <str>  Path to the output    │
│                                                        folder (shared across │
│                                                        chunks).              │
│                                                        [required]            │
│ *  --bedbase-config                             <str>  Path to the bedbase   │
│                                                        config file           │
│                                                        [required]            │
│    --create-bedset      --no-create-bedset             Create a new bedset   │
│                                                        [default:             │
│                                                        create-bedset]        │
│    --bedset-heavy       --no-bedset-heavy              Run heavy bedbuncher  │
│                                                        [default:             │
│                                                        no-bedset-heavy]      │
│    --check-qc           --no-check-qc                  Check the quality of  │
│                                                        the input file?       │
│                                                        [default: check-qc]   │
│    --ensdb                                      <str>  Path to the EnsDb     │
│                                                        database file         │
│    --just-db-commit     --no-just-db-commit            Just commit to the    │
│                                                        database?             │
│                                                        [default:             │
│                                                        no-just-db-commit]    │
│    --force-overwrite    --no-force-overwrite           Force overwrite the   │
│                                                        output files          │
│                                                        [default:             │
│                                                        no-force-overwrite]   │
│    --update             --no-update                    Update existing       │
│                                                        records               │
│                                                        [default: no-update]  │
│    --upload-qdrant      --no-upload-qdrant             Upload to Qdrant      │
│                                                        [default:             │
│                                                        upload-qdrant]        │
│    --upload-s3          --no-upload-s3                 Upload to S3          │
│                                                        [default: upload-s3]  │
│    --no-fail            --no-no-fail                   Do not fail on error  │
│                                                        [default: no-no-fail] │
│    --license-id                                 <str>  License ID            │
│                                                        [default:             │
│                                                        DUO:0000042]          │
│    --standardize-pep    --no-standardize-pep           Standardize the PEP   │
│                                                        using bedMS           │
│                                                        [default:             │
│                                                        no-standardize-pep]   │
│    --lite               --no-lite                      Run the pipeline in   │
│                                                        lite mode.            │
│                                                        [default: no-lite]    │
│    --rerun              --no-rerun                     Rerun already         │
│                                                        processed samples     │
│                                                        [default: no-rerun]   │
│    --multi              --no-multi                     Run multiple samples  │
│                                                        [default: no-multi]   │
│    --recover            --no-recover                   Recover from previous │
│                                                        run                   │
│                                                        [default: recover]    │
│    --dirty              --no-dirty                     Run without removing  │
│                                                        existing files        │
│                                                        [default: no-dirty]   │
│    --slurm-template                             <str>  Path to a custom      │
│                                                        sbatch template. See  │
│                                                        bedboss/bedboss_hpc.… │
│                                                        for placeholders.     │
│    --slurm-account                              <str>  SLURM --account       │
│                                                        [default: shefflab]   │
│    --slurm-partition                            <str>  SLURM --partition     │
│                                                        [default: standard]   │
│    --slurm-time                                 <str>  SLURM --time          │
│                                                        [default: 72:00:00]   │
│    --slurm-mem                                  <str>  SLURM --mem (MB)      │
│                                                        [default: 60000]      │
│    --slurm-cpus                                 <int>  SLURM --cpus-per-task │
│                                                        [default: 4]          │
│    --slurm-ntasks                               <int>  SLURM --ntasks        │
│                                                        [default: 2]          │
│    --dry-run            --no-dry-run                   Split and write       │
│                                                        sbatch files but do   │
│                                                        not submit.           │
│                                                        [default: no-dry-run] │
│    --help                                              Show this message and │
│                                                        exit.                 │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss run-pep-hpc-status --help`
```console
                                                                                
 Usage: bedboss run-pep-hpc-status [OPTIONS]                                    
                                                                                
 Show status of a run-pep-hpc workdir.                                          
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --workdir        <str>  Working directory created by run-pep-hpc.         │
│                            [required]                                        │
│    --help                  Show this message and exit.                       │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss reprocess-all --help`
```console
                                                                                
 Usage: bedboss reprocess-all [OPTIONS]                                         
                                                                                
 Run unprocessed files or reprocess them. Currently, only hg38, hg19, and mm10  
 genomes are supported.                                                         
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                    <str>  Path to the bedbase config     │
│                                               file                           │
│                                               [required]                     │
│ *  --outfolder                         <str>  Path to the output folder      │
│                                               [required]                     │
│    --limit                             <int>  Limit the number of files to   │
│                                               reprocess                      │
│                                               [default: 100]                 │
│    --no-fail           --no-no-fail           Do not fail on error           │
│                                               [default: no-fail]             │
│    --help                                     Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss reprocess-one --help`
```console
                                                                                
 Usage: bedboss reprocess-one [OPTIONS]                                         
                                                                                
 Run unprocessed file, or reprocess it [Only 1 file]                            
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                    <str>  Path to the bedbase config     │
│                                               file                           │
│                                               [required]                     │
│ *  --outfolder                         <str>  Path to the output folder      │
│                                               [required]                     │
│ *  --identifier                        <str>  Identifier of the bed file     │
│                                               [required]                     │
│    --multi             --no-multi             Run multiple samples           │
│                                               [default: multi]               │
│    --recover           --no-recover           Recover from previous run      │
│                                               [default: recover]             │
│    --dirty             --no-dirty             Run without removing existing  │
│                                               files                          │
│                                               [default: no-dirty]            │
│    --help                                     Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss reprocess-bedset --help`
```console
                                                                                
 Usage: bedboss reprocess-bedset [OPTIONS]                                      
                                                                                
 Reprocess a bedset                                                             
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                    <str>  Path to the bedbase config     │
│                                               file                           │
│                                               [required]                     │
│ *  --outfolder                         <str>  Path to the output folder      │
│                                               [required]                     │
│ *  --identifier                        <str>  Bedset ID [required]           │
│    --no-fail           --no-no-fail           Do not fail on error           │
│                                               [default: no-fail]             │
│    --heavy             --no-heavy             Run the heavy version of the   │
│                                               pipeline                       │
│                                               [default: no-heavy]            │
│    --help                                     Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss make-bed --help`
```console
                                                                                
 Usage: bedboss make-bed [OPTIONS]                                              
                                                                                
 Create a bed files from a  file                                                
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --input-file                        <str>  Path to the input file         │
│                                               [required]                     │
│ *  --input-type                        <str>  Type of the input file.        │
│                                               Options are: bigwig, bedgraph, │
│                                               bed, bigbed, wig               │
│                                               [required]                     │
│ *  --outfolder                         <str>  Path to the output folder      │
│                                               [required]                     │
│ *  --genome                            <str>  Genome name. Example: 'hg38'   │
│                                               [required]                     │
│    --narrowpeak     --no-narrowpeak           Is the input file a narrowpeak │
│                                               file?                          │
│                                               [default: no-narrowpeak]       │
│    --chrom-sizes                       <str>  Path to the chrom sizes file   │
│    --multi          --no-multi                Run multiple samples           │
│                                               [default: no-multi]            │
│    --recover        --no-recover              Recover from previous run      │
│                                               [default: recover]             │
│    --dirty          --no-dirty                Run without removing existing  │
│                                               files                          │
│                                               [default: no-dirty]            │
│    --help                                     Show this message and exit.    │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss make-bigbed --help`
```console
                                                                                
 Usage: bedboss make-bigbed [OPTIONS]                                           
                                                                                
 Create a bigbed files form a bed file                                          
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-file         <str>  Path to the input file [required]               │
│ *  --outfolder        <str>  Path to the output folder [required]            │
│ *  --genome           <str>  Genome name. Example: 'hg38' [required]         │
│    --help                    Show this message and exit.                     │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss run-stats --help`
```console
                                                                                
 Usage: bedboss run-stats [OPTIONS]                                             
                                                                                
 Create the statistics for a single bed file.                                   
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-file                                    <str>  Path to the bed file │
│                                                         [required]           │
│ *  --genome                                      <str>  Genome name.         │
│                                                         Example: 'hg38'      │
│                                                         [required]           │
│ *  --outfolder                                   <str>  Path to the output   │
│                                                         folder               │
│                                                         [required]           │
│    --ensdb                                       <str>  Path to the EnsDb    │
│                                                         database file        │
│    --open-signal-matr…                           <str>  Path to the open     │
│                                                         signal matrix file   │
│    --just-db-commit       --no-just-db-commit           Just commit to the   │
│                                                         database?            │
│                                                         [default:            │
│                                                         no-just-db-commit]   │
│    --multi                --no-multi                    Run multiple samples │
│                                                         [default: no-multi]  │
│    --recover              --no-recover                  Recover from         │
│                                                         previous run         │
│                                                         [default: recover]   │
│    --dirty                --no-dirty                    Run without removing │
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
│ *  --bedbase-config                  <str>  Path to the bedbase config file  │
│                                             [required]                       │
│    --purge             --no-purge           Purge existing index before      │
│                                             reindexing                       │
│                                             [default: no-purge]              │
│    --batch                           <int>  Number of items to upload in one │
│                                             batch                            │
│                                             [default: 1000]                  │
│    --help                                   Show this message and exit.      │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss reindex-text --help`
```console
                                                                                
 Usage: bedboss reindex-text [OPTIONS]                                          
                                                                                
 Reindex semantic (text) search.                                                
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bedbase-config                  <str>  Path to the bedbase config file  │
│                                             [required]                       │
│    --purge             --no-purge           Purge existing index before      │
│                                             reindexing                       │
│                                             [default: no-purge]              │
│    --batch                           <int>  Number of items to upload in one │
│                                             batch                            │
│                                             [default: 1000]                  │
│    --help                                   Show this message and exit.      │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss make-bedset --help`
```console
                                                                                
 Usage: bedboss make-bedset [OPTIONS]                                           
                                                                                
 Create a bedset from a pep file, and insert it to the bedbase database.        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --pep                                        <str>  PEP file. Local or    │
│                                                        remote path           │
│                                                        [required]            │
│ *  --outfolder                                  <str>  Path to the output    │
│                                                        folder                │
│                                                        [required]            │
│ *  --bedbase-config                             <str>  Path to the bedbase   │
│                                                        config file           │
│                                                        [required]            │
│ *  --bedset-name                                <str>  Name of the bedset    │
│                                                        [required]            │
│    --heavy              --no-heavy                     Run the heavy version │
│                                                        of the pipeline       │
│                                                        [default: no-heavy]   │
│    --force-overwrite    --no-force-overwrite           Force overwrite the   │
│                                                        output files          │
│                                                        [default:             │
│                                                        no-force-overwrite]   │
│    --upload-s3          --no-upload-s3                 Upload to S3          │
│                                                        [default:             │
│                                                        no-upload-s3]         │
│    --no-fail            --no-no-fail                   Do not fail on error  │
│                                                        [default: no-no-fail] │
│    --help                                              Show this message and │
│                                                        exit.                 │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss init-config --help`
```console
                                                                                
 Usage: bedboss init-config [OPTIONS]                                           
                                                                                
 Initialize the new, sample configuration file                                  
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --outfolder        <str>  Path to the output folder [required]            │
│    --help                    Show this message and exit.                     │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss delete-bed --help`
```console
                                                                                
 Usage: bedboss delete-bed [OPTIONS]                                            
                                                                                
 Delete bed from the bedbase database                                           
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --sample-id        <str>  Sample ID [required]                            │
│ *  --config           <str>  Path to the bedbase config file [required]      │
│    --help                    Show this message and exit.                     │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss delete-bedset --help`
```console
                                                                                
 Usage: bedboss delete-bedset [OPTIONS]                                         
                                                                                
 Delete BedSet from the bedbase database                                        
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --identifier        <str>  BedSet ID [required]                           │
│ *  --config            <str>  Path to the bedbase config file [required]     │
│    --help                     Show this message and exit.                    │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss tokenize-bed --help`
```console
                                                                                
 Usage: bedboss tokenize-bed [OPTIONS]                                          
                                                                                
 Tokenize a bedfile                                                             
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-id                              <str>  Path to the bed file         │
│                                                 [required]                   │
│ *  --universe-id                         <str>  Universe ID [required]       │
│    --cache-folder                        <str>  Path to the cache folder     │
│    --add-to-db         --no-add-to-db           Add the tokenized bed file   │
│                                                 to the bedbase database      │
│                                                 [default: no-add-to-db]      │
│    --bedbase-config                      <str>  Path to the bedbase config   │
│                                                 file                         │
│    --overwrite         --no-overwrite           Overwrite the existing       │
│                                                 tokenized bed file           │
│                                                 [default: no-overwrite]      │
│    --help                                       Show this message and exit.  │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss delete-tokenized --help`
```console
                                                                                
 Usage: bedboss delete-tokenized [OPTIONS]                                      
                                                                                
 Delete tokenized bed file                                                      
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --universe-id        <str>  Universe ID [required]                        │
│ *  --bed-id             <str>  Bed ID [required]                             │
│    --config             <str>  Path to the bedbase config file               │
│    --help                      Show this message and exit.                   │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss convert-universe --help`
```console
                                                                                
 Usage: bedboss convert-universe [OPTIONS]                                      
                                                                                
 Convert bed file to universe                                                   
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --bed-id        <str>  Path to the bed file [required]                    │
│ *  --config        <str>  Path to the bedbase config file [required]         │
│    --method        <str>  Method used to create the universe                 │
│    --bedset        <str>  Bedset used to create the universe                 │
│    --help                 Show this message and exit.                        │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss update-genomes --help`
```console
                                                                                
 Usage: bedboss update-genomes [OPTIONS]                                        
                                                                                
 Update reference genomes in the database                                       
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --config        <str>  Path to the bedbase config file [required]         │
│    --help                 Show this message and exit.                        │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss download-umap --help`
```console
                                                                                
 Usage: bedboss download-umap [OPTIONS]                                         
                                                                                
 Download UMAP                                                                  
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --config                <str>  Path to the bedbase config file [required] │
│ *  --output-file           <str>  Path to the output json file where UMAP    │
│                                   embeddings will be saved. (*Python version │
│                                   will be added to the filename)             │
│                                   [required]                                 │
│    --n-components          <int>  Number of UMAP components [default: 2]     │
│    --plot-name             <str>  Name of the plot file                      │
│    --plot-label            <str>  Label for the plot                         │
│    --top-assays            <int>  Number of top assays to include            │
│                                   [default: 15]                              │
│    --top-cell-lines        <int>  Number of top cell lines to include        │
│                                   [default: 15]                              │
│    --method                <str>  Dimensionality reduction method to use.    │
│                                   Options: 'umap', 'pca', or 'tsne'. To use  │
│                                   UMAP, 'umap-learn' package must be         │
│                                   installed.                                 │
│                                   [default: umap]                            │
│    --output-format         <str>  Output format: 'json', 'parquet', or       │
│                                   'both'                                     │
│                                   [default: parquet]                         │
│    --help                         Show this message and exit.                │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss update-umap-metadata --help`
```console
                                                                                
 Usage: bedboss update-umap-metadata [OPTIONS]                                  
                                                                                
 Update UMAP parquet metadata without regenerating geometry                     
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ *  --config             <str>  Path to the bedbase config file [required]    │
│ *  --output-path        <str>  Path to write parquet file (without           │
│                                extension)                                    │
│                                [required]                                    │
│    --geometry           <str>  Path to existing geometry Parquet (to read    │
│                                bed IDs). If not provided, fetches IDs from   │
│                                Qdrant.                                       │
│    --help                      Show this message and exit.                   │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss check-requirements --help`
```console
                                                                                
 Usage: bedboss check-requirements [OPTIONS]                                    
                                                                                
 Check installed R packages                                                     
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss install-requirements --help`
```console
                                                                                
 Usage: bedboss install-requirements [OPTIONS]                                  
                                                                                
 Install R dependencies                                                         
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss verify-config --help`
```console
                                                                                
 Usage: bedboss verify-config [OPTIONS] {config}                                
                                                                                
 Verify configuration file                                                      
                                                                                
╭─ Arguments ──────────────────────────────────────────────────────────────────╮
│ *    config      <str>  Path to the bedbase config file [required]           │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯

```

## `bedboss get-commands --help`
```console
                                                                                
 Usage: bedboss get-commands [OPTIONS]                                          
                                                                                
 Get available commands                                                         
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯

```

