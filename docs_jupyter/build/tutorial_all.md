jupyter:True
# Bedboss-all tutorial

This tutorial is attended to show base exaple of using bedboss all function that inclueds all 3 pipelines: bedmake, bedqc and bedstat

### 1. First let's create new working repository


```bash
mkdir all_tutorial ; cd all_tutorial 
```

### 2. To run our pipelines we need to check if we have installed all dependencies. To do so we can run dependencies check script that can be found in docs.


```bash
wget -O req_test.sh https://raw.githubusercontent.com/bedbase/bedboss/68910f5142a95d92c27ef53eafb9c35599af2fbd/test/bash_requirements_test.sh
```

```.output
--2023-02-27 11:23:14--  https://raw.githubusercontent.com/bedbase/bedboss/68910f5142a95d92c27ef53eafb9c35599af2fbd/test/bash_requirements_test.sh
Resolving raw.githubusercontent.com (raw.githubusercontent.com)... 185.199.110.133, 185.199.108.133, 185.199.109.133, ...
Connecting to raw.githubusercontent.com (raw.githubusercontent.com)|185.199.110.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 3927 (3.8K) [text/plain]
Saving to: ‘req_test.sh’

req_test.sh         100%[===================>]   3.83K  --.-KB/s    in 0s      

2023-02-27 11:23:14 (33.2 MB/s) - ‘req_test.sh’ saved [3927/3927]


```


```bash
chmod u+x ./req_test.sh
```


```bash
./req_test.sh
```

```.output
-----------------------------------------------------------
                                                           
             bedboss installation check                    
                                                           
-----------------------------------------------------------
Checking native installation...                            
Language compilers...                            
-----------------------------------------------------------
✔ python is installed correctly
✔ R is installed correctly
-----------------------------------------------------------
Checking bedmaker dependencies...                            
-----------------------------------------------------------
✔ package bedboss==0.1.0.dev1
✔ package refgenconf==0.12.2
✔ bedToBigBed is installed correctly
⚠ WARNING: 'bigBedToBed' is not installed. To install 'bigBedToBed' check bedboss documentation: https://bedboss.databio.org/
⚠ WARNING: 'bigWigToBedGraph' is not installed. To install 'bigWigToBedGraph' check bedboss documentation: https://bedboss.databio.org/
⚠ WARNING: 'wigToBigWig' is not installed. To install 'wigToBigWig' check bedboss documentation: https://bedboss.databio.org/
-----------------------------------------------------------
Checking required R packages for bedstat...                            
-----------------------------------------------------------
✔ SUCCESS: R package: optparse
✔ SUCCESS: R package: ensembldb
✔ SUCCESS: R package: ExperimentHub
✔ SUCCESS: R package: AnnotationHub
✔ SUCCESS: R package: AnnotationFilter
✔ SUCCESS: R package: BSgenome
✔ SUCCESS: R package: GenomicFeatures
✔ SUCCESS: R package: GenomicDistributions
✔ SUCCESS: R package: GenomicDistributionsData
✔ SUCCESS: R package: GenomeInfoDb
✔ SUCCESS: R package: ensembldb
✔ SUCCESS: R package: tools
✔ SUCCESS: R package: R.utils
✔ SUCCESS: R package: LOLA
Number of WARNINGS: 3

```

### 3. All requirements are installed, now lets run our pipeline

To run pipeline, we need to provide few required arguments:
1. sample_name
2. input_file
3. input_type
4. outfolder
5. genome
6. bedbase_config

If you don't have bedbase config file, or initialized bedbase db you can check documnetation how to do it: https://bedboss.databio.org/


```bash
bedboss all
```

```.output
usage: bedboss all [-h] --outfolder OUTFOLDER -s SAMPLE_NAME -f INPUT_FILE -t
                   INPUT_TYPE -g GENOME [-r RFG_CONFIG]
                   [--chrom-sizes CHROM_SIZES] [-n] [--standard-chrom]
                   [--check-qc] [--open-signal-matrix OPEN_SIGNAL_MATRIX]
                   [--ensdb ENSDB] --bedbase-config BEDBASE_CONFIG
                   [-y SAMPLE_YAML] [--no-db-commit] [--just-db-commit]
bedboss all: error: the following arguments are required: --outfolder, -s/--sample-name, -f/--input-file, -t/--input-type, -g/--genome, --bedbase-config

```



Let's download sample file. Information about this file you can find here: https://pephub.databio.org/bedbase/GSE177859?tag=default


```bash
wget -O sample1.bed.gz ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM5379nnn/GSM5379062/suppl/GSM5379062_ENCFF834LRN_peaks_GRCh38.bed.gz
```

```.output
--2023-02-27 11:55:49--  ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM5379nnn/GSM5379062/suppl/GSM5379062_ENCFF834LRN_peaks_GRCh38.bed.gz
           => ‘sample1.bed.gz’
Resolving ftp.ncbi.nlm.nih.gov (ftp.ncbi.nlm.nih.gov)... 165.112.9.229, 165.112.9.230, 2607:f220:41f:250::228, ...
Connecting to ftp.ncbi.nlm.nih.gov (ftp.ncbi.nlm.nih.gov)|165.112.9.229|:21... connected.
Logging in as anonymous ... Logged in!
==> SYST ... done.    ==> PWD ... done.
==> TYPE I ... done.  ==> CWD (1) /geo/samples/GSM5379nnn/GSM5379062/suppl ... done.
==> SIZE GSM5379062_ENCFF834LRN_peaks_GRCh38.bed.gz ... 5470278
==> PASV ... done.    ==> RETR GSM5379062_ENCFF834LRN_peaks_GRCh38.bed.gz ... done.
Length: 5470278 (5.2M) (unauthoritative)

GSM5379062_ENCFF834 100%[===================>]   5.22M  18.4MB/s    in 0.3s    

2023-02-27 11:55:50 (18.4 MB/s) - ‘sample1.bed.gz’ saved [5470278]


```

let's create bedbase config file:


```bash
cat bedbase_config_test.yaml
```

```.output
path:
  pipeline_output_path: $BEDBOSS_OUTPUT_PATH  # do not change it
  bedstat_dir: bedstat_output
  remote_url_base: null
  bedbuncher_dir: bedbucher_output
database:
  host: localhost
  port: 5432
  password: docker
  user: postgres
  name: pep-db
  dialect: postgresql
  driver: psycopg2
server:
  host: 0.0.0.0
  port: 8000
remotes:
  http:
    prefix: https://data.bedbase.org/
    description: HTTP compatible path
  s3:
    prefix: s3://data.bedbase.org/
    description: S3 compatible path

```

Now let's run bedboss:


```bash
bedboss all --sample-name tutorial_f1 \
--input-file sample1.bed.gz \
--input-type bed \
--outfolder ./tutorial \
--genome GRCh38 \
--bedbase-config bedbase_config_test.yaml
```

```.output
Warning: You're running an interactive python session. This works, but pypiper cannot tee the output, so results are only logged to screen.
### Pipeline run code and environment:

*              Command:  `/home/bnt4me/virginia/venv/jupyter/bin/bedboss all --sample-name tutorial_f1 --input-file sample1.bed.gz --input-type bed --outfolder ./tutorial --genome GRCh38 --bedbase-config bedbase_config_test.yaml`
*         Compute host:  bnt4me-Precision-5560
*          Working dir:  /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial
*            Outfolder:  ./tutorial/
*  Pipeline started at:   (02-27 12:47:26) elapsed: 0.0 _TIME_

### Version log:

*       Python version:  3.10.6
*          Pypiper dir:  `/home/bnt4me/virginia/venv/jupyter/lib/python3.10/site-packages/pypiper`
*      Pypiper version:  0.12.3
*         Pipeline dir:  `/home/bnt4me/virginia/venv/jupyter/bin`
*     Pipeline version:  None

### Arguments passed to pipeline:


----------------------------------------

Unused arguments: {'command': 'all'}
Getting Open Signal Matrix file path...
output_bed = ./tutorial/bed_files/sample1.bed.gz
output_bigbed = ./tutorial/bigbed_files
Output directory does not exist. Creating: ./tutorial/bed_files
BigBed directory does not exist. Creating: ./tutorial/bigbed_files
bedmaker logs directory doesn't exist. Creating one...
Got input type: bed
Converting sample1.bed.gz to BED format.
Target to produce: `./tutorial/bed_files/sample1.bed.gz`  

> `cp sample1.bed.gz ./tutorial/bed_files/sample1.bed.gz` (434320)
<pre>
</pre>
Command completed. Elapsed time: 0:00:00. Running peak memory: 0GB.  
  PID: 434320;	Command: cp;	Return code: 0;	Memory used: 0.0GB

Running bedqc...
Unused arguments: {}
Target to produce: `./tutorial/bed_files/bedmaker_logs/tutorial_f1/rigumni8`  

> `zcat ./tutorial/bed_files/sample1.bed.gz > ./tutorial/bed_files/bedmaker_logs/tutorial_f1/rigumni8` (434322)
<pre>
</pre>
Command completed. Elapsed time: 0:00:00. Running peak memory: 0.003GB.  
  PID: 434322;	Command: zcat;	Return code: 0;	Memory used: 0.003GB

Targetless command, running...  

> `bash /home/bnt4me/virginia/venv/jupyter/lib/python3.10/site-packages/bedboss/bedqc/est_line.sh ./tutorial/bed_files/bedmaker_logs/tutorial_f1/rigumni8 ` (434324)
<pre>
236000</pre>
Command completed. Elapsed time: 0:00:00. Running peak memory: 0.003GB.  
  PID: 434324;	Command: bash;	Return code: 0;	Memory used: 0.0GB

File (./tutorial/bed_files/bedmaker_logs/tutorial_f1/rigumni8) has passed Quality Control!
Generating bigBed files for: sample1.bed.gz
Determining path to chrom.sizes asset via Refgenie.
Creating refgenie genome config file...
Reading refgenie genome configuration file from file: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/genome_config.yaml
/home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/alias/hg38/fasta/default/hg38.chrom.sizes
Determined path to chrom.sizes asset: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/alias/hg38/fasta/default/hg38.chrom.sizes
Target to produce: `./tutorial/bigbed_files/vzxyqexz`  

> `zcat ./tutorial/bed_files/sample1.bed.gz  | sort -k1,1 -k2,2n > ./tutorial/bigbed_files/vzxyqexz` (434335,434336)
<pre>
</pre>
Command completed. Elapsed time: 0:00:00. Running peak memory: 0.007GB.  
  PID: 434335;	Command: zcat;	Return code: 0;	Memory used: 0.002GB  
  PID: 434336;	Command: sort;	Return code: 0;	Memory used: 0.007GB

Running: /home/bnt4me/virginia/repos/bedbase_all/bedboss/bedToBigBed -type=bed6+4 ./tutorial/bigbed_files/vzxyqexz /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/alias/hg38/fasta/default/hg38.chrom.sizes ./tutorial/bigbed_files/sample1.bigBed
Target to produce: `./tutorial/bigbed_files/sample1.bigBed`  

> `/home/bnt4me/virginia/repos/bedbase_all/bedboss/bedToBigBed -type=bed6+4 ./tutorial/bigbed_files/vzxyqexz /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/alias/hg38/fasta/default/hg38.chrom.sizes ./tutorial/bigbed_files/sample1.bigBed` (434338)
<pre>
pass1 - making usageList (25 chroms): 27 millis
pass2 - checking and writing primary data (222016 records, 10 fields): 413 millis
</pre>
Command completed. Elapsed time: 0:00:01. Running peak memory: 0.007GB.  
  PID: 434338;	Command: /home/bnt4me/virginia/repos/bedbase_all/bedboss/bedToBigBed;	Return code: 0;	Memory used: 0.004GB

Target to produce: `/home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1.json`  

> `Rscript /home/bnt4me/virginia/venv/jupyter/lib/python3.10/site-packages/bedboss/bedstat/tools/regionstat.R --bedfilePath=./tutorial/bed_files/sample1.bed.gz --fileId=sample1 --openSignalMatrix=./openSignalMatrix/openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz --outputFolder=/home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5 --genome=hg38 --ensdb=None --digest=eb617f28e129c401be94069e0fdedbb5` (434343)
<pre>
Loading required package: IRanges
Loading required package: BiocGenerics

Attaching package: ‘BiocGenerics’

The following objects are masked from ‘package:stats’:

    IQR, mad, sd, var, xtabs

The following objects are masked from ‘package:base’:

    anyDuplicated, append, as.data.frame, basename, cbind, colnames,
    dirname, do.call, duplicated, eval, evalq, Filter, Find, get, grep,
    grepl, intersect, is.unsorted, lapply, Map, mapply, match, mget,
    order, paste, pmax, pmax.int, pmin, pmin.int, Position, rank,
    rbind, Reduce, rownames, sapply, setdiff, sort, table, tapply,
    union, unique, unsplit, which.max, which.min

Loading required package: S4Vectors
Loading required package: stats4

Attaching package: ‘S4Vectors’

The following objects are masked from ‘package:base’:

    expand.grid, I, unname

Loading required package: GenomicRanges
Loading required package: GenomeInfoDb
[?25hsnapshotDate(): 2021-10-19
[?25h[?25hLoading required package: GenomicFeatures
Loading required package: AnnotationDbi
Loading required package: Biobase
Welcome to Bioconductor

    Vignettes contain introductory material; view with
    'browseVignettes()'. To cite Bioconductor, see
    'citation("Biobase")', and for packages 'citation("pkgname")'.

Loading required package: AnnotationFilter

Attaching package: 'ensembldb'

The following object is masked from 'package:stats':

    filter

[?25h[?25h[?25hLoading required package: R.oo
Loading required package: R.methodsS3
R.methodsS3 v1.8.2 (2022-06-13 22:00:14 UTC) successfully loaded. See ?R.methodsS3 for help.
R.oo v1.25.0 (2022-06-12 02:20:02 UTC) successfully loaded. See ?R.oo for help.

Attaching package: 'R.oo'

The following object is masked from 'package:R.methodsS3':

    throw

The following object is masked from 'package:GenomicRanges':

    trim

The following object is masked from 'package:IRanges':

    trim

The following objects are masked from 'package:methods':

    getClasses, getMethods

The following objects are masked from 'package:base':

    attach, detach, load, save

R.utils v2.12.2 (2022-11-11 22:00:03 UTC) successfully loaded. See ?R.utils for help.

Attaching package: 'R.utils'

The following object is masked from 'package:utils':

    timestamp

The following objects are masked from 'package:base':

    cat, commandArgs, getOption, isOpen, nullfile, parse, warnings

[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25h[?25hsee ?GenomicDistributionsData and browseVignettes('GenomicDistributionsData') for documentation
loading from cache
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_tssdist"
Scale for x is already present.
Adding another scale for x, which will replace the existing scale.
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_tssdist"
Successfully calculated and plot TSS distance.
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_chrombins"
see ?GenomicDistributionsData and browseVignettes('GenomicDistributionsData') for documentation
loading from cache
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_chrombins"
Successfully calculated and plot chromosomes region distribution.
see ?GenomicDistributionsData and browseVignettes('GenomicDistributionsData') for documentation
loading from cache
Calculating overlaps...
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_paritions"
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_paritions"
Successfully calculated and plot regions distribution over genomic partitions.
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_expected_partitions"
see ?GenomicDistributionsData and browseVignettes('GenomicDistributionsData') for documentation
loading from cache
see ?GenomicDistributionsData and browseVignettes('GenomicDistributionsData') for documentation
loading from cache
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_expected_partitions"
Successfully calculated and plot expected distribution over genomic partitions.
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_cumulative_partitions"
see ?GenomicDistributionsData and browseVignettes('GenomicDistributionsData') for documentation
loading from cache
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_cumulative_partitions"
Successfully calculated and plot cumulative distribution over genomic partitions.
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_widths_histogram"
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_widths_histogram"
Successfully calculated and plot quantile-trimmed histogram of widths.
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_neighbor_distances"
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_neighbor_distances"
Successfully calculated and plot distance between neighbor regions.
[1] "Plotting: /home/bnt4me/virginia/repos/bedbase_all/bedboss/docs_jupyter/all_tutorial/tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_open_chromatin"
[1] "Writing plot json: output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/sample1_open_chromatin"
Successfully calculated and plot cell specific enrichment for open chromatin.
[?25h[?25h</pre>
Command completed. Elapsed time: 0:00:49. Running peak memory: 3.843GB.  
  PID: 434343;	Command: Rscript;	Return code: 0;	Memory used: 3.843GB

These results exist for 'eb617f28e129c401be94069e0fdedbb5': name, regions_no, mean_region_width, md5sum, bedfile, genome, bigbedfile, widths_histogram, neighbor_distances
Starting cleanup: 2 files; 0 conditional files for cleanup

Cleaning up flagged intermediate files. . .

### Pipeline completed. Epilogue
*        Elapsed time (this run):  0:00:50
*  Total elapsed time (all runs):  0:00:50
*         Peak memory (this run):  3.8432 GB
*        Pipeline completed time: 2023-02-27 12:48:16

```

Now let's check if all files where saved


```bash
ls tutorial/bed_files
```

```.output
bedmaker_logs  sample1.bed.gz

```


```bash
ls tutorial/bigbed_files
```

```.output
sample1.bigBed

```


```bash
ls tutorial/output/bedstat_output/eb617f28e129c401be94069e0fdedbb5/
```

```.output
sample1_chrombins.pdf              sample1_open_chromatin.pdf
sample1_chrombins.png              sample1_open_chromatin.png
sample1_cumulative_partitions.pdf  sample1_paritions.pdf
sample1_cumulative_partitions.png  sample1_paritions.png
sample1_expected_partitions.pdf    sample1_plots.json
sample1_expected_partitions.png    sample1_tssdist.pdf
sample1.json                       sample1_tssdist.png
sample1_neighbor_distances.pdf     sample1_widths_histogram.pdf
sample1_neighbor_distances.png     sample1_widths_histogram.png

```

Everything was ran correctly:)
