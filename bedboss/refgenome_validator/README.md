# Reference Genome Predictor and Validator

-----
### Background
Original Lab Blog Post: https://lab.databio.org/docs/guide/Reference-genome-predictor-tool/


## Research Project Outline

#### Question
Original: Can we give a BED file as an input and predict the most likely reference genome assembly associated with the BED file?

Modified: Can we give a BED file as an input and then determine a level of compatibility for different reference genome assemblies?


#### High Level Execution 

1. Ingest query bed file (without any annotations or metadata, for now).

2. Compare regions with chromosome lengths and chrom names.

   - Assumption: if regions exist beyond defined ref genome chromosomes,the bed file is less likely to be associated with the ref genome.
   - Use size files to get chrom lengths and names
   - How many chrom names in BED file that are _not_ in the size file?
   
3. Compare regions in bed files with excluded ranges/black list regions for each of reference genome.

   - Assumption: during bed file creation, excluded ranges were filtered out. Therefore, if a bed file has more regions in excluded ranges from hg19 vs less from hg38, then the bed file is more likely associated with hg38.
   - Two Tiers: 
     1. Gaps/Centromeres/Telomeres
     2. All other Excluded Ranges

4. Once the above are quantified, we can exclude some reference genomes altogether and give probability of compatibility with the remaining.


#### Detailed Execution Steps

1. Retrieve chrom size files for ref genome assemblies (can use seq collections API: https://refget.databio.org/).
2. Cache relevant BED files which contain excluded ranges using [BBClient](https://docs.bedbase.org/geniml/tutorials/bbclient/)
3. Build database for each refgenome assembly excluded ranges, gaps centromeres telomeres using [IGD](https://github.com/databio/gtars/tree/dev_igd) (Use rust implementation if finished, else use C++ implementation).
4. Query "unknown" BED File against chrom size files and the IGDs (using `igd search`).
5. Obtain overlap stats for each of the IGDs, rank them based on _least overlaps_.
6. Run on BED files whose ref genomes are _known_ and calculate accuracy of highest probability compatible ref genome.


#### Additional Notes
- Begin with human bed files and reference genomes for now.
  - predicting between hg38 and hg19 should be the first (simple) task
- Prediction happens at a high level for mutually exclusive options,e.g. predicting hg38 vs hg19 as the coordinate systems are different
  - this mutual exclusivity would also apply to different types of hg38 (UCSC, ensembl, ncbi).
- Validation occurs at the next level after basic prediction is done.
  - different/levels of tiers based on cutoffs wrt specificity and sensitivity
  - These tiers are based on a variety of parameters of increasing complexity:
    - name matching of chromosomes
    - overlaps (chr.sizes, excluded ranges)
    - ML (BED embeddings)
    - Bed annotations (text similarity)
- Future work could involve machine learning for ref genome prediction. However, we will begin with a simple classifier (which may be sufficient).
- Future work could add annotations/metadata for making the prediction.

#### Software Notes

- Create a validator class that can ingest a BED file as well as a genome_model object