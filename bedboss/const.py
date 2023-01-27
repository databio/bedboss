OPEN_SIGNAL_FOLDER = "./openSignalMatrix"
OPEN_SIGNAL_URL = "https://big.databio.org/open_chromatin_matrix/"

OS_HG38 = "openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz"
OS_HG19 = "openSignalMatrix_hg19_percentile99_01_quantNormalized_round4d.txt.gz"
OS_MM10 = "openSignalMatrix_mm10_percentile99_01_quantNormalized_round4d.txt.gz"

BED_FOLDER_NAME = "bed_files"
BIGBED_FOLDER_NAME = "bigbed_files"

# bedmaker

BED_TO_BIGBED_PROGRAM = "bedToBigBed"
BIGBED_TO_BED_PROGRAM = "bigBedToBed"


# COMMANDS TEMPLATES
# bedGraph to bed
BEDGRAPH_TEMPLATE = "macs2 {width} -i {input} -o {output}"
# bigBed to bed
BIGBED_TEMPLATE = f"{BIGBED_TO_BED_PROGRAM} {{input}} {{output}}"
# bigWig to bed
BIGWIG_TEMPLATE = (
    "bigWigToBedGraph {input} /dev/stdout | macs2 {width} -i /dev/stdin -o {output}"
)
# preliminary for wig to bed
# wig_template =  "wigToBigWig {input} {chrom_sizes} /dev/stdout -clip | bigWigToBedGraph /dev/stdin  /dev/stdout | macs2 {width} -i /dev/stdin -o {output}"
WIG_TEMPLATE = "wigToBigWig {input} {chrom_sizes} {intermediate_bw} -clip"
# bed default link
# bed_template = "ln -s {input} {output}"
BED_TEMPLATE = "cp {input} {output}"
# gzip output files
GZIP_TEMPLATE = "gzip {unzipped_converted_file} "

# CONSTANTS
# Creating list of standard chromosome names:
STANDARD_CHROM_LIST = ["chr" + str(chr_nb) for chr_nb in list(range(1, 23))]
STANDARD_CHROM_LIST[len(STANDARD_CHROM_LIST):] = ["chrX", "chrY", "chrM"]

# bedqc
MAX_FILE_SIZE = 1024 * 1024 * 1024 * 2
MAX_REGION_SIZE = 5000000
MIN_REGION_WIDTH = 10

