OPEN_SIGNAL_FOLDER = "./openSignalMatrix"
OPEN_SIGNAL_URL = "http://big.databio.org/open_chromatin_matrix/"

OS_HG38 = "openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz"
OS_HG19 = "openSignalMatrix_hg19_percentile99_01_quantNormalized_round4d.txt.gz"
OS_MM10 = "openSignalMatrix_mm10_percentile99_01_quantNormalized_round4d.txt.gz"

BED_FOLDER_NAME = "bed_files"
BIGBED_FOLDER_NAME = "bigbed_files"

# bedmaker

# if using PyCharm for development (debugging), you must add these to PATH in the .profile
# export PATH="/home/drc/r_dependencies:$PATH"
BED_TO_BIGBED_PROGRAM = "bedToBigBed"
#BED_TO_BIGBED_PROGRAM = "/home/bnt4me/virginia/repos/bedbase_all/bedboss/bedToBigBed"
#BED_TO_BIGBED_PROGRAM  = "/home/drc/r_dependencies/bedToBigBed"
BIGBED_TO_BED_PROGRAM = "bigBedToBed"
#BIGBED_TO_BED_PROGRAM = "/home/drc/r_dependencies/bigBedToBed"



# COMMANDS TEMPLATES
# bedGraph to bed
BEDGRAPH_TEMPLATE = "macs2 {width} -i {input} -o {output}"
# bigBed to bed
BIGBED_TEMPLATE = f"{BIGBED_TO_BED_PROGRAM} {{input}} {{output}}"
# bigWig to bed
BIGWIG_TEMPLATE = (
    "bigWigToBedGraph {input} /dev/stdout | macs2 {width} -i /dev/stdin -o {output}"
)

WIG_TEMPLATE = "wigToBigWig {input} {chrom_sizes} {intermediate_bw} -clip"
# bed default link
# bed_template = "ln -s {input} {output}"
BED_TEMPLATE = "cp {input} {output}"
# gzip output files
GZIP_TEMPLATE = "gzip {unzipped_converted_file} "

# CONSTANTS
# Creating list of standard chromosome names:
STANDARD_CHROM_LIST = ["chr" + str(chr_nb) for chr_nb in list(range(1, 23))]
STANDARD_CHROM_LIST[len(STANDARD_CHROM_LIST) :] = ["chrX", "chrY", "chrM"]

# bedqc
MAX_FILE_SIZE = 1024 * 1024 * 1024 * 2
MAX_REGION_NUMBER = 5000000
MIN_REGION_WIDTH = 10

# bedstat
