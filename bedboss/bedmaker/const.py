# COMMANDS TEMPLATES

# bedGraph to bed
BEDGRAPH_TEMPLATE = "macs2 {width} -i {input} -o {output}"
# bigBed to bed
BIGBED_TEMPLATE = "bigBedToBed {input} {output}"
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
STANDARD_CHROM_LIST[len(STANDARD_CHROM_LIST) :] = ["chrX", "chrY", "chrM"]

# bedqc
MAX_FILE_SIZE = 1024 * 1024 * 1024 * 2
MAX_REGION_SIZE = 5000000
MIN_REGION_WIDTH = 10
