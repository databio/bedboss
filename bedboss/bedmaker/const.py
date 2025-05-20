BED_TO_BIGBED_PROGRAM = "bedToBigBed"
# BED_TO_BIGBED_PROGRAM = "/home/bnt4me/virginia/repos/bedbase_all/bedboss/bedToBigBed"
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

QC_FOLDER_NAME = "bed_qc"

BIGBED_FOLDER_NAME = "bigbed_files"
