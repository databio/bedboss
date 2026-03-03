import os

PKG_NAME = "bedboss"
DEFAULT_BEDBASE_API_URL = "https://api.bedbase.org"
# DEFAULT_BEDBASE_API_URL = "http://localhost:8000/api"

HOME_PATH = os.getenv("HOME")
if not HOME_PATH:
    HOME_PATH = os.path.expanduser("~")

OPEN_SIGNAL_FOLDER_NAME = "openSignalMatrix"
OPEN_SIGNAL_URL = "http://big.databio.org/open_chromatin_matrix/"

OS_HG38 = "openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz"
OS_HG19 = "openSignalMatrix_hg19_percentile99_01_quantNormalized_round4d.txt.gz"
OS_MM10 = "openSignalMatrix_mm10_percentile99_01_quantNormalized_round4d.txt.gz"

BED_FOLDER_NAME = "bed_files"
BIGBED_FOLDER_NAME = "bigbed_files"
OUTPUT_FOLDER_NAME = "output"
BEDSTAT_OUTPUT = "bedstat_output"
QC_FOLDER_NAME = "bed_qc"

# bedqc
MAX_FILE_SIZE = 1024 * 1024 * 1024 * 2
MAX_FILE_SIZE_QC = 1024 * 1024 * 25  # 25 MB
MAX_REGION_NUMBER = 5000000
MIN_REGION_WIDTH = 10

# bedstat — Ensembl GTF auto-download
GTF_FOLDER_NAME = "ensembl_gtf"
ENSEMBL_RELEASE = 114
ENSEMBL_GENOMES_RELEASE = 62  # Ensembl Genomes (plants, fungi, protists)

# UCSC/common genome alias -> (species, assembly, release, division)
# division: "ensembl" | "grch37" | "plants" | "fungi" | "protists"
# Legacy assemblies are pinned to the last Ensembl release that included them.
# Genomes not in this map are resolved dynamically via Ensembl REST API.
ENSEMBL_GENOMES = {
    # Human
    "hg38": ("homo_sapiens", "GRCh38", ENSEMBL_RELEASE, "ensembl"),
    "GRCh38": ("homo_sapiens", "GRCh38", ENSEMBL_RELEASE, "ensembl"),
    "hg19": ("homo_sapiens", "GRCh37", 87, "grch37"),
    "GRCh37": ("homo_sapiens", "GRCh37", 87, "grch37"),
    "hg18": ("homo_sapiens", "NCBI36", 54, "ensembl"),
    "NCBI36": ("homo_sapiens", "NCBI36", 54, "ensembl"),
    # Mouse
    "mm39": ("mus_musculus", "GRCm39", ENSEMBL_RELEASE, "ensembl"),
    "GRCm39": ("mus_musculus", "GRCm39", ENSEMBL_RELEASE, "ensembl"),
    "mm10": ("mus_musculus", "GRCm38", 102, "ensembl"),
    "GRCm38": ("mus_musculus", "GRCm38", 102, "ensembl"),
    "mm9": ("mus_musculus", "NCBIM37", 67, "ensembl"),
    "NCBIM37": ("mus_musculus", "NCBIM37", 67, "ensembl"),
    # Drosophila
    "dm6": ("drosophila_melanogaster", "BDGP6.54", ENSEMBL_RELEASE, "ensembl"),
    "dm3": ("drosophila_melanogaster", "BDGP5", 75, "ensembl"),
    # C. elegans
    "ce11": ("caenorhabditis_elegans", "WBcel235", ENSEMBL_RELEASE, "ensembl"),
    "ce10": ("caenorhabditis_elegans", "WS220", 66, "ensembl"),
    # Yeast
    "sacCer3": ("saccharomyces_cerevisiae", "R64-1-1", ENSEMBL_RELEASE, "ensembl"),
    "saccer3": ("saccharomyces_cerevisiae", "R64-1-1", ENSEMBL_RELEASE, "ensembl"),
    # Arabidopsis
    "tair10": ("arabidopsis_thaliana", "TAIR10", ENSEMBL_GENOMES_RELEASE, "plants"),
    "TAIR10": ("arabidopsis_thaliana", "TAIR10", ENSEMBL_GENOMES_RELEASE, "plants"),
    # Dictyostelium
    "ax4": ("dictyostelium_discoideum", "dicty_2.7", ENSEMBL_GENOMES_RELEASE, "protists"),
}

# bedstat — chrom.sizes via seqcol
REFGENIE_API_URL = "https://api.refgenie.org"
CHROM_SIZES_FOLDER_NAME = "chrom_sizes"

# bedbuncher
DEFAULT_BEDBASE_CACHE_PATH = "./bedabse_cache"

BEDBOSS_PEP_SCHEMA_PATH = "https://schema.databio.org/pipelines/bedboss.yaml"
REFGENIE_ENV_VAR = "REFGENIE"

DEFAULT_REFGENIE_PATH = os.path.join(HOME_PATH, ".refgenie")

BED_PEP_REGISTRY = "databio/allbeds:bedbase"
