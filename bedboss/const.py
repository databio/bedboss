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
MAX_REGION_NUMBER = 5000000
MIN_REGION_WIDTH = 10

# bedstat

# bedbuncher
DEFAULT_BEDBASE_CACHE_PATH = "./bedabse_cache"

BEDBOSS_PEP_SCHEMA_PATH = "https://schema.databio.org/pipelines/bedboss.yaml"
REFGENIE_ENV_VAR = "REFGENIE"

DEFAULT_REFGENIE_PATH = os.path.join(HOME_PATH, ".refgenie")

BED_PEP_REGISTRY = "databio/allbeds:bedbase"
