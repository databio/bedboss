import os

PKG_NAME: str = "bedboss"
DEFAULT_BEDBASE_API_URL: str = "https://api.bedbase.org"
# DEFAULT_BEDBASE_API_URL = "http://localhost:8000/api"

HOME_PATH: str = os.getenv("HOME") or os.path.expanduser("~")

OPEN_SIGNAL_FOLDER_NAME: str = "openSignalMatrix"
OPEN_SIGNAL_URL: str = "http://big.databio.org/open_chromatin_matrix/"

OS_HG38: str = "openSignalMatrix_hg38_percentile99_01_quantNormalized_round4d.txt.gz"
OS_HG19: str = "openSignalMatrix_hg19_percentile99_01_quantNormalized_round4d.txt.gz"
OS_MM10: str = "openSignalMatrix_mm10_percentile99_01_quantNormalized_round4d.txt.gz"

BED_FOLDER_NAME: str = "bed_files"
BIGBED_FOLDER_NAME: str = "bigbed_files"
OUTPUT_FOLDER_NAME: str = "output"
BEDSTAT_OUTPUT: str = "bedstat_output"
QC_FOLDER_NAME: str = "bed_qc"

# bedqc
MAX_FILE_SIZE: int = 1024 * 1024 * 1024 * 2
MAX_FILE_SIZE_QC: int = 1024 * 1024 * 25  # 25 MB
MAX_REGION_NUMBER: int = 5000000
MIN_REGION_WIDTH: int = 10

# bedstat

# bedbuncher
DEFAULT_BEDBASE_CACHE_PATH: str = "./bedbase_cache"

BEDBOSS_PEP_SCHEMA_PATH: str = "https://schema.databio.org/pipelines/bedboss.yaml"
REFGENIE_ENV_VAR: str = "REFGENIE"

DEFAULT_REFGENIE_PATH: str = os.path.join(HOME_PATH, ".refgenie")

BED_PEP_REGISTRY: str = "databio/allbeds:bedbase"

# UMAP constants
DB_QUERY_BATCH_SIZE: int = 5000
UMAP_GENOME: str = "hg38"

UMAP_PARQUET_COLUMNS: list[str] = [
    "id",
    "x",
    "y",
    "name",
    "description",
    "assay",
    "cell_line",
    "cell_type",
    "tissue",
]
