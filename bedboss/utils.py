import glob
import logging
import os
import time
import urllib.request
from functools import wraps
import gzip
from io import StringIO
import pandas as pd

import peppy
import requests
from bedms import AttrStandardizer
from pephubclient.files_manager import FilesManager
from peppy.const import SAMPLE_RAW_DICT_KEY
from pypiper import PipelineManager

from bedboss.refgenome_validator.main import ReferenceValidator
from bedboss.exceptions import QualityException
from bedboss.const import MIN_REGION_WIDTH

_LOGGER = logging.getLogger("bedboss")


def standardize_genome_name(input_genome: str, bedfile: str = None) -> str:
    """
    Standardizing user provided genome

    :param input_genome: standardize user provided genome, so bedboss know what genome
    we should use
    :param bedfile: path to bed file
    :return: genome name string
    """
    if not isinstance(input_genome, str):
        input_genome = ""
    input_genome = input_genome.strip().lower()
    # TODO: we have to add more genome options and preprocessing of the string
    if input_genome == "hg38" or input_genome == "grch38":
        return "hg38"
    elif input_genome == "hg19" or input_genome == "grch37":
        return "hg19"
    elif input_genome == "mm10" or input_genome == "grcm38":
        return "mm10"
    elif input_genome == "mm9" or input_genome == "grcm37":
        return "mm9"

    elif not input_genome or len(input_genome) > 7:
        if bedfile:
            predictor = ReferenceValidator()
            return predictor.predict(bedfile) or ""
        else:
            return input_genome
    # else:
    #     raise GenomeException("Incorrect genome assembly was provided")
    else:
        return input_genome


def download_file(url: str, path: str, no_fail: bool = False) -> None:
    """
    Download file from the url to specific location

    :param url: URL of the file
    :param path: Local path with filename
    :param no_fail: If True, do not raise exception if download fails
    :return: NoReturn
    """
    _LOGGER.info(f"Downloading remote file: {url}")
    _LOGGER.info(f"Local path: {os.path.abspath(path)}")
    try:
        urllib.request.urlretrieve(url, path)
        _LOGGER.info("File downloaded successfully!")
    except Exception as e:
        _LOGGER.error("File download failed.")
        if not no_fail:
            raise e
        _LOGGER.error("File download failed. Continuing anyway...")


def get_genome_digest(genome: str) -> str:
    return requests.get(
        f"http://refgenomes.databio.org/genomes/genome_digest/{genome}"
    ).text.strip('""')


def example_bedbase_config():
    """
    Return example configuration for BedBase
    """
    return {
        "database": {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "docker",
            "database": "bedbase",
            "dialect": "postgresql",
            "driver": "psycopg",
        },
        "qdrant": {
            "host": "localhost",
            "port": 6333,
            "api_key": None,
            "collection": "test_collection",
        },
        "server": {"host": "0.0.0.0", "port": 8000},
        "path": {
            "region2vec": "databio/r2v-encode-hg38",
            "vec2vec": "databio/v2v-geo-hg38",
            "text2vec": "sentence-transformers/all-MiniLM-L6-v2",
        },
        "access_methods": {
            "http": {
                "type": "https",
                "description": "HTTP compatible path",
                "prefix": "https://data2.bedbase.org/",
            },
            "s3": {
                "type": "s3",
                "description": "S3 compatible path",
                "prefix": "s3://data2.bedbase.org/",
            },
            "local": {
                "type": "https",
                "description": "How to serve local files.",
                "prefix": "/static/",
            },
        },
        "s3": {
            "endpoint_url": None,
            "aws_access_key_id": None,
            "aws_secret_access_key": None,
            "bucket": "bedbase",
        },
        "phc": {"namespace": "bedbase", "name": "bedbase", "tag": "latest"},
    }


def save_example_bedbase_config(path: str) -> None:
    """
    Save example configuration for BedBase

    :param path: path to the file
    """
    file_path = os.path.abspath(os.path.join(path, "bedbase_config.yaml"))
    FilesManager.save_yaml(example_bedbase_config(), file_path)
    _LOGGER.info(f"Example BedBase configuration saved to: {file_path}")


def standardize_pep(
    pep: peppy.Project, standard_columns: list = None, model: str = "BEDBASE"
) -> peppy.Project:
    """
    Standardize PEP file by using bedMS standardization model
    :param pep: peppy project
    :param standard_columns: list of columns to standardize

    :return: peppy project

    """
    if standard_columns is None:
        standard_columns = ["library_source", "assay", "genome", "species_name"]
    model = AttrStandardizer(model)
    suggestions = model.standardize(pep)

    changes = {}
    if suggestions is None:
        return pep
    for original, suggestion_dict in suggestions.items():
        for suggestion, value in suggestion_dict.items():
            if value > 0.9 and suggestion in standard_columns:
                if suggestion not in changes:
                    changes[suggestion] = {original: value}
                else:
                    if list(changes[suggestion].values())[0] < value:
                        changes[suggestion] = {original: value}

    raw_pep = pep.to_dict(extended=True)
    for suggestion, original_dict in changes.items():
        original_key = list(original_dict.keys())[0]
        if (
            suggestion not in raw_pep[SAMPLE_RAW_DICT_KEY]
            and original_key in raw_pep[SAMPLE_RAW_DICT_KEY]
        ):
            raw_pep[SAMPLE_RAW_DICT_KEY][suggestion] = raw_pep[SAMPLE_RAW_DICT_KEY][
                original_key
            ]
            del raw_pep[SAMPLE_RAW_DICT_KEY][original_key]

    return peppy.Project.from_dict(raw_pep)


def cleanup_pm_temp(pm: PipelineManager) -> None:
    """
    Cleanup temporary files from the PipelineManager

    :param pm: PipelineManager
    """
    if len(pm.cleanup_list_conditional) > 0:
        for cleandir in pm.cleanup_list_conditional:
            try:
                items_to_clean = glob.glob(cleandir)
                for clean_item in items_to_clean:
                    if os.path.isfile(clean_item):
                        os.remove(clean_item)
                    elif os.path.isdir(clean_item):
                        os.rmdir(clean_item)
            except Exception as e:
                _LOGGER.error(f"Error cleaning up: {e}")
        pm.cleanup_list_conditional = []


def calculate_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        # print(f"--> Arguments: {args}")
        # print(f"--> Keyword arguments: {kwargs}")

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time

        hours, remainder = divmod(execution_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        print(
            f"Function '{func.__name__}' executed in {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds"
        )

        return result

    return wrapper


def run_initial_qc(url: str, min_region_width: int = MIN_REGION_WIDTH) -> bool:
    """
    Run initial QC on the bed file

    :param url: URL of the file
    :param min_region_width: Minimum region width threshold to pass the quality check. Default is 20

    :return: bool. Returns True if QC passed, False if unable to open in pandas
    :raises: QualityException
    """
    _LOGGER.info(f"Running initial QC on the bed file: {url}")

    try:
        with urllib.request.urlopen(url) as response:
            with gzip.GzipFile(fileobj=response) as f:
                content = f.read(10240).decode()  # Read first 10KB after decompression

        df = pd.read_csv(StringIO(content), sep="\t", header=None)
        mean_width = (df.iloc[:, 2] - df.iloc[:, 1])[:-1].mean()

    except Exception as err:
        _LOGGER.warning(
            "Unable to read the file, initial QC failed, but continuing anyway..."
            f"Error: {str(err)}"
        )
        return False

    if mean_width < min_region_width:
        raise QualityException(
            f"Initial QC failed for '{url}'. Mean region width is '{mean_width}', where min region width is set to: '{min_region_width}'"
        )

    _LOGGER.info(f"Initial QC passed for {url}")
    return True
