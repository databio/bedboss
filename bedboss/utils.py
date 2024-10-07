import logging
import os
import urllib.request
import glob

import requests
from pephubclient.files_manager import FilesManager
import peppy
from peppy.const import SAMPLE_RAW_DICT_KEY
from bedms import AttrStandardizer
from pypiper import PipelineManager

_LOGGER = logging.getLogger("bedboss")


def standardize_genome_name(input_genome: str) -> str:
    """
    Standardizing user provided genome

    :param input_genome: standardize user provided genome, so bedboss know what genome
    we should use
    :return: genome name string
    """
    if not input_genome:
        return ""
    input_genome = input_genome.strip().lower()
    # TODO: we have to add more genome options and preprocessing of the string
    if input_genome == "hg38" or input_genome == "grch38":
        return "hg38"
    elif input_genome == "hg19" or input_genome == "grch37":
        return "hg19"
    elif input_genome == "mm10":
        return "mm10"
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
