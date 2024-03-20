import os
import logging
import urllib.request
import requests


_LOGGER = logging.getLogger("bedboss")


def standardize_genome_name(input_genome: str) -> str:
    """
    Standardizing user provided genome

    :param input_genome: standardize user provided genome, so bedboss know what genome
    we should use
    :return: genome name string
    """
    input_genome = input_genome.strip()
    # TODO: we have to add more genome options and preprocessing of the string
    if input_genome == "hg38" or input_genome == "GRCh38":
        return "hg38"
    elif input_genome == "hg19" or input_genome == "GRCh37":
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
