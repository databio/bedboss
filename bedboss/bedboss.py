import logging
import os
import urllib.request
from typing import NoReturn, Union

from .bedstat.bedstat import run_bedstat
from .bedmaker.bedmaker import BedMaker
from .const import (
    OS_HG19,
    OS_HG38,
    OS_MM10,
    OPEN_SIGNAL_FOLDER,
    OPEN_SIGNAL_URL,
    BED_FOLDER_NAME,
    BIGBED_FOLDER_NAME,
)
from .exceptions import OSMException, GenomeException

_LOGGER = logging.getLogger("bedboss")


def download_file(url: str, path: str) -> NoReturn:
    """
    Download file from the url to specific location
    :param url: URL of the file
    :param path: Local path with filename
    :return: NoReturn
    """
    _LOGGER.info(f"Downloading file: {url}")
    _LOGGER.info(f"Downloading location: {path}")
    urllib.request.urlretrieve(url, path)
    _LOGGER.info(f"File has been downloaded successfully!")


def get_osm_path(genome: str) -> Union[str, None]:
    """
    By providing genome name download Open Signal Matrix
    :param genome: genome assembly
    :return: path to the Open Signal Matrix
    """
    _LOGGER.info(f"Getting Open Signal Matrix file path...")
    if genome == "hg19":
        osm_name = OS_HG19
    elif genome == "hg38":
        osm_name = OS_HG38
    elif genome == "mm10":
        osm_name = OS_MM10
    else:
        # raise OSMException(
        #     "For this genome open Signal Matrix was not found. Exiting..."
        # )
        return None
    osm_path = os.path.join(OPEN_SIGNAL_FOLDER, osm_name)
    if not os.path.exists(osm_path):
        if not os.path.exists(OPEN_SIGNAL_FOLDER):
            os.makedirs(OPEN_SIGNAL_FOLDER)
        download_file(url=f"{OPEN_SIGNAL_URL}{osm_name}", path=osm_path)
    return osm_path


def standard_genome_name(input_genome: str) -> str:
    """
    Standardizing user provided genome
    :param input_genome: standardize user provided genome, so bedboss know what genome
    we should use
    :return: genome name string
    """
    _LOGGER.info(f"processing genome name...")
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
        return input()


def extract_file_name(file_path: str) -> str:
    """
    Extraction file name from file path
    :param file_path: full file path
    :return: file name without extension
    """
    file_name = os.path.basename(file_path)
    file_name = file_name.split(".")[0]
    return file_name


def run_bedboss(
    sample_name: str,
    input_file: str,
    input_type: str,
    output_folder: str,
    genome: str,
    bedbase_config: str,
    rfg_config: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    standard_chrom: bool = False,
    chrom_sizes: str = None,
    open_signal_matrix: str = None,
    ensdb: str = None,
    sample_yaml: str = None,
    just_db_commit: bool = False,
    no_db_commit: bool = False,
) -> NoReturn:
    """
    Running bedmaker, bedqc and bedstat in one package
    :param sample_name: Sample name [required]
    :param input_file: Input file [required]
    :param input_type: Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
    :param output_folder: Folder, where output should be saved  [required]
    :param genome: genome_assembly of the sample. [required] options: (hg19, hg38) #TODO: add more
    :param bedbase_config: a path to the bedbase configuration file. [required] #TODO: add example
    :param open_signal_matrix: a full path to the openSignalMatrix required for the tissue [optional]
    :param rfg_config: file path to the genome config file [optional]
    :param narrowpeak: whether the regions are narrow
        (transcription factor implies narrow, histone mark implies broad peaks) [optional]
    :param check_qc: set True to run quality control during badmaking [optional] (default: True)
    :param standard_chrom: Standardize chromosome names. [optional] (Default: False)
    :param chrom_sizes: a full path to the chrom.sizes required for the bedtobigbed conversion [optional]
    :param sample_yaml: a yaml config file with sample attributes to pass on MORE METADATA into the database [optional]
    :param ensdb: a full path to the ensdb gtf file required for genomes not in GDdata [optional]
        (basically genomes that's not in GDdata)
    :param just_db_commit: whether just to commit the JSON to the database (default: False)
    :param no_db_commit: whether the JSON commit to the database should be skipped (default: False)
    :return: NoReturn
    """

    file_name = extract_file_name(input_file)
    genome = standard_genome_name(genome)
    cwd = os.getcwd()
    if not rfg_config:
        rfg_config = os.path.join(cwd, "genome_config.yaml")

    # find/download open signal matrix
    if not open_signal_matrix:
        open_signal_matrix = get_osm_path(genome)
    else:
        if not os.path.exists(open_signal_matrix):
            open_signal_matrix = get_osm_path(genome)

    if not sample_yaml:
        sample_yaml = f"{sample_name}.yaml"

    output_bed = os.path.join(output_folder, BED_FOLDER_NAME, f"{file_name}.bed.gz")
    output_bigbed = os.path.join(output_folder, BIGBED_FOLDER_NAME)
    _LOGGER.info(f"output_bed = {output_bed}")
    _LOGGER.info(f"output_bigbed = {output_bigbed}")

    # TODO: should we keep bed and bigfiles in output folder?
    # set env for bedstat:
    output_folder_bedstat = os.path.join(output_folder, "output")
    os.environ["BEDBOSS_OUTPUT_PATH"] = output_folder_bedstat

    BedMaker(
        input_file=input_file,
        input_type=input_type,
        output_bed=output_bed,
        output_bigbed=output_bigbed,
        sample_name=sample_name,
        genome=genome,
        rfg_config=rfg_config,
        narrowpeak=narrowpeak,
        check_qc=check_qc,
        standard_chrom=standard_chrom,
        chrom_sizes=chrom_sizes,
    ).make()

    run_bedstat(
        bedfile=output_bed,
        bigbed=output_bigbed,
        genome_assembly=genome,
        ensdb=ensdb,
        open_signal_matrix=open_signal_matrix,
        bedbase_config=bedbase_config,
        sample_yaml=sample_yaml,
        just_db_commit=just_db_commit,
        no_db_commit=no_db_commit,
    )
