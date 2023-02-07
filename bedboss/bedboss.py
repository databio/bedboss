import logging
import os
import urllib.request
from typing import NoReturn, Union, Dict

from .bedstat.bedstat import run_bedstat
from .bedmaker.bedmaker import BedMaker
from .bedqc.bedqc import bedqc
from .const import (
    OS_HG19,
    OS_HG38,
    OS_MM10,
    OPEN_SIGNAL_FOLDER,
    OPEN_SIGNAL_URL,
    BED_FOLDER_NAME,
    BIGBED_FOLDER_NAME,
)
from .utils import extract_file_name, standardize_genome_name, download_file
from .exceptions import OSMException, GenomeException

_LOGGER = logging.getLogger("bedboss")


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


def run_all(
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
    Run bedboss: bedmaker, bedqc and bedstat.
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
    genome = standardize_genome_name(genome)

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


def bedboss(pipeline: str, args_dict: Dict[str, str]) -> NoReturn:
    """
    Run pipeline that was specified in as positional argument.
    :param str pipeline: one of the bedboss pipelines
    :param dict args_dict: dict of arguments used in provided pipeline.
    """
    if pipeline == "all":
        run_all(**args_dict)
    elif pipeline == "make":
        BedMaker(**args_dict).make()
    elif pipeline == "qc":
        bedqc(**args_dict)
    elif pipeline == "stat":
        run_bedstat(**args_dict)
    else:
        raise Exception("Incorrect pipeline name.")
