import logging
import os
import urllib.request
from typing import NoReturn, Union, Dict
import pypiper
from argparse import Namespace

from bedboss.bedstat.bedstat import bedstat
from bedboss.bedmaker.bedmaker import BedMaker
from bedboss.bedqc.bedqc import bedqc
from bedboss.cli import build_argparser

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
from .exceptions import OpenSignalMatrixException

_LOGGER = logging.getLogger("bedboss")


def get_osm_path(genome: str) -> Union[str, None]:
    """
    By providing genome name download Open Signal Matrix
    :param genome: genome assembly
    :return: path to the Open Signal Matrix
    """
    # TODO: add more osm
    _LOGGER.info(f"Getting Open Signal Matrix file path...")
    if genome == "hg19":
        osm_name = OS_HG19
    elif genome == "hg38":
        osm_name = OS_HG38
    elif genome == "mm10":
        osm_name = OS_MM10
    else:
        raise OpenSignalMatrixException(
            "For this genome open Signal Matrix was not found. Exiting..."
        )
        # return None
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
    outfolder: str,
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
    force_overwrite: bool = False,
    pm: pypiper.PipelineManager = None,
    **kwargs,
) -> NoReturn:
    """
    Run bedboss: bedmaker, bedqc and bedstat.
    :param sample_name: Sample name [required]
    :param input_file: Input file [required]
    :param input_type: Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
    :param outfolder: Folder, where output should be saved  [required]
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
    :param force_overwrite: force overwrite analysis
    :param no_db_commit: whether the JSON commit to the database should be skipped (default: False)
    :param pm: pypiper object
    :return: NoReturn
    """
    _LOGGER.warning(f"Unused arguments: {kwargs}")
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

    output_bed = os.path.join(outfolder, BED_FOLDER_NAME, f"{file_name}.bed.gz")
    output_bigbed = os.path.join(outfolder, BIGBED_FOLDER_NAME)

    _LOGGER.info(f"output_bed = {output_bed}")
    _LOGGER.info(f"output_bigbed = {output_bigbed}")

    # set env for bedstat:
    output_folder_bedstat = os.path.join(outfolder, "output")
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
        pm=pm,
    )

    bedstat(
        bedfile=output_bed,
        bigbed=output_bigbed,
        genome_assembly=genome,
        ensdb=ensdb,
        open_signal_matrix=open_signal_matrix,
        bedbase_config=bedbase_config,
        sample_yaml=sample_yaml,
        just_db_commit=just_db_commit,
        no_db_commit=no_db_commit,
        force_overwrite=force_overwrite,
        pm=pm,
    )


def main(test_args: dict = None) -> NoReturn:
    """
    Run pipeline that was specified in as positional argument.
    :param str pipeline: one of the bedboss pipelines
    :param dict args_dict: dict of arguments used in provided pipeline.
    """
    # parser = logmuse.add_logging_options(build_argparser())
    parser = build_argparser()
    if test_args:
        args_dict = test_args
    else:
        args, _ = parser.parse_known_args()
        args_dict = vars(args)
    # TODO: use Pypiper to simplify/standardize arg parsing

    pm = pypiper.PipelineManager(
        name="bedboss-pipeline",
        outfolder=args_dict.get("outfolder")
        if args_dict.get("outfolder")
        else "test_outfolder",
        recover=True,
        multi=True,
    )

    if args_dict["command"] == "all":
        run_all(pm=pm, **args_dict)
    elif args_dict["command"] == "make":
        BedMaker(pm=pm, **args_dict)
    elif args_dict["command"] == "qc":
        bedqc(pm=pm, **args_dict)
    elif args_dict["command"] == "stat":
        bedstat(pm=pm, **args_dict)
    else:
        pm.stop_pipeline()
        raise Exception("Incorrect pipeline name.")
    pm.stop_pipeline()
