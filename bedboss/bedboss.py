import logging
import os
from typing import NoReturn, Union

import pypiper
from argparse import Namespace
import logmuse
import peppy
from eido import validate_project
import pephubclient
from pephubclient.helpers import is_registry_path
import bbconf

from bedboss.bedstat.bedstat import bedstat
from bedboss.bedmaker.bedmaker import BedMaker
from bedboss.bedqc.bedqc import bedqc
from bedboss.bedbuncher import run_bedbuncher
from bedboss.qdrant_index import add_to_qdrant
from bedboss.cli import build_argparser
from bedboss.const import (
    OS_HG19,
    OS_HG38,
    OS_MM10,
    OPEN_SIGNAL_FOLDER,
    OPEN_SIGNAL_URL,
    BED_FOLDER_NAME,
    BIGBED_FOLDER_NAME,
    BEDBOSS_PEP_SCHEMA_PATH,
    OUTPUT_FOLDER_NAME,
)
from bedboss.utils import (
    extract_file_name,
    standardize_genome_name,
    download_file,
    check_db_connection,
)
from bedboss.exceptions import OpenSignalMatrixException, BedBossException
from bedboss._version import __version__

_LOGGER = logging.getLogger("bedboss")


def get_osm_path(genome: str) -> Union[str, None]:
    """
    By providing genome name download Open Signal Matrix

    :param genome: genome assembly
    :return: path to the Open Signal Matrix
    """
    # TODO: add more osm
    _LOGGER.info("Getting Open Signal Matrix file path...")
    if genome == "hg19" or genome == "GRCh37":
        osm_name = OS_HG19
    elif genome == "hg38" or genome == "GRCh38":
        osm_name = OS_HG38
    elif genome == "mm10" or genome == "GRCm38":
        osm_name = OS_MM10
    else:
        raise OpenSignalMatrixException(
            "For this genome open Signal Matrix was not found."
        )

    osm_path = os.path.join(OPEN_SIGNAL_FOLDER, osm_name)
    if not os.path.exists(osm_path):
        if not os.path.exists(OPEN_SIGNAL_FOLDER):
            os.makedirs(OPEN_SIGNAL_FOLDER)
        download_file(
            url=f"{OPEN_SIGNAL_URL}{osm_name}",
            path=osm_path,
            no_fail=True,
        )
    return osm_path


def run_all(
    sample_name: str,
    input_file: str,
    input_type: str,
    outfolder: str,
    genome: str,
    bedbase_config: Union[str, bbconf.BedBaseConf],
    rfg_config: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    standard_chrom: bool = False,
    chrom_sizes: str = None,
    open_signal_matrix: str = None,
    ensdb: str = None,
    treatment: str = None,
    description: str = None,
    cell_type: str = None,
    other_metadata: dict = None,
    just_db_commit: bool = False,
    no_db_commit: bool = False,
    force_overwrite: bool = False,
    skip_qdrant: bool = True,
    pm: pypiper.PipelineManager = None,
    **kwargs,
) -> str:
    """
    Run bedboss: bedmaker, bedqc, bedstat, and bedbuncher pipelines from PEP.

    :param sample_name: Sample name [required]
    :param input_file: Input file [required]
    :param input_type: Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
    :param outfolder: Folder, where output should be saved  [required]
    :param genome: genome_assembly of the sample. [required] options: (hg19, hg38) #TODO: add more
    :param bedbase_config: The path to the bedbase configuration file, or bbconf object.
    :param open_signal_matrix: a full path to the openSignalMatrix required for the tissue [optional]
    :param rfg_config: file path to the genome config file [optional]
    :param narrowpeak: whether the regions are narrow
        (transcription factor implies narrow, histone mark implies broad peaks) [optional]
    :param check_qc: set True to run quality control during badmaking [optional] (default: True)
    :param standard_chrom: Standardize chromosome names. [optional] (Default: False)
    :param chrom_sizes: a full path to the chrom.sizes required for the bedtobigbed conversion [optional]
        :param str description: a description of the bed file
    :param str treatment: a treatment of the bed file
    :param str cell_type: a cell type of the bed file
    :param dict other_metadata: a dictionary of other metadata to pass
    :param ensdb: a full path to the ensdb gtf file required for genomes not in GDdata [optional]
        (basically genomes that's not in GDdata)
    :param just_db_commit: whether just to commit the JSON to the database (default: False)
    :param force_overwrite: force overwrite analysis
    :param no_db_commit: whether the JSON commit to the database should be skipped (default: False)
    :param skip_qdrant: whether to skip qdrant indexing
    :param pm: pypiper object
    :return: bed digest
    """
    _LOGGER.warning(f"Unused arguments: {kwargs}")

    if isinstance(bedbase_config, str):
        if not check_db_connection(bedbase_config=bedbase_config):
            raise Exception("Database connection failed. Exiting...")

    file_name = extract_file_name(input_file)
    genome = standardize_genome_name(genome)

    # find/download open signal matrix
    if not open_signal_matrix or not os.path.exists(open_signal_matrix):
        try:
            open_signal_matrix = get_osm_path(genome)
        except OpenSignalMatrixException:
            _LOGGER.warning(
                f"Open Signal Matrix was not found for {genome}. Skipping..."
            )
            open_signal_matrix = None

    output_bed = os.path.join(outfolder, BED_FOLDER_NAME, f"{file_name}.bed.gz")
    output_bigbed = os.path.join(outfolder, BIGBED_FOLDER_NAME)

    _LOGGER.info(f"output_bed = {output_bed}")
    _LOGGER.info(f"output_bigbed = {output_bigbed}")

    # set env for bedstat:
    output_folder_bedstat = os.path.join(outfolder, "output")
    os.environ["BEDBOSS_OUTPUT_PATH"] = output_folder_bedstat

    if not pm:
        pm_out_folder = os.path.join(os.path.abspath(outfolder), "pipeline_manager")
        pm = pypiper.PipelineManager(
            name="bedboss-pipeline",
            outfolder=pm_out_folder,
            version=__version__,
            recover=True,
        )

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

    bed_digest = bedstat(
        bedfile=output_bed,
        outfolder=outfolder,
        bedbase_config=bedbase_config,
        genome=genome,
        ensdb=ensdb,
        open_signal_matrix=open_signal_matrix,
        bigbed=output_bigbed,
        description=description,
        treatment=treatment,
        cell_type=cell_type,
        other_metadata=other_metadata,
        just_db_commit=just_db_commit,
        no_db_commit=no_db_commit,
        force_overwrite=force_overwrite,
        skip_qdrant=skip_qdrant,
        pm=pm,
    )
    return bed_digest


def insert_pep(
    bedbase_config: str,
    output_folder: str,
    pep: Union[str, peppy.Project],
    rfg_config: str = None,
    create_bedset: bool = True,
    skip_qdrant: bool = True,
    check_qc: bool = True,
    standard_chrom: bool = False,
    ensdb: str = None,
    just_db_commit: bool = False,
    no_db_commit: bool = False,
    force_overwrite: bool = False,
    upload_s3: bool = False,
    pm: pypiper.PipelineManager = None,
    *args,
    **kwargs,
) -> NoReturn:
    """
    Run all bedboss pipelines for all samples in the pep file.
    bedmaker -> bedqc -> bedstat -> qdrant_indexing -> bedbuncher

    :param bedbase_config: bedbase configuration file path
    :param output_folder: output statistics folder
    :param pep: path to the pep file or pephub registry path
    :param rfg_config: path to the genome config file (refgenie)
    :param create_bedset: whether to create bedset
    :param skip_qdrant: whether to skip qdrant indexing
    :param check_qc: whether to run quality control during badmaking
    :param standard_chrom: whether to standardize chromosome names
    :param ensdb: a full path to the ensdb gtf file required for genomes not in GDdata
    :param just_db_commit: whether just to commit the JSON to the database
    :param no_db_commit: whether the JSON commit to the database should be skipped
    :param force_overwrite: whether to overwrite the existing record
    :param upload_s3: whether to upload to s3
    :param pm: pypiper object
    :return: None
    """

    pephub_registry_path = None
    if isinstance(pep, peppy.Project):
        pass
    elif isinstance(pep, str):
        if is_registry_path(pep):
            pephub_registry_path = pep
            pep = pephubclient.PEPHubClient().load_project(pep)
        else:
            pep = peppy.Project(pep)
    else:
        raise BedBossException("Incorrect pep type. Exiting...")

    bbc = bbconf.BedBaseConf(config_path=bedbase_config, database_only=True)

    validate_project(pep, BEDBOSS_PEP_SCHEMA_PATH)

    for i, pep_sample in enumerate(pep.samples):
        _LOGGER.info(f"Running bedboss pipeline for {pep_sample.sample_name}")

        if pep_sample.get("file_type").lower() == "narrowpeak":
            is_narrow_peak = True
        else:
            is_narrow_peak = False

        bed_id = run_all(
            sample_name=pep_sample.sample_name,
            input_file=pep_sample.input_file,
            input_type=pep_sample.input_type,
            genome=pep_sample.genome,
            narrowpeak=is_narrow_peak,
            chrom_sizes=pep_sample.get("chrom_sizes"),
            open_signal_matrix=pep_sample.get("open_signal_matrix"),
            description=pep_sample.get("description"),
            cell_type=pep_sample.get("cell_type"),
            treatment=pep_sample.get("treatment"),
            outfolder=output_folder,
            bedbase_config=bbc,
            rfg_config=rfg_config,
            check_qc=check_qc,
            standard_chrom=standard_chrom,
            ensdb=ensdb,
            just_db_commit=just_db_commit,
            no_db_commit=no_db_commit,
            force_overwrite=force_overwrite,
            skip_qdrant=skip_qdrant,
            pm=pm,
        )
        pep.samples[i].record_identifier = bed_id

    if upload_s3:
        load_to_s3(output_folder, pm)
    else:
        _LOGGER.info("Skipping uploading to s3. Flag `upload_s3` is set to False")

    if create_bedset:
        _LOGGER.info(f"Creating bedset from {pep.name}")
        run_bedbuncher(
            bedbase_config=bbc,
            bedset_pep=pep,
            pephub_registry_path=pephub_registry_path,
        )
    else:
        _LOGGER.info(
            f"Skipping bedset creation. Create_bedset is set to {create_bedset}"
        )


def load_to_s3(output_folder: str, pm: pypiper.PipelineManager) -> NoReturn:
    """
    Load bedfiles and statistics to s3

    :param output_folder: base output folder
    :param pm: pipelineManager object
    :return: NoReturn
    """
    command = f"aws s3 sync {os.path.join(output_folder, BED_FOLDER_NAME)} s3://bedbase/{BED_FOLDER_NAME} --size-only --exclude 'bed_qc/*'"
    _LOGGER.info("Uploading to s3 bed files")
    pm.run(cmd=command, lock_name="s3_sync_big")
    command = f"aws s3 sync {os.path.join(output_folder, BIGBED_FOLDER_NAME)} s3://bedbase/{BIGBED_FOLDER_NAME} --size-only"
    _LOGGER.info("Uploading to s3 bigbed files")
    pm.run(cmd=command, lock_name="s3_sync_bigbed")
    command = f"aws s3 sync {os.path.join(output_folder, OUTPUT_FOLDER_NAME)} s3://bedbase/{OUTPUT_FOLDER_NAME} --size-only"
    _LOGGER.info("Uploading to s3 bed statistics files")
    pm.run(cmd=command, lock_name="s3_sync_bedstat")


def main(test_args: dict = None) -> NoReturn:
    """
    Run pipeline that was specified in as positional argument.

    :param str test_args: one of the bedboss pipelines
    """
    parser = build_argparser()
    if test_args:
        args = Namespace(**test_args)
    else:
        args, _ = parser.parse_known_args()
        global _LOGGER
        _LOGGER = logmuse.logger_via_cli(args, make_root=True)

    args_dict = vars(args)

    pm_out_folder = (
        args_dict.get("outfolder")
        or args_dict.get("output_folder")
        or "test_outfolder",
    )
    pm_out_folder = os.path.join(os.path.abspath(pm_out_folder[0]), "pipeline_manager")

    pm = pypiper.PipelineManager(
        name="bedboss-pipeline",
        outfolder=pm_out_folder,
        version=__version__,
        args=args,
    )
    if args_dict["command"] == "all":
        run_all(pm=pm, **args_dict)
    elif args_dict["command"] == "insert":
        insert_pep(pm=pm, **args_dict)
    elif args_dict["command"] == "make":
        BedMaker(pm=pm, **args_dict)
    elif args_dict["command"] == "qc":
        bedqc(pm=pm, **args_dict)
    elif args_dict["command"] == "stat":
        bedstat(pm=pm, **args_dict)
    elif args_dict["command"] == "bunch":
        run_bedbuncher(pm=pm, **args_dict)
    elif args_dict["command"] == "index":
        add_to_qdrant(pm=pm, **args_dict)
    else:
        parser.print_help()
        # raise Exception("Incorrect pipeline name.")
    pm.stop_pipeline()
