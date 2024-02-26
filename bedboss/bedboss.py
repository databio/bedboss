import logging
import os
from typing import NoReturn, Union

import pypiper
from argparse import Namespace
import logmuse
import peppy
from eido import validate_project
import bbconf
import subprocess

import pephubclient
from pephubclient import PEPHubClient
from pephubclient.helpers import is_registry_path
from ubiquerg import parse_registry_path

from bedboss.bedstat.bedstat import bedstat
from bedboss.bedmaker.bedmaker import make_all
from bedboss.bedqc.bedqc import bedqc
from bedboss.bedbuncher import run_bedbuncher
from bedboss.qdrant_index import add_to_qdrant
from bedboss.cli import build_argparser
from bedboss.const import (
    BED_FOLDER_NAME,
    BIGBED_FOLDER_NAME,
    BEDBOSS_PEP_SCHEMA_PATH,
    OUTPUT_FOLDER_NAME,
    BEDSTAT_OUTPUT,
    BED_PEP_REGISTRY,
)
from bedboss.models import BedMetadata, BedStatCLIModel, BedMakerCLIModel, BedQCCLIModel
from bedboss.utils import (
    extract_file_name,
    standardize_genome_name,
    check_db_connection,
)
from bedboss.exceptions import BedBossException
from bedboss._version import __version__

_LOGGER = logging.getLogger("bedboss")


def load_to_pephub(
    pep_registry_path: str, bed_digest: str, genome: str, metadata: dict
) -> None:
    """
    Load bedfile and metadata to PEPHUB

    :param str pep_registry_path: registry path to pep on pephub
    :param str bed_digest: unique bedfile identifier
    :param str genome: genome associated with bedfile
    :param dict metadata: Any other metadata that has been collected

    :return None
    """

    if is_registry_path(pep_registry_path):
        parsed_pep_dict = parse_registry_path(pep_registry_path)

        # Combine data into a dict for sending to pephub
        sample_data = {}
        sample_data.update({"sample_name": bed_digest, "genome": genome})

        metadata = BedMetadata(**metadata).model_dump()

        for key, value in metadata.items():
            # TODO: Confirm this key is in the schema
            # Then update sample_data
            sample_data.update({key: value})

        try:
            PEPHubClient().sample.create(
                namespace=parsed_pep_dict["namespace"],
                name=parsed_pep_dict["item"],
                tag=parsed_pep_dict["tag"],
                sample_name=bed_digest,
                overwrite=True,
                sample_dict=sample_data,
            )

        except Exception as e:  # Need more specific exception
            _LOGGER.error(f"Failed to upload BEDFILE to PEPhub: See {e}")
    else:
        _LOGGER.error(f"{pep_registry_path} is not a valid registry path")


def load_to_s3(
    output_folder: str,
    pm: pypiper.PipelineManager,
    bed_file: str,
    digest: str,
    bigbed_file: str = None,
) -> None:
    """
    Load bedfiles and statistics to s3

    :param output_folder: base output folder
    :param pm: pipelineManager object
    :param bed_file: bedfile name
    :param digest: bedfile digest
    :param bigbed_file: bigbed file name
    :return: NoReturn
    """
    command = f"aws s3 cp {os.path.join(output_folder, bed_file)} s3://bedbase/{BED_FOLDER_NAME}"
    _LOGGER.info("Uploading to s3 bed file")
    pm.run(cmd=command, lock_name="s3_sync_bed")
    if bigbed_file:
        command = f"aws s3 cp {os.path.join(output_folder, bigbed_file)} s3://bedbase/{BIGBED_FOLDER_NAME}"
        _LOGGER.info("Uploading to s3 bigbed file")
        pm.run(cmd=command, lock_name="s3_sync_bigbed")
    command = f"aws s3 sync {os.path.join(output_folder, OUTPUT_FOLDER_NAME,BEDSTAT_OUTPUT, digest)} s3://bedbase/{OUTPUT_FOLDER_NAME}/{BEDSTAT_OUTPUT}/{digest} --size-only"
    _LOGGER.info("Uploading to s3 bed statistic files")
    pm.run(cmd=command, lock_name="s3_sync_bedstat")


def requirements_check() -> None:
    """
    Check if all requirements are installed

    :return: None
    """
    _LOGGER.info("Checking requirements...")
    subprocess.run(
        ["bash", f"{os.path.dirname(os.path.abspath(__file__))}/requirements_test.sh"]
    )


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
    standardize: bool = False,
    chrom_sizes: str = None,
    open_signal_matrix: str = None,
    ensdb: str = None,
    other_metadata: dict = None,
    just_db_commit: bool = False,
    db_commit: bool = True,
    force_overwrite: bool = False,
    upload_qdrant: bool = False,
    upload_s3: bool = False,
    upload_pephub: bool = False,
    pm: pypiper.PipelineManager = None,
    **kwargs,
) -> str:
    """
    Run bedboss: bedmaker, bedqc, bedstat, and bedbuncher pipelines from PEP.

    :param str sample_name: Sample name [required]
    :param str input_file: Input file [required]
    :param str input_type: Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
    :param str outfolder: Folder, where output should be saved  [required]
    :param str genome: genome_assembly of the sample. [required] options: (hg19, hg38, mm10) # TODO: add more
    :param Union[str, bbconf.BedBaseConf] bedbase_config: The path to the bedbase configuration file, or bbconf object.
    :param str rfg_config: file path to the genome config file [optional]
    :param bool narrowpeak: whether the regions are narrow
        (transcription factor implies narrow, histone mark implies broad peaks) [optional]
    :param bool check_qc: set True to run quality control during badmaking [optional] (default: True)
    :param bool standardize: Standardize bed file: filter the input file to contain only the standard chromosomes,
        and remove headers if necessary [optional] (default: False)
    :param str chrom_sizes: a full path to the chrom.sizes required for the bedtobigbed conversion [optional]
    :param str open_signal_matrix: a full path to the openSignalMatrix required for the tissue [optional]
    :param dict other_metadata: a dict containing all attributes from the sample
    :param str ensdb: a full path to the ensdb gtf file required for genomes not in GDdata [optional]
        (basically genomes that's not in GDdata)
    :param bool just_db_commit: whether just to commit the JSON to the database (default: False)
    :param bool force_overwrite: force overwrite analysis

    :param bool db_commit: whether the JSON commit to the database should be skipped (default: False)
    :param bool upload_qdrant: whether to skip qdrant indexing
    :param bool upload_s3: whether to upload to s3
    :param bool upload_pephub: whether to push bedfiles and metadata to pephub (default: False)
    :param pypiper.PipelineManager pm: pypiper object
    :return str bed_digest: bed digest
    """
    _LOGGER.warning(f"!Unused arguments: {kwargs}")

    if isinstance(bedbase_config, str):
        if not check_db_connection(bedbase_config=bedbase_config):
            raise BedBossException("Database connection failed. Exiting...")
        bbc = bbconf.BedBaseConf(config_path=bedbase_config, database_only=True)
    else:
        bbc = bedbase_config

    file_name = extract_file_name(input_file)
    genome = standardize_genome_name(genome)

    output_bed = os.path.join(outfolder, BED_FOLDER_NAME, f"{file_name}.bed.gz")
    output_bigbed = os.path.join(outfolder, BIGBED_FOLDER_NAME)

    # set env for bedstat:
    output_folder_bedstat = os.path.join(outfolder, "output")
    os.environ["BEDBOSS_OUTPUT_PATH"] = output_folder_bedstat

    _LOGGER.info(f"Input file = '{input_file}'")
    _LOGGER.info(f"Output bed file = '{output_bed}'")
    _LOGGER.info(f"Output bigbed file = '{output_bigbed}'")
    _LOGGER.info(f"Output folder for bedstat = '{output_folder_bedstat}'")

    if not pm:
        pm_out_folder = os.path.join(os.path.abspath(outfolder), "pipeline_manager")
        _LOGGER.info(f"Pipeline info folder = '{pm_out_folder}'")
        pm = pypiper.PipelineManager(
            name="bedboss-pipeline",
            outfolder=pm_out_folder,
            version=__version__,
            recover=True,
        )
        stop_pipeline = True
    else:
        stop_pipeline = False

    classification_meta = make_all(
        input_file=input_file,
        input_type=input_type,
        output_bed=output_bed,
        output_bigbed=output_bigbed,
        sample_name=sample_name,
        genome=genome,
        rfg_config=rfg_config,
        narrowpeak=narrowpeak,
        check_qc=check_qc,
        standardize=standardize,
        chrom_sizes=chrom_sizes,
        pm=pm,
    )
    if not other_metadata:
        other_metadata = {}

    bed_digest = classification_meta.get("digest")

    statistics_dict = bedstat(
        bedfile=output_bed,
        outfolder=outfolder,
        genome=genome,
        ensdb=ensdb,
        bed_digest=bed_digest,
        open_signal_matrix=open_signal_matrix,
        bigbed=output_bigbed,
        just_db_commit=just_db_commit,
        pm=pm,
    )
    statistics_dict.update(
        {
            "bed_type": classification_meta["bed_type"],
            "bed_format": classification_meta["bed_format"],
        }
    )

    if db_commit:
        bbc.bed.report(
            record_identifier=bed_digest,
            values=statistics_dict,
            force_overwrite=force_overwrite,
        )

    if upload_s3:
        _LOGGER.info(f"Uploading '{bed_digest}' data to S3 ...")
        load_to_s3(
            os.path.abspath(outfolder), pm, output_bed, bed_digest, output_bigbed
        )
    else:
        _LOGGER.info(
            f"Skipping uploading '{bed_digest}' data to S3. 'upload_s3' is set to False. "
        )

    if upload_qdrant:
        _LOGGER.info(f"Adding '{bed_digest}' vector to Qdrant ...")

        bbc.add_bed_to_qdrant(
            bed_id=bed_digest,
            bed_file=output_bed,
            payload={"digest": bed_digest},
        )
        bbc.bed.report(
            record_identifier=bed_digest,
            values={"added_to_qdrant": True},
            force_overwrite=True,
        )
    else:
        _LOGGER.info(
            f"Skipping adding '{bed_digest}' vector to Qdrant, 'skip_qdrant' is set to True. "
        )

    if upload_pephub:
        _LOGGER.info(f"Uploading metadata of '{bed_digest}' TO PEPhub ...")
        load_to_pephub(
            pep_registry_path=BED_PEP_REGISTRY,
            bed_digest=bed_digest,
            genome=genome,
            metadata=other_metadata,
        )
    else:
        _LOGGER.info(
            f"Metadata of '{bed_digest}' is NOT uploaded to PEPhub. 'upload_pephub' is set to False. "
        )

    if stop_pipeline:
        pm.stop_pipeline()

    return bed_digest


def insert_pep(
    bedbase_config: str,
    output_folder: str,
    pep: Union[str, peppy.Project],
    rfg_config: str = None,
    create_bedset: bool = True,
    check_qc: bool = True,
    standardize: bool = False,
    ensdb: str = None,
    just_db_commit: bool = False,
    no_db_commit: bool = False,
    force_overwrite: bool = False,
    upload_s3: bool = False,
    upload_pephub: bool = False,
    upload_qdrant: bool = False,
    pm: pypiper.PipelineManager = None,
    *args,
    **kwargs,
) -> None:
    """
    Run all bedboss pipelines for all samples in the pep file.
    bedmaker -> bedqc -> bedstat -> qdrant_indexing -> bedbuncher

    :param str bedbase_config: bedbase configuration file path
    :param str output_folder: output statistics folder
    :param Union[str, peppy.Project] pep: path to the pep file or pephub registry path
    :param str rfg_config: path to the genome config file (refgenie)
    :param bool create_bedset: whether to create bedset
    :param bool upload_qdrant: whether to upload bedfiles to qdrant
    :param bool check_qc: whether to run quality control during badmaking
    :param bool standardize: "Standardize bed files: remove non-standard chromosomes and headers if necessary Default: False"
    :param str ensdb: a full path to the ensdb gtf file required for genomes not in GDdata
    :param bool just_db_commit: whether save only to the database (Without saving locally )
    :param bool db_commit: whether to upload data to the database
    :param bool force_overwrite: whether to overwrite the existing record
    :param bool upload_s3: whether to upload to s3
    :param bool upload_pephub: whether to push bedfiles and metadata to pephub (default: False)
    :param bool upload_qdrant: whether to execute qdrant indexing
    :param pypiper.PipelineManager pm: pypiper object
    :return: None
    """

    _LOGGER.warning(f"!Unused arguments: {kwargs}")
    failed_samples = []
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
        if pep_sample.get("file_type"):
            if pep_sample.get("file_type").lower() == "narrowpeak":
                is_narrow_peak = True
            else:
                is_narrow_peak = False
        else:
            is_narrow_peak = False
        try:
            bed_id = run_all(
                sample_name=pep_sample.sample_name,
                input_file=pep_sample.input_file,
                input_type=pep_sample.input_type,
                genome=pep_sample.genome,
                narrowpeak=is_narrow_peak,
                chrom_sizes=pep_sample.get("chrom_sizes"),
                open_signal_matrix=pep_sample.get("open_signal_matrix"),
                other_metadata=pep_sample.to_dict(),
                outfolder=output_folder,
                bedbase_config=bbc,
                rfg_config=rfg_config,
                check_qc=check_qc,
                standardize=standardize,
                ensdb=ensdb,
                just_db_commit=just_db_commit,
                no_db_commit=no_db_commit,
                force_overwrite=force_overwrite,
                upload_qdrant=upload_qdrant,
                upload_s3=upload_s3,
                upload_pephub=upload_pephub,
                pm=pm,
            )
            pep.samples[i].record_identifier = bed_id
        except BedBossException as e:
            _LOGGER.error(f"Failed to process {pep_sample.sample_name}. See {e}")
            failed_samples.append(pep_sample.sample_name)

    else:
        _LOGGER.info("Skipping uploading to s3. Flag `upload_s3` is set to False")

    if create_bedset:
        _LOGGER.info(f"Creating bedset from {pep.name}")
        run_bedbuncher(
            bedbase_config=bbc,
            bedset_pep=pep,
            pephub_registry_path=pephub_registry_path,
            upload_pephub=upload_pephub,
        )
    else:
        _LOGGER.info(
            f"Skipping bedset creation. Create_bedset is set to {create_bedset}"
        )
    _LOGGER.info(f"Failed samples: {failed_samples}")


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
        # args=args,
        multi=args_dict.get("multy", False),
        recover=True,
    )
    if args_dict["command"] == "all":
        run_all(pm=pm, **args_dict)
    elif args_dict["command"] == "insert":
        insert_pep(pm=pm, **args_dict)
    elif args_dict["command"] == "make":
        make_all(**BedMakerCLIModel(pm=pm, **args_dict).model_dump())
    elif args_dict["command"] == "qc":
        bedqc(**BedQCCLIModel(pm=pm, **args_dict).model_dump())
    elif args_dict["command"] == "stat":
        bedstat(**BedStatCLIModel(pm=pm, **args_dict).model_dump())
    elif args_dict["command"] == "bunch":
        run_bedbuncher(pm=pm, **args_dict)
    elif args_dict["command"] == "index":
        add_to_qdrant(pm=pm, **args_dict)
    elif args_dict["command"] == "requirements-check":
        requirements_check()
    else:
        parser.print_help()
        # raise Exception("Incorrect pipeline name.")
    pm.stop_pipeline()
