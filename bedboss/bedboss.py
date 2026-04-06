import datetime
import logging
import os
import subprocess
from importlib.metadata import version as _pkg_version
from urllib.parse import urlparse

import bbconf
import peprs
import pypiper
import yaml
from bbconf.bbagent import BedBaseAgent
from bbconf.const import DEFAULT_LICENSE
from bbconf.models.base_models import FileModel
from geniml.bbclient import BBClient
from pephubclient import PEPHubClient
from pephubclient.helpers import MessageHandler as m
from pephubclient.helpers import is_registry_path
from peprs.eido import validate_project

__version__ = _pkg_version("bedboss")
from bedboss.bedbuncher import run_bedbuncher
from bedboss.bedmaker.bedmaker import make_all
from bedboss.bedstat.bedstat import bedstat
from bedboss.bedstat.r_service import RServiceManager
from bedboss.const import MAX_FILE_SIZE_QC, PKG_NAME
from bedboss.exceptions import BedBossException, QualityException
from bedboss.models import (
    BedClassificationUpload,
    BedSetAnnotations,
    FilesUpload,
    PlotsUpload,
    StatsUpload,
)
from bedboss.refgenome_validator.main import ReferenceValidator
from bedboss.refgenome_validator.utils import predict_from_compatibility_resutlts
from bedboss.skipper import Skipper
from bedboss.utils import (
    calculate_time,
    get_genome_digest,
    run_initial_qc,
    standardize_genome_name,
)
from bedboss.utils import standardize_pep as pep_standardizer

_LOGGER = logging.getLogger(PKG_NAME)


def requirements_check() -> None:
    """Check if all requirements are installed."""
    _LOGGER.info("Checking requirements...")
    subprocess.run(
        [
            "bash",
            f"{os.path.dirname(os.path.abspath(__file__))}/scripts/requirements_test.sh",
        ]
    )


@calculate_time
def run_all(
    input_file: str,
    input_type: str,
    outfolder: str,
    genome: str,
    bedbase_config: str | bbconf.BedBaseAgent,
    name: str = None,
    license_id: str = DEFAULT_LICENSE,
    rfg_config: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    validate_reference: bool = True,
    chrom_sizes: str = None,
    open_signal_matrix: str = None,
    ensdb: str = None,
    other_metadata: dict = None,
    just_db_commit: bool = False,
    force_overwrite: bool = False,
    update: bool = False,
    upload_qdrant: bool = False,
    upload_s3: bool = False,
    upload_pephub: bool = False,
    lite: bool = False,
    # Universes
    universe: bool = False,
    universe_method: str = None,
    universe_bedset: str = None,
    pm: pypiper.PipelineManager = None,
    r_service: RServiceManager = None,
    reference_genome_validator: ReferenceValidator = None,
) -> str:
    """
    Run bedboss: bedmaker -> bedqc -> bedclassifier -> bedstat -> upload to s3, qdrant, pephub, and bedbase.

    Args:
        input_file: Input file path.
        input_type: Input type, one of (bigwig|bedgraph|bed|bigbed|wig).
        outfolder: Folder where output should be saved.
        genome: Genome assembly of the sample (e.g. hg19, hg38, mm10).
        bedbase_config: The path to the bedbase configuration file, or bbconf object.
        name: Name of the sample (human-readable name, e.g. "H3K27ac in liver").
        license_id: License identifier. Default: "DUO:0000042".
        rfg_config: File path to the genome config file.
        narrowpeak: Whether the regions are narrow. Used to create bed file from bedgraph or bigwig.
        check_qc: Set True to run quality control during bedmaking. Default: True.
        validate_reference: Set True to run genome reference validator.
        chrom_sizes: A full path to the chrom.sizes required for the bedtobigbed conversion.
        open_signal_matrix: A full path to the openSignalMatrix required for the tissue.
        ensdb: A full path to the ensdb gtf file required for genomes not in GDdata.
        other_metadata: A dict containing all attributes from the sample.
        just_db_commit: Whether just to commit the JSON to the database. Default: False.
        force_overwrite: Force overwrite analysis. Default: False.
        update: Whether to update the record in the database. If True, overwrites 'force_overwrite'. Default: False.
        upload_qdrant: Whether to upload to qdrant. Default: False.
        upload_s3: Whether to upload to s3.
        upload_pephub: Whether to push bedfiles and metadata to pephub. Default: False.
        lite: Whether to run lite version of the pipeline. Default: False.
        universe: Whether to add the sample as the universe. Default: False.
        universe_method: Method used to create the universe.
        universe_bedset: Bedset identifier for the universe.
        pm: Pypiper object.
        r_service: RServiceManager object that will run R services.
        reference_genome_validator: ReferenceValidator object that will validate reference genome compatibility.

    Returns:
        Bed digest string.
    """
    if isinstance(bedbase_config, str):
        bbagent = BedBaseAgent(config=bedbase_config, init_ml=not lite)
    elif isinstance(bedbase_config, bbconf.BedBaseAgent):
        bbagent = bedbase_config
    else:
        raise BedBossException("Incorrect bedbase_config type. Exiting...")

    genome = standardize_genome_name(genome)

    _LOGGER.info(f"Input file = '{input_file}'")
    _LOGGER.info(f"Output folder = '{outfolder}'")
    _LOGGER.info(f"Sample genome = '{genome}'")

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

    if urlparse(input_file).scheme in ("http", "https", "ftp"):
        _LOGGER.info(
            "Remote input detected. Running initial_qc to inspect remote file size."
        )
        file_size = run_initial_qc(input_file)

        if file_size > MAX_FILE_SIZE_QC:
            raise QualityException(
                f"Remote file size {file_size} exceeds the maximum allowed size of {bbconf.MAX_REMOTE_FILE_SIZE}. Please download the file and provide a local path."
            )
        else:
            _LOGGER.info(
                f"Remote file size {file_size} is within the allowed limit. Proceeding with processing."
            )

    bed_metadata = make_all(
        input_file=input_file,
        input_type=input_type,
        output_path=outfolder,
        genome=genome,
        rfg_config=rfg_config,
        narrowpeak=narrowpeak,
        check_qc=check_qc,
        chrom_sizes=chrom_sizes,
        lite=lite,
        pm=pm,
    )
    if not other_metadata:
        other_metadata = {"sample_name": name}

    if not update:
        other_metadata["original_file_name"] = os.path.basename(input_file)

    if lite:
        statistics_dict = {}
        statistics_dict["number_of_regions"] = len(bed_metadata.bed_object)
    else:
        statistics_dict = bedstat(
            bedfile=bed_metadata.bed_file,
            outfolder=outfolder,
            genome=genome,
            ensdb=ensdb,
            bed_digest=bed_metadata.bed_digest,
            open_signal_matrix=open_signal_matrix,
            just_db_commit=just_db_commit,
            rfg_config=rfg_config,
            pm=pm,
            r_service=r_service,
        )

    if "mean_region_width" not in statistics_dict:
        statistics_dict["mean_region_width"] = (
            bed_metadata.bed_object.mean_region_width()
        )

    statistics_dict["bed_compliance"] = bed_metadata.bed_compliance
    statistics_dict["data_format"] = bed_metadata.data_format.value

    if bed_metadata.bigbed_file:
        genome_digest = get_genome_digest(genome)
    else:
        genome_digest = None

    stats = StatsUpload(**statistics_dict)
    plots = PlotsUpload(**statistics_dict)

    if bed_metadata.bigbed_file:
        big_bed = FileModel(
            name="bigbedfile",
            title="BigBed file",
            path=bed_metadata.bigbed_file,
            description="Path to the bigbed file",
            thumbnail_path=None,
            file_digest=None,
        )
    else:
        big_bed = None
    files = FilesUpload(
        bed_file=FileModel(
            name="bedfile",
            title="BED file",
            path=bed_metadata.bed_file,
            description="Path to the BED file",
            thumbnail_path=None,
            file_digest=bed_metadata.bed_object.file_digest,
        ),
        bigbed_file=big_bed,
    )

    classification = BedClassificationUpload(
        name=name or bed_metadata.bed_digest,
        genome_digest=genome_digest,
        genome_alias=genome,
        bed_compliance=bed_metadata.bed_compliance,
        data_format=bed_metadata.data_format.value,
        compliant_columns=bed_metadata.compliant_columns,
        non_compliant_columns=bed_metadata.non_compliant_columns,
        header=bed_metadata.bed_object.header,
    )
    if not reference_genome_validator:
        reference_genome_validator = ReferenceValidator()
    if validate_reference:
        _LOGGER.info("Validating reference genome")
        ref_valid_stats = reference_genome_validator.determine_compatibility(
            bedfile=bed_metadata.bed_object, concise=True
        )
        predicted_alias, predicted_digest = predict_from_compatibility_resutlts(
            ref_valid_stats
        )
        if predicted_alias and predicted_digest:
            _LOGGER.info(
                f"Predicted genome: {predicted_alias} (digest: {predicted_digest})"
            )
            classification.genome_alias = predicted_alias
            classification.genome_digest = predicted_digest
    else:
        ref_valid_stats = None

    if update:
        bbagent.bed.update(
            identifier=bed_metadata.bed_digest,
            stats=stats.model_dump(exclude_unset=True),
            metadata=other_metadata,
            plots=plots.model_dump(exclude_unset=True),
            files=files.model_dump(exclude_unset=True),
            classification=classification.model_dump(exclude_unset=True),
            ref_validation=ref_valid_stats,
            license_id=license_id,
            upload_qdrant=upload_qdrant and not lite,
            upload_pephub=upload_pephub,
            upload_s3=upload_s3,
            local_path=outfolder,
            overwrite=True,
            processed=not lite,
            nofail=True,
        )
    else:
        bbagent.bed.add(
            identifier=bed_metadata.bed_digest,
            stats=stats.model_dump(exclude_unset=True),
            metadata=other_metadata,
            plots=plots.model_dump(exclude_unset=True),
            files=files.model_dump(exclude_unset=True),
            classification=classification.model_dump(exclude_unset=True),
            ref_validation=ref_valid_stats,
            license_id=license_id,
            upload_qdrant=upload_qdrant and not lite,
            upload_pephub=upload_pephub,
            upload_s3=upload_s3,
            local_path=outfolder,
            overwrite=force_overwrite,
            processed=not lite,
            nofail=True,
        )

    if universe:
        bbagent.bed.add_universe(
            bedfile_id=bed_metadata.bed_digest,
            bedset_id=universe_bedset,
            construct_method=universe_method,
        )

    if stop_pipeline:
        pm.stop_pipeline()

    _LOGGER.info(f"All done! Bed digest: {bed_metadata.bed_digest}")
    return bed_metadata.bed_digest


@calculate_time
def insert_pep(
    bedbase_config: str,
    output_folder: str,
    pep: str | peprs.Project,
    bedset_id: str = None,
    bedset_name: str = None,
    rfg_config: str = None,
    license_id: str = DEFAULT_LICENSE,
    create_bedset: bool = False,
    bedset_heavy: bool = False,
    check_qc: bool = True,
    ensdb: str = None,
    just_db_commit: bool = False,
    force_overwrite: bool = False,
    update: bool = False,
    upload_s3: bool = False,
    upload_pephub: bool = False,
    upload_qdrant: bool = False,
    no_fail: bool = False,
    standardize_pep: bool = False,
    lite: bool = False,
    rerun: bool = False,
    pm: pypiper.PipelineManager = None,
) -> None:
    """
    Run all bedboss pipelines for all samples in the pep file.

    Pipeline order: bedmaker -> bedqc -> bedstat -> qdrant_indexing -> bedbuncher.

    Args:
        bedbase_config: Bedbase configuration file path.
        output_folder: Output statistics folder.
        pep: Path to the pep file or pephub registry path.
        bedset_id: Bedset identifier.
        bedset_name: Bedset name.
        rfg_config: Path to the genome config file (refgenie).
        license_id: License identifier. Default: "DUO:0000042". Used for bedfiles where license is not provided in PEP file.
        create_bedset: Whether to create bedset.
        bedset_heavy: Whether to use heavy processing (add all columns to the database).
        check_qc: Whether to run quality control during bedmaking.
        ensdb: A full path to the ensdb gtf file required for genomes not in GDdata.
        just_db_commit: Whether save only to the database (without saving locally).
        force_overwrite: Whether to overwrite the existing record.
        update: Whether to update the record in the database. Overwrites force_overwrite. Default: False.
        upload_s3: Whether to upload to s3.
        upload_pephub: Whether to push bedfiles and metadata to pephub. Default: False.
        upload_qdrant: Whether to execute qdrant indexing.
        no_fail: Whether to raise an error if bedset was not added to the database.
        standardize_pep: Whether to standardize the pep file before processing by using bedms. Default: False.
        lite: Whether to run lite version of the pipeline.
        rerun: Whether to rerun processed samples.
        pm: Pypiper object.
    """

    failed_samples = []
    processed_ids = []
    pep: peprs.Project = pep_any_to_object(pep)

    if standardize_pep:
        pep = pep_standardizer(pep)

    if not pm:
        pm_out_folder = os.path.join(os.path.abspath(output_folder), "pipeline_manager")
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

    bbagent = BedBaseAgent(bedbase_config)

    phc = PEPHubClient()
    validate_project(pep, phc.schema.get("databio", "pipelines-bedboss"))

    bedset_annotation = BedSetAnnotations(**(pep.config or {})).model_dump()
    skipper = Skipper(output_folder, pep.name or "")

    if rerun:
        skipper.reinitialize()

    if not lite:
        r_service = RServiceManager()
    else:
        r_service = None

    reference_genome_validator = ReferenceValidator()

    for i, pep_sample in enumerate(pep.samples):
        is_processed = skipper.is_processed(pep_sample.sample_name)
        if is_processed:
            m.print_success(
                f"Skipping {pep_sample.sample_name} : {is_processed}. Already processed."
            )
            processed_ids.append(is_processed)
            continue

        m.print_success(f"Processing sample {i + 1}/{len(pep.samples)}")
        _LOGGER.info(f"Running bedboss pipeline for {pep_sample.sample_name}")
        if pep_sample.get("file_type"):
            if pep_sample.get("file_type").lower() == "narrowpeak":
                is_narrow_peak = True
            else:
                is_narrow_peak = False
        else:
            is_narrow_peak = False
        try:
            if pep_sample.get("file_size", None):
                if int(pep_sample.file_size) > MAX_FILE_SIZE_QC:
                    raise QualityException(
                        f"File size {pep_sample.file_size} exceeds the maximum allowed size of {MAX_FILE_SIZE_QC}. Please provide a smaller file or set check_qc to False to skip this check."
                    )

            bed_id = run_all(
                input_file=pep_sample.input_file,
                input_type=pep_sample.input_type,
                genome=pep_sample.genome,
                name=pep_sample.sample_name,
                bedbase_config=bbagent,
                license_id=pep_sample.get("license_id") or license_id,
                narrowpeak=is_narrow_peak,
                chrom_sizes=pep_sample.get("chrom_sizes"),
                open_signal_matrix=pep_sample.get("open_signal_matrix"),
                other_metadata=pep_sample.to_dict(),
                outfolder=output_folder,
                rfg_config=rfg_config,
                check_qc=check_qc,
                ensdb=ensdb,
                just_db_commit=just_db_commit,
                force_overwrite=force_overwrite,
                update=update,
                upload_qdrant=upload_qdrant,
                upload_s3=upload_s3,
                upload_pephub=upload_pephub,
                universe=pep_sample.get("universe"),
                universe_method=pep_sample.get("universe_method"),
                universe_bedset=pep_sample.get("universe_bedset"),
                lite=lite,
                pm=pm,
                r_service=r_service,
                reference_genome_validator=reference_genome_validator,
            )

            processed_ids.append(bed_id)
            skipper.add_processed(pep_sample.sample_name, bed_id, success=True)

        except BedBossException as e:
            _LOGGER.error(f"Failed to process {pep_sample.sample_name}. See {e}")
            failed_samples.append(pep_sample.sample_name)
            skipper.add_failed(pep_sample.sample_name, f"{e}")

    if create_bedset:
        _LOGGER.info(f"Creating bedset from {pep.name}")
        run_bedbuncher(
            bedbase_config=bbagent,
            record_id=bedset_id or pep.name,
            bed_set=processed_ids,
            name=bedset_name or pep.name,
            output_folder=output_folder,
            description=pep.description,
            heavy=bedset_heavy,
            upload_pephub=upload_pephub,
            upload_s3=upload_s3,
            no_fail=no_fail,
            force_overwrite=force_overwrite,
            annotation=bedset_annotation,
            lite=lite,
        )
    else:
        _LOGGER.info(
            f"Skipping bedset creation. Create_bedset is set to {create_bedset}"
        )
    m.print_success(f"Processed samples: {processed_ids}")
    m.print_error(f"Failed samples: {failed_samples}")

    m.print_success(f"Processed samples: {processed_ids}")
    m.print_error(f"Failed samples: {failed_samples}")
    if stop_pipeline:
        pm.stop_pipeline()

    m.print_success(f"Processing of '{pep.name}' completed successfully")
    return None


def pep_any_to_object(pep: str | peprs.Project) -> peprs.Project:
    """
    Convert String and pep object to pep object.

    If the input is a string, it can be either a path to a local pep file or a pephub registry path.

    Args:
        pep: String path or pephub registry path, or peprs.Project object.

    Returns:
        peprs.Project object.
    """

    if isinstance(pep, peprs.Project):
        return pep
    elif isinstance(pep, str):
        if is_registry_path(pep):
            return peprs.Project.from_pephub(pep)
        else:
            return peprs.Project(pep)
    else:
        raise BedBossException("Incorrect pep type. Exiting...")


@calculate_time
def reprocess_all(
    bedbase_config: str | BedBaseAgent,
    output_folder: str,
    limit: int = 10,
    no_fail: bool = False,
    pm: pypiper.PipelineManager = None,
) -> None:
    """
    Run bedboss pipeline for all unprocessed beds in the bedbase.

    Currently only beds with genomes hg19, hg38, and mm10 are processed.

    Args:
        bedbase_config: Bedbase configuration file path.
        output_folder: Output folder of the pipeline.
        limit: Limit of the number of beds to process.
        no_fail: Whether to raise an error if bedset was not added to the database.
        pm: Pypiper object.
    """

    if not pm:
        pm_out_folder = os.path.join(os.path.abspath(output_folder), "pipeline_manager")
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

    r_service = RServiceManager()

    if isinstance(bedbase_config, str):
        bbagent = BedBaseAgent(config=bedbase_config)
    elif isinstance(bedbase_config, bbconf.BedBaseAgent):
        bbagent = bedbase_config
    else:
        raise BedBossException("Incorrect bedbase_config type. Exiting...")

    unprocessed_beds = bbagent.bed.get_unprocessed(
        limit=limit, genome=["hg38", "hg19", "mm10"]
    )

    bbclient = BBClient()
    failed_samples = []
    for i, bed_annot in enumerate(unprocessed_beds.results):
        bed_file = bbclient.load_bed(bed_annot.id)

        m.print_success(
            f"\n#### Processing sample: {i + 1} / {len(unprocessed_beds.results)} ####"
        )
        try:
            run_all(
                input_file=bed_file.path,
                input_type="bed",
                outfolder=output_folder,
                genome=bed_annot.genome_alias,
                bedbase_config=bbagent,
                name=bed_annot.name,
                license_id=bed_annot.license_id,
                rfg_config=None,
                check_qc=False,
                validate_reference=True,
                chrom_sizes=None,
                open_signal_matrix=None,
                ensdb=None,
                other_metadata=None,
                just_db_commit=False,
                update=True,
                upload_qdrant=True,
                upload_s3=True,
                upload_pephub=True,
                lite=False,
                universe=False,
                universe_method=None,
                universe_bedset=None,
                pm=pm,
                r_service=r_service,
            )
        except Exception as e:
            _LOGGER.error(f"Failed to process {bed_annot.name}. See {e}")
            if no_fail:
                raise BedBossException(f"Failed to process {bed_annot.name}. See {e}")

            failed_samples.append(
                {
                    "id": bed_annot.id,
                    "error": e,
                }
            )

    if failed_samples:
        date_now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        with open(
            os.path.join(output_folder, f"failed_samples_{date_now}.yaml"), "w"
        ) as file:
            yaml.dump(failed_samples, file)

            m.print_warning(f"Logs with failed samples are saved in {output_folder}")

    m.print_success("Processing completed successfully")

    print_values = dict(
        unprocessed_files=unprocessed_beds.count,
        processed_files=unprocessed_beds.limit,
        failed_files=len(failed_samples),
        success_files=unprocessed_beds.limit - len(failed_samples),
    )
    print_values["unprocessed_files_left"] = (
        print_values["unprocessed_files"] - print_values["processed_files"]
    )
    m.print_success(str(print_values))
    if stop_pipeline:
        pm.stop_pipeline()


@calculate_time
def reprocess_one(
    bedbase_config: str | BedBaseAgent,
    output_folder: str,
    identifier: str,
    pm: pypiper.PipelineManager = None,
) -> None:
    """
    Run bedboss pipeline for one bed in the bedbase (Reprocess).

    Args:
        bedbase_config: Bedbase configuration file path.
        output_folder: Output folder of the pipeline.
        identifier: Bed identifier.
        pm: Pypiper object.
    """

    if pm is None:
        pm_out_folder = os.path.join(os.path.abspath(output_folder), "pipeline_manager")
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

    if isinstance(bedbase_config, str):
        bbagent = BedBaseAgent(config=bedbase_config)
    elif isinstance(bedbase_config, bbconf.BedBaseAgent):
        bbagent = bedbase_config
    else:
        raise BedBossException("Incorrect bedbase_config type. Exiting...")

    bbclient = BBClient()

    bed_annot = bbagent.bed.get(identifier)
    bed_file = bbclient.load_bed(bed_annot.id)

    run_all(
        input_file=bed_file.path,
        input_type="bed",
        outfolder=output_folder,
        genome=bed_annot.genome_alias,
        bedbase_config=bbagent,
        name=bed_annot.name,
        license_id=bed_annot.license_id,
        rfg_config=None,
        check_qc=False,
        validate_reference=True,
        chrom_sizes=None,
        open_signal_matrix=None,
        ensdb=None,
        other_metadata=None,
        just_db_commit=False,
        update=True,
        upload_qdrant=True,
        upload_s3=True,
        upload_pephub=True,
        lite=False,
        universe=False,
        universe_method=None,
        universe_bedset=None,
        pm=None,
    )

    _LOGGER.info(f"Successfully processed {identifier}")

    if stop_pipeline:
        pm.stop_pipeline()


@calculate_time
def reprocess_bedset(
    bedbase_config: str | BedBaseAgent,
    output_folder: str,
    identifier: str,
    no_fail: bool = True,
    heavy: bool = False,
):
    """
    Recalculate bedset from the bedbase.

    Args:
        bedbase_config: Bedbase configuration file path.
        output_folder: Output folder of the pipeline.
        identifier: Bedset identifier.
        no_fail: Whether to raise an error if bedset was not added to the database.
        heavy: Whether to use heavy processing. Calculate plots for bedset.
    """

    if isinstance(bedbase_config, str):
        bbagent = BedBaseAgent(config=bedbase_config)
    elif isinstance(bedbase_config, bbconf.BedBaseAgent):
        bbagent = bedbase_config
    else:
        raise BedBossException("Incorrect bedbase_config type. Exiting...")

    bedset_annot = bbagent.bedset.get(identifier)

    run_bedbuncher(
        bedbase_config=bbagent,
        record_id=bedset_annot.id,
        bed_set=bedset_annot.bed_ids,
        name=bedset_annot.name,
        output_folder=output_folder,
        description=bedset_annot.description,
        heavy=heavy,
        upload_pephub=False,
        upload_s3=heavy,
        no_fail=no_fail,
        force_overwrite=True,
        annotation={
            **bedset_annot.model_dump(
                exclude={
                    "bed_ids",
                }
            )
        },
        lite=False,
    )
