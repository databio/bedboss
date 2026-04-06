import logging
import os
from importlib.metadata import version as _pkg_version
from typing import Literal

import peprs
import pypiper
from bbconf import BedBaseAgent
from bbconf.db_utils import GeoGseStatus, GeoGsmStatus
from geniml.exceptions import GenimlBaseError
from pephubclient import PEPHubClient
from pephubclient.helpers import MessageHandler
from pephubclient.models import SearchReturnModel
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from bedboss.bbuploader.constants import (
    DEFAULT_GEO_TAG,
    FILE_FOLDER_NAME,
    PKG_NAME,
    STATUS,
)
from bedboss.bbuploader.metadata_extractor import find_assay, find_cell_line
from bedboss.bbuploader.models import (
    BedBossMetadata,
    BedBossMetadataSeries,
    BedBossRequired,
    ProjectProcessingStatus,
)
from bedboss.bbuploader.utils import (
    build_gse_identifier,
    create_gsm_sub_name,
    middle_underscored,
)
from bedboss.bedboss import run_all
from bedboss.bedbuncher.bedbuncher import run_bedbuncher
from bedboss.bedstat.r_service import RServiceManager
from bedboss.const import MAX_FILE_SIZE
from bedboss.exceptions import BedBossException, QualityException
from bedboss.refgenome_validator.main import ReferenceValidator
from bedboss.skipper import Skipper
from bedboss.utils import (
    calculate_time,
    download_file,
    run_initial_qc,
    standardize_genome_name,
)
from bedboss.utils import standardize_pep as pep_standardizer

__version__ = _pkg_version("bedboss")

_LOGGER = logging.getLogger(PKG_NAME)
_LOGGER.setLevel(logging.DEBUG)


reference_validator = ReferenceValidator()


@calculate_time
def upload_all(
    bedbase_config: str,
    outfolder: str = os.getcwd(),
    geo_tag: str = DEFAULT_GEO_TAG,
    start_date: str = None,
    end_date: str = None,
    search_limit: int = 10,
    search_offset: int = 0,
    download_limit: int = 100,
    genome: str = None,
    create_bedset: bool = True,
    preload=True,
    rerun: bool = False,
    run_skipped: bool = False,
    run_failed: bool = True,
    standardize_pep: bool = False,
    use_skipper=True,
    reinit_skipper=False,
    overwrite=False,
    overwrite_bedset=False,
    lite=False,
):
    """
    Main function responsible for processing bed files from PEPHub.

    Args:
        outfolder: Working directory where files will be downloaded, processed and statistics saved.
        bedbase_config: Path to bedbase configuration file.
        geo_tag: GEO tag to use when loading projects from PEPHub ('samples' or 'series').
        start_date: The earliest date when pep was updated. Default: 2000/01/01.
        end_date: The latest date when pep was updated. Default: today's date.
        search_limit: Limit of projects to be searched.
        search_offset: Offset of projects to be searched.
        download_limit: Limit of GSE projects to be downloaded (used for testing). Default: 100.
        genome: Reference genome (e.g. hg38). If None, all genomes will be processed.
        create_bedset: Create bedset from bed files.
        preload: Pre-download files to the local folder (used for faster reproducibility).
        rerun: Rerun processing of the series. If you want to reupload file use overwrite.
        run_skipped: Rerun files that were skipped. If you want to reupload file use overwrite.
        run_failed: Rerun failed files. If you want to reupload file use overwrite.
        standardize_pep: Standardize pep metadata using BEDMS.
        use_skipper: Use skipper to skip already processed files logged locally.
        reinit_skipper: If True, skipper will be reinitialized and all log files cleaned.
        lite: Lite mode, skipping statistic processing for memory optimization and time saving.
    """

    phc = PEPHubClient()
    os.makedirs(outfolder, exist_ok=True)

    _LOGGER.info(f"Initializing BedBaseAgent with config: '{bedbase_config}'")
    bbagent = BedBaseAgent(config=bedbase_config, init_ml=not lite)
    _LOGGER.info(f"BedBaseAgent initialized (ML enabled: {not lite})")

    genome = standardize_genome_name(genome, reference_validator=reference_validator)
    if genome:
        _LOGGER.info(f"Filtering for genome: '{genome}'")

    pep_annotation_list = find_peps(
        geo_tag=geo_tag,
        start_date=start_date,
        end_date=end_date,
        limit=search_limit,
        offset=search_offset,
        phc=phc,
    )

    _LOGGER.info(
        f"parameters: start_date={start_date}, end_date={end_date}, search_limit={search_limit}, search_offset={search_offset}"
    )
    _LOGGER.info(f"found {pep_annotation_list.count} projects")

    count = 0
    total_projects = len(pep_annotation_list.results)

    pm_out_folder = os.path.join(os.path.abspath(outfolder), "pipeline_manager")
    _LOGGER.info(f"Pipeline info folder = '{pm_out_folder}'")
    pm = pypiper.PipelineManager(
        name="bedboss-pipeline",
        outfolder=pm_out_folder,
        version=__version__,
        recover=True,
    )

    if not lite:
        _LOGGER.info("Initializing R service for statistics")
        r_service = RServiceManager()
    else:
        _LOGGER.info("Lite mode: R service disabled")
        r_service = None

    for gse_pep in pep_annotation_list.results:
        count += 1
        gse_id = build_gse_identifier(gse_pep.name, geo_tag)
        with Session(bbagent.config.db_engine.engine) as session:
            MessageHandler.print_success(f"{'##' * 30}")
            MessageHandler.print_success(
                f"#### Processing: '{gse_id}'. #### Processing {count} / {total_projects}. ####"
            )

            gse_status = session.scalar(
                select(GeoGseStatus).where(GeoGseStatus.gse == gse_id)
            )

            if gse_status:
                if gse_status.status == STATUS.SUCCESS and not rerun:
                    _LOGGER.info(
                        f"Skipping: '{gse_id}' - already processed and rerun set to false"
                    )
                    continue

                elif (
                    gse_status.status == STATUS.FAIL
                    or gse_status.status == STATUS.PROCESSING
                ) and not run_failed:
                    _LOGGER.info("Reprocessing of failed set to false, exiting.")
                    continue

                elif (
                    gse_status.status == STATUS.SKIPPED
                    or gse_status.status == STATUS.PARTIAL
                ) and not run_skipped:
                    _LOGGER.info(
                        f"Run skipped files set to false, exiting. GSE: {gse_id}"
                    )
                    continue

            else:
                gse_status = GeoGseStatus(gse=gse_id, status=STATUS.PROCESSING)
                session.add(gse_status)
                session.commit()

            try:
                upload_result = _upload_gse(
                    gse=gse_pep.name,
                    bedbase_config=bbagent,
                    outfolder=outfolder,
                    geo_tag=geo_tag,
                    create_bedset=create_bedset,
                    genome=genome,
                    sa_session=session,
                    gse_status_sa_model=gse_status,
                    standardize_pep=standardize_pep,
                    rerun=rerun,
                    use_skipper=use_skipper,
                    reinit_skipper=reinit_skipper,
                    preload=preload,
                    overwrite=overwrite,
                    overwrite_bedset=overwrite_bedset,
                    lite=lite,
                    r_service=r_service,
                    pm=pm,
                )
            except Exception as err:
                _LOGGER.error(
                    f"Processing of '{gse_pep.name}' failed with error: {err}"
                )
                gse_status.status = STATUS.FAIL
                gse_status.error = str(err)
                session.commit()
                continue

            status_parser(gse_status, upload_result)
            session.commit()

            if count >= download_limit:
                break

    pm.stop_pipeline()

    return None


def process_pep_sample(
    bed_sample: peprs.Sample,
    geo_tag: str = DEFAULT_GEO_TAG,
) -> BedBossRequired:
    """
    Process pep sample that contains a bed file.

    Downloads the bed file and composes a BedBossRequired data model with all required metadata
    (e.g. reference genome, type, organism).

    Args:
        bed_sample: peprs sample with bed file url.
        geo_tag: GEO tag to use when loading projects from PEPHub ('samples' or 'series').

    Returns:
        BedBossRequired with sample_name, gse, gsm, file_path, ref_genome, and other fields.
    """
    _LOGGER.debug(
        f"Standardizing metadata for: '{bed_sample.sample_name}' . GSE: {bed_sample.gse}"
    )

    if geo_tag == "series":
        project_metadata = BedBossMetadataSeries(**bed_sample.to_dict())

        description_text = f"{bed_sample.sample_name} {project_metadata.description}"
        if not project_metadata.assay or project_metadata.assay == "OTHER":
            predicted_assay = find_assay(description_text)
            if predicted_assay:
                project_metadata.assay = predicted_assay

        if not project_metadata.cell_line:
            predicted_cell_line = find_cell_line(description_text)
            if predicted_cell_line:
                project_metadata.cell_line = predicted_cell_line

        return BedBossRequired(
            sample_name=bed_sample.sample_name,
            file_path=bed_sample.file_url,
            ref_genome="undefined",
            type="bed",
            narrowpeak=False,
            pep=project_metadata,
            title=middle_underscored(bed_sample.get("sample_name", "")),
        )
    elif geo_tag == "samples":
        if bed_sample.type == "NARROWPEAK":
            file_type = "bed"
            is_narrowpeak = True
        elif bed_sample.type == "BROADPEAK":
            file_type = "bed"
            is_narrowpeak = False
        else:
            file_type = "bed"
            is_narrowpeak = False

        project_metadata = BedBossMetadata(**bed_sample.to_dict())

        description_text = f"{bed_sample.sample_name} {project_metadata.description}"

        if not project_metadata.assay or project_metadata.assay == "OTHER":
            predicted_assay = find_assay(description_text)
            if predicted_assay:
                project_metadata.assay = predicted_assay

        if not project_metadata.cell_line:
            predicted_cell_line = find_cell_line(description_text)
            if predicted_cell_line:
                project_metadata.cell_line = predicted_cell_line

        return BedBossRequired(
            sample_name=bed_sample.sample_name,
            file_path=bed_sample.file_url,
            ref_genome=(
                bed_sample.ref_genome.strip()
                if bed_sample.ref_genome
                else bed_sample.ref_genome
            ),
            type=file_type,
            narrowpeak=is_narrowpeak,
            pep=project_metadata,
            title=bed_sample.get("sample_title", ""),
        )
    else:
        raise BedBossException(
            f"Unsupported geo_tag: '{geo_tag}'. Please use 'samples' or 'series'."
        )


def get_pep(
    namespace: str,
    name: str,
    tag: str,
    phc: PEPHubClient = None,
):
    """
    Retrieve PEP from PEPHub.

    Args:
        namespace: Namespace of the project.
        name: Name of the project.
        tag: Tag of the project.
        phc: PEPHubClient instance.

    Returns:
        ProjectModel for the requested PEP.
    """
    if not phc:
        phc = PEPHubClient()
    return peprs.Project.from_pephub(f"{namespace}/{name}:{tag}")


def find_peps(
    start_date: str = "2000/01/01",
    end_date: str = None,
    filter_by: Literal["submission_date", "last_update_date"] = "submission_date",
    limit: int = 10000,
    offset: int = 0,
    geo_tag: str = DEFAULT_GEO_TAG,
    phc: PEPHubClient = None,
) -> SearchReturnModel:
    """
    Retrieve list of PEPs from 'bedbase' namespace in a certain time period.

    Args:
        start_date: Earliest date when pep was updated. Default: 2000/01/01.
        end_date: Latest date when pep was updated. Default: today's date.
        filter_by: Filter by submission_date or last_update_date.
        limit: Limit of projects to be searched.
        offset: Offset of projects to be searched.
        geo_tag: GEO tag to use when loading projects from PEPHub ('samples' or 'series').
        phc: PEPHubClient instance.

    Returns:
        SearchReturnModel with count, limit, offset, and list of ProjectAnnotationModel items.
    """
    if not phc:
        phc = PEPHubClient()
    _LOGGER.info(
        f"Searching PEPHub for projects (namespace='bedbase', tag='{geo_tag}', "
        f"dates={start_date} to {end_date}, limit={limit}, offset={offset})"
    )
    return phc.find_project(
        namespace="bedbase",
        tag=geo_tag,
        limit=limit,
        offset=offset,
        filter_by=filter_by,
        start_date=start_date,
        end_date=end_date,
    )


@calculate_time
def upload_gse(
    gse: str,
    bedbase_config: str | BedBaseAgent,
    outfolder: str = os.getcwd(),
    geo_tag: str = DEFAULT_GEO_TAG,
    create_bedset: bool = True,
    genome: str = None,
    preload: bool = True,
    rerun: bool = False,
    run_skipped: bool = False,
    run_failed: bool = True,
    standardize_pep: bool = False,
    use_skipper=True,
    reinit_skipper=False,
    overwrite=False,
    overwrite_bedset=True,
    lite=False,
):
    """
    Upload bed files from GEO series to BedBase.

    Args:
        gse: GEO series number.
        bedbase_config: Path to bedbase configuration file, or bbagent object.
        outfolder: Working directory where files will be downloaded, processed and statistics saved.
        geo_tag: GEO tag to use when loading projects from PEPHub ('samples' or 'series').
        create_bedset: Create bedset from bed files.
        genome: Reference genome to upload. If None, all genomes will be processed.
        preload: Pre-download files to the local folder (used for faster reproducibility).
        rerun: Rerun processing of the series.
        run_skipped: Rerun files that were skipped.
        run_failed: Rerun failed files.
        standardize_pep: Standardize pep metadata using BEDMS.
        use_skipper: Use skipper to skip already processed files logged locally.
        reinit_skipper: If True, skipper will be reinitialized and all log files cleaned.
        overwrite: Overwrite existing bedfiles.
        overwrite_bedset: Overwrite existing bedset.
        lite: Lite mode, skipping statistic processing for memory optimization and time saving.
    """
    _LOGGER.info(f"Initializing BedBaseAgent with config: '{bedbase_config}'")
    bbagent = BedBaseAgent(config=bedbase_config, init_ml=not lite)
    _LOGGER.info(f"BedBaseAgent initialized (ML enabled: {not lite})")
    gse_id = build_gse_identifier(gse, geo_tag)

    with Session(bbagent.config.db_engine.engine) as session:
        _LOGGER.info(f"Processing: '{gse_id}'")

        gse_status = session.scalar(
            select(GeoGseStatus).where(GeoGseStatus.gse == gse_id)
        )
        if gse_status:
            if gse_status.status == STATUS.SUCCESS and not rerun:
                _LOGGER.info(
                    f"Skipping: '{gse_id}' - already processed and rerun set to false"
                )
                exit()

            elif (
                gse_status.status == STATUS.FAIL
                or gse_status.status == STATUS.PROCESSING
            ) and not run_failed:
                _LOGGER.info("Reprocessing of failed set to false, exiting.")
                exit()

            elif (
                gse_status.status == STATUS.SKIPPED
                or gse_status.status == STATUS.PARTIAL
            ) and not run_skipped:
                _LOGGER.info(f"Run skipped files set to false, exiting. GSE: {gse_id}")
                exit()

        else:
            gse_status = GeoGseStatus(gse=gse_id, status=STATUS.FAIL)
            session.add(gse_status)
            session.commit()

        try:
            upload_result = _upload_gse(
                gse=gse,
                bedbase_config=bbagent,
                outfolder=outfolder,
                geo_tag=geo_tag,
                create_bedset=create_bedset,
                genome=genome,
                sa_session=session,
                gse_status_sa_model=gse_status,
                standardize_pep=standardize_pep,
                preload=preload,
                overwrite=overwrite,
                rerun=rerun,
                overwrite_bedset=overwrite_bedset,
                use_skipper=use_skipper,
                reinit_skipper=reinit_skipper,
                lite=lite,
            )
        except Exception as e:
            _LOGGER.error(f"Processing of '{gse_id}' failed with error: {e}")
            gse_status.status = STATUS.FAIL
            gse_status.error = str(e)
            session.commit()
            exit()

        status_parser(gse_status, upload_result)

        _LOGGER.info(f"Processing of '{gse_id}' is finished with success!")
        session.commit()


def status_parser(
    gse_status: GeoGseStatus, upload_result: ProjectProcessingStatus
) -> None:
    """
    Update status in SA object.

    Args:
        gse_status: GSE status of project (SQLAlchemy object).
        upload_result: Project processing status.
    """

    gse_status.number_of_files = upload_result.number_of_samples
    gse_status.number_of_success = upload_result.number_of_processed
    gse_status.number_of_skips = upload_result.number_of_skipped
    gse_status.number_of_fails = upload_result.number_of_failed
    if upload_result.number_of_samples == upload_result.number_of_processed:
        gse_status.status = STATUS.SUCCESS
    elif upload_result.number_of_skipped == upload_result.number_of_samples:
        gse_status.status = STATUS.SKIPPED
    elif upload_result.number_of_skipped > 0 and upload_result.number_of_failed == 0:
        gse_status.status = STATUS.PARTIAL
    else:
        gse_status.status = STATUS.FAIL


def _upload_gse(
    gse: str,
    bedbase_config: str | BedBaseAgent,
    outfolder: str = os.getcwd(),
    geo_tag: str = DEFAULT_GEO_TAG,
    create_bedset: bool = True,
    genome: str = None,
    sa_session: Session = None,
    gse_status_sa_model: GeoGseStatus = None,
    standardize_pep: bool = False,
    rerun: bool = False,
    overwrite: bool = False,
    overwrite_bedset: bool = False,
    use_skipper: bool = True,
    reinit_skipper: bool = False,
    preload: bool = True,
    lite=False,
    max_file_size: int = 20 * 1000000,
    r_service: RServiceManager = None,
    pm: pypiper.PipelineManager = None,
) -> ProjectProcessingStatus:
    """
    Upload bed files from GEO series to BedBase.

    Args:
        gse: GEO series number.
        bedbase_config: Path to bedbase configuration file, or bbagent object.
        outfolder: Working directory where files will be downloaded, processed and statistics saved.
        geo_tag: GEO tag to use when loading projects from PEPHub ('samples' or 'series').
        create_bedset: Create bedset from bed files.
        genome: Reference genome to upload. If None, all genomes will be processed.
        sa_session: Opened session to the database.
        gse_status_sa_model: SQLAlchemy model for project status.
        standardize_pep: Standardize pep metadata using BEDMS.
        rerun: Rerun processing of the series.
        overwrite: Overwrite existing bedfiles.
        overwrite_bedset: Overwrite existing bedset.
        use_skipper: Use skipper to skip already processed files logged locally.
        reinit_skipper: If True, skipper will be reinitialized and all logs cleaned.
        preload: Pre-download files to the local folder (used for faster reproducibility).
        lite: Lite mode, skipping statistic processing for memory optimization and time saving.
        max_file_size: Maximum file size in bytes. Default: 20MB.
        pm: PipelineManager object.
        r_service: RServiceManager object.

    Returns:
        ProjectProcessingStatus with counts of processed, skipped, and failed samples.
    """
    if isinstance(bedbase_config, str):
        bedbase_config = BedBaseAgent(config=bedbase_config)
    if genome:
        genome = standardize_genome_name(
            genome, reference_validator=reference_validator
        )

    gse_id = build_gse_identifier(gse, geo_tag)

    os.makedirs(outfolder, exist_ok=True)

    _LOGGER.info(f"Loading project from PEPHub: 'bedbase/{gse}:{geo_tag}'")
    project = peprs.Project.from_pephub(f"bedbase/{gse}:{geo_tag}")
    _LOGGER.info(f"Loaded project with {len(project.samples)} samples")

    if standardize_pep:
        project = pep_standardizer(project)

    project_status = ProjectProcessingStatus(number_of_samples=len(project.samples))
    uploaded_files = []
    gse_status_sa_model.number_of_files = len(project.samples)
    sa_session.commit()

    total_sample_number = len(project.samples)

    if use_skipper:
        skipper_obj = Skipper(output_path=outfolder, name=gse_id)
        if reinit_skipper:
            skipper_obj.reinitialize()
        _LOGGER.info(f"Skipper initialized for: '{gse_id}'")
    else:
        skipper_obj = None

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

    if not lite and not r_service:
        r_service = RServiceManager()
    elif lite:
        r_service = None
    else:
        r_service = r_service

    for counter, project_sample in enumerate(project.samples):
        _LOGGER.info(f">> Processing {counter + 1} / {total_sample_number}")
        sample_gsm = project_sample.get("sample_geo_accession", "").lower()
        sample_sample_name = project_sample.get("sample_name", "").lower()

        if skipper_obj:
            is_processed = skipper_obj.is_processed(
                f"{sample_gsm}_{sample_sample_name}"
            )
            if is_processed:
                _LOGGER.info(
                    f"Skipping: '{sample_gsm}_{sample_sample_name}' - already processed"
                )
                uploaded_files.append(is_processed)
                continue

        required_metadata = process_pep_sample(
            bed_sample=project_sample,
            geo_tag=geo_tag,
        )

        sample_status = sa_session.scalar(
            select(GeoGsmStatus).where(
                and_(
                    GeoGsmStatus.sample_name == required_metadata.sample_name,
                    GeoGsmStatus.gse_status_id == gse_status_sa_model.id,
                )
            )
        )

        if not sample_status:
            sample_status = GeoGsmStatus(
                gse_status_mapper=gse_status_sa_model,
                sample_name=required_metadata.sample_name,
                gsm=sample_gsm,
                status=STATUS.PROCESSING,
            )
            sa_session.add(sample_status)
            sa_session.commit()
        else:
            if sample_status.status == STATUS.SUCCESS and not rerun:
                _LOGGER.info(
                    f"Skipping: '{required_metadata.sample_name}' - already processed"
                )
                uploaded_files.append(sample_status.bed_id)
                project_status.number_of_processed += 1
                continue

        sample_status.genome = required_metadata.ref_genome
        # to upload files only with a specific genome
        if genome:
            if required_metadata.ref_genome != genome:
                _LOGGER.info(
                    f"Skipping: '{required_metadata.sample_name}' - genome mismatch. Expected: '{genome}' Found: '{required_metadata.ref_genome}'"
                )
                sample_status.status = STATUS.SKIPPED

                sa_session.commit()
                project_status.number_of_skipped += 1

                continue

        _LOGGER.info(
            f"Processing global_sample_id: '{required_metadata.pep.global_sample_id}' file: '{required_metadata.sample_name}' gse: '{gse}', geo_tag: '{geo_tag}'"
        )
        sample_status.status = STATUS.PROCESSING
        sa_session.commit()

        sample_status.file_size = project_sample.get("file_size", 0)
        sample_status.source_submission_date = project_sample.get(
            "sample_submission_date", None
        )

        try:
            if int(project_sample.get("file_size") or 0) > max_file_size:
                raise QualityException(
                    f"File size is too big. {int(project_sample.get('file_size', 0)) / 1000000} MB"
                )

            # to speed up the process, we can run initial QC on the file
            qc_file_size = run_initial_qc(project_sample.file_url)
            if qc_file_size > 0:
                sample_status.file_size = qc_file_size
        except QualityException as err:
            _LOGGER.error(f"Processing of '{sample_gsm}' failed with error: {str(err)}")
            sample_status.status = STATUS.FAIL
            sample_status.error = str(err)
            if err.file_size > 0:
                sample_status.file_size = min(
                    err.file_size, MAX_FILE_SIZE
                )  # we need to limit file size to MAX_FILE_SIZE for DB storage
            project_status.number_of_failed += 1

            if skipper_obj:
                skipper_obj.add_failed(
                    f"{sample_gsm}_{sample_sample_name}", f"Error: {str(err)}"
                )
            sa_session.commit()
            continue

        if preload:
            gsm_folder = create_gsm_sub_name(sample_gsm)
            files_path = os.path.join(outfolder, FILE_FOLDER_NAME, gsm_folder)
            os.makedirs(files_path, exist_ok=True)
            file_abs_path = os.path.abspath(
                os.path.join(files_path, project_sample.file)
            )
            _LOGGER.info(f"Downloading file to: '{file_abs_path}'")
            download_file(project_sample.file_url, file_abs_path, no_fail=True)
        else:
            file_abs_path = required_metadata.file_path

        try:
            original_genome = required_metadata.ref_genome
            # This code will standardize or predict genome if not provided
            required_metadata.ref_genome = standardize_genome_name(
                required_metadata.ref_genome,
                file_abs_path,
                reference_validator=reference_validator,
            )
            if original_genome != required_metadata.ref_genome:
                _LOGGER.info(
                    f"Genome standardized: '{original_genome}' -> '{required_metadata.ref_genome}'"
                )

            _LOGGER.info(
                f"Starting bed processing for '{required_metadata.sample_name}' (genome: {required_metadata.ref_genome})"
            )
            file_digest = run_all(
                name=required_metadata.title,
                input_file=file_abs_path,
                input_type=required_metadata.type,
                outfolder=os.path.join(outfolder, "outputs"),
                genome=required_metadata.ref_genome,
                bedbase_config=bedbase_config,
                narrowpeak=required_metadata.narrowpeak,
                other_metadata=required_metadata.pep.model_dump(),
                upload_pephub=True,
                upload_s3=True,
                upload_qdrant=True,
                force_overwrite=overwrite,
                lite=lite,
                pm=pm,
                r_service=r_service,
                reference_genome_validator=reference_validator,
            )
            _LOGGER.info(
                f"Successfully processed '{required_metadata.sample_name}' -> digest: {file_digest}"
            )
            uploaded_files.append(file_digest)
            if skipper_obj:
                skipper_obj.add_processed(
                    f"{sample_gsm}_{sample_sample_name}", file_digest
                )
            sample_status.status = STATUS.SUCCESS
            sample_status.bed_id = file_digest
            project_status.number_of_processed += 1

        except BedBossException as exc:
            _LOGGER.error(f"Processing of '{sample_gsm}' failed with error: {str(exc)}")
            sample_status.status = STATUS.FAIL
            sample_status.error = str(exc)
            project_status.number_of_failed += 1

            if skipper_obj:
                skipper_obj.add_failed(
                    f"{sample_gsm}_{sample_sample_name}", f"Error: {str(exc)}"
                )

        except GenimlBaseError as exc:
            _LOGGER.error(f"Processing of '{sample_gsm}' failed with error: {str(exc)}")
            sample_status.status = STATUS.FAIL
            sample_status.error = str(exc)
            project_status.number_of_failed += 1

            if skipper_obj:
                skipper_obj.add_failed(
                    f"{sample_gsm}_{sample_sample_name}", f"Error: {str(exc)}"
                )

        sa_session.commit()

    if create_bedset and uploaded_files:
        _LOGGER.info(f"Creating bedset for: '{gse_id}'")

        experiment_metadata = project.config.get("experiment_metadata", {})

        # Check if bedset already exists and merge bed IDs
        combined_bed_ids = list(uploaded_files)
        try:
            existing_bedset = bedbase_config.bedset.get(gse)
            if existing_bedset and existing_bedset.bed_ids:
                _LOGGER.info(
                    f"Bedset '{gse}' already exists with {len(existing_bedset.bed_ids)} files. Merging with {len(uploaded_files)} new files."
                )
                # Merge existing and new bed IDs, avoiding duplicates
                existing_ids = set(existing_bedset.bed_ids)
                for bed_id in uploaded_files:
                    if bed_id not in existing_ids:
                        existing_ids.add(bed_id)
                combined_bed_ids = list(existing_ids)
                _LOGGER.info(
                    f"Combined bedset will have {len(combined_bed_ids)} files."
                )
        except Exception:
            _LOGGER.debug(f"Bedset '{gse}' does not exist yet. Creating new bedset.")

        run_bedbuncher(
            bedbase_config=bedbase_config,
            record_id=gse,
            bed_set=combined_bed_ids,
            output_folder=os.path.join(outfolder, "outputs"),
            name=gse,
            description=project.description,
            heavy=False,  # TODO: set to False because can't handle bedset > 10 files
            upload_pephub=True,
            upload_s3=True,
            no_fail=True,
            force_overwrite=overwrite_bedset,
            lite=lite,
            annotation={
                "summary": experiment_metadata.get("series_summary", ""),
                "author": ", ".join(
                    filter(
                        None,
                        experiment_metadata.get("series_contact_name", "").split(","),
                    )
                ),
                "source": gse,
            },
        )

    else:
        _LOGGER.info(f"Skipping bedset creation for: '{gse_id}'")

    if stop_pipeline:
        pm.stop_pipeline()

    _LOGGER.info(
        f"Processing of '{gse_id}' completed: "
        f"{project_status.number_of_processed}/{project_status.number_of_samples} processed, "
        f"{project_status.number_of_skipped} skipped, {project_status.number_of_failed} failed"
    )
    return project_status
