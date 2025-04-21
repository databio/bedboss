import logging
import os
from typing import Literal, Union

import pypiper

import peppy
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
from bedboss.bbuploader.models import (
    BedBossMetadata,
    BedBossRequired,
    ProjectProcessingStatus,
)
from bedboss.bbuploader.utils import create_gsm_sub_name
from bedboss.bedboss import run_all
from bedboss.bedbuncher.bedbuncher import run_bedbuncher
from bedboss.exceptions import BedBossException, QualityException
from bedboss.skipper import Skipper
from bedboss.utils import (
    calculate_time,
    download_file,
    standardize_genome_name,
    run_initial_qc,
)
from bedboss.utils import standardize_pep as pep_standardizer
from bedboss.bedstat.r_service import RServiceManager
from bedboss._version import __version__

_LOGGER = logging.getLogger(PKG_NAME)
_LOGGER.setLevel(logging.DEBUG)


@calculate_time
def upload_all(
    bedbase_config: str,
    outfolder: str = os.getcwd(),
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
    This is main function that is responsible for processing bed files from PEPHub.

    :param outfolder: working directory, where files will be downloaded, processed and statistics will be saved
    :param bedbase_config: path to bedbase configuration file
    :param start_date: The earliest date when opep was updated [Default: 2000/01/01]
    :param end_date: The latest date when opep was updated [Default: today's date]
    :param search_limit: limit of projects to be searched
    :param search_offset: offset of projects to be searched
    :param download_limit: limit of GSE projects to be downloaded (used for testing purposes) [Default: 100]
    :param genome: reference genome [Default: None] (e.g. hg38) - if None, all genomes will be processed
    :param create_bedset: create bedset from bed files
    :param preload: pre - download files to the local folder (used for faster reproducibility)
    :param rerun: rerun processing of the series. Used in logging system. If you want to reupload file use overwrite
    :param run_skipped: rerun files that were skipped. Used in logging system. If you want to reupload file use overwrite
    :param run_failed: rerun failed files. Used in logging system. If you want to reupload file use overwrite
    :param standardize_pep: standardize pep metadata using BEDMS
    :param use_skipper: use skipper to skip already processed logged locally. Skipper creates local log of processed
        and failed files.
    :param reinit_skipper: reinitialize skipper, if set to True, skipper will be reinitialized and all logs files will be cleaned
    :param lite: lite mode, where skipping statistic processing for memory optimization and time saving
    """

    phc = PEPHubClient()
    os.makedirs(outfolder, exist_ok=True)

    bbagent = BedBaseAgent(config=bedbase_config, init_ml=not lite)
    genome = standardize_genome_name(genome)

    pep_annotation_list = find_peps(
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
        r_service = RServiceManager()
    else:
        r_service = None

    for gse_pep in pep_annotation_list.results:
        count += 1
        with Session(bbagent.config.db_engine.engine) as session:
            MessageHandler.print_success(f"{'##' * 30}")
            MessageHandler.print_success(
                f"#### Processing: '{gse_pep.name}'. #### Processing {count} / {total_projects}. ####"
            )

            gse_status = session.scalar(
                select(GeoGseStatus).where(GeoGseStatus.gse == gse_pep.name)
            )

            if gse_status:
                if gse_status.status == STATUS.SUCCESS and not rerun:
                    _LOGGER.info(
                        f"Skipping: '{gse_pep.name}' - already processed and rerun set to false"
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
                        f"Run skipped files set to false, exiting. GSE: {gse_pep.name}"
                    )
                    continue

            else:
                gse_status = GeoGseStatus(gse=gse_pep.name, status=STATUS.PROCESSING)
                session.add(gse_status)
                session.commit()

            try:
                upload_result = _upload_gse(
                    gse=gse_pep.name,
                    bedbase_config=bbagent,
                    outfolder=outfolder,
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
    bed_sample: peppy.Sample,
) -> BedBossRequired:
    """
    Process pep sample that contains bed file. Download bed file and compose BedBossRequired data model
        that contains all required bed file metadata (e.g. reference genome, type, organism...)

    :param bed_sample: peppy sample with bed file url
    :return: BedBossRequired {sample_name: str,
                              gse: str,
                              gsm: str,
                              file_path: str,
                              ref_genome:str,
                              ...
                                }
    """
    _LOGGER.debug(
        f"Standardizing metadata for: '{bed_sample.sample_name}' . GSE: {bed_sample.gse}"
    )

    if bed_sample.type == "NARROWPEAK":
        file_type = "bed"
        is_narrowpeak = True
    elif bed_sample.type == "BROADPEAK":
        file_type = "bed"
        is_narrowpeak = False
    else:
        file_type = bed_sample.type.lower()
        is_narrowpeak = False

    project_metadata = BedBossMetadata(**bed_sample.to_dict())

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
        title=bed_sample.sample_title,
    )


def get_pep(
    namespace: str,
    name: str,
    tag: str,
    phc: PEPHubClient = None,
):
    """
    Retrieve PEP from PEPHub

    :param namespace: namespace of the project
    :param name: name of the project
    :param tag: tag of the project
    :param phc: PEPHubClient instance
    :return: ProjectModel
    """
    if not phc:
        phc = PEPHubClient()
    return phc.load_project(f"{namespace}/{name}:{tag}")


def find_peps(
    start_date: str = "2000/01/01",
    end_date: str = None,
    filter_by: Literal["submission_date", "last_update_date"] = "submission_date",
    limit: int = 10000,
    offset: int = 0,
    phc: PEPHubClient = None,
) -> SearchReturnModel:
    """
    Retrieve list of PEPs from 'bedbase' namespace in certain time period

    :param start_date: earliest date when opep was updated [Default: 2000/01/01]
    :param end_date: latest date when opep was updated [Default: today's date]
    :param filter_by: filter by submission date [Default: submission_date] Option: [submission_date, last_update_date]
    :param limit: limit of projects to be searched
    :param offset: offset of projects to be searched
    :param phc: PEPHubClient instance
    :return SearchReturnModel: {count: int
                                limit: int
                                offset: int
                                items: List[ProjectAnnotationModel]
                            }
    """
    if not phc:
        phc = PEPHubClient()
    return phc.find_project(
        namespace="bedbase",
        limit=limit,
        offset=offset,
        filter_by=filter_by,
        start_date=start_date,
        end_date=end_date,
    )


@calculate_time
def upload_gse(
    gse: str,
    bedbase_config: Union[str, BedBaseAgent],
    outfolder: str = os.getcwd(),
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
    Upload bed files from GEO series to BedBase

    :param gse: GEO series number
    :param bedbase_config: path to bedbase configuration file, or bbagent object
    :param outfolder: working directory, where files will be downloaded, processed and statistics will be saved
    :param create_bedset: create bedset from bed files
    :param genome: reference genome to upload to database. If None, all genomes will be processed
    :param preload: pre - download files to the local folder (used for faster reproducibility)
    :param rerun: rerun processing of the series
    :param run_skipped: rerun files that were skipped
    :param run_failed: rerun failed files
    :param standardize_pep: standardize pep metadata using BEDMS
    :param use_skipper: use skipper to skip already processed logged locally. Skipper creates local log of processed
        and failed files.
    :param reinit_skipper: reinitialize skipper, if set to True, skipper will be reinitialized and all logs files will be cleaned
    :param overwrite: overwrite existing bedfiles
    :param overwrite_bedset: overwrite existing bedset
    :param lite: lite mode, where skipping statistic processing for memory optimization and time saving

    :return: None
    """
    bbagent = BedBaseAgent(config=bedbase_config, init_ml=not lite)

    with Session(bbagent.config.db_engine.engine) as session:
        _LOGGER.info(f"Processing: '{gse}'")

        gse_status = session.scalar(select(GeoGseStatus).where(GeoGseStatus.gse == gse))
        if gse_status:
            if gse_status.status == STATUS.SUCCESS and not rerun:
                _LOGGER.info(
                    f"Skipping: '{gse}' - already processed and rerun set to false"
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
                _LOGGER.info(f"Run skipped files set to false, exiting. GSE: {gse}")
                exit()

        else:
            gse_status = GeoGseStatus(gse=gse, status=STATUS.FAIL)
            session.add(gse_status)
            session.commit()

        try:
            upload_result = _upload_gse(
                gse=gse,
                bedbase_config=bbagent,
                outfolder=outfolder,
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
            _LOGGER.error(f"Processing of '{gse}' failed with error: {e}")
            gse_status.status = STATUS.FAIL
            gse_status.error = str(e)
            session.commit()
            exit()

        status_parser(gse_status, upload_result)

        _LOGGER.info(f"Processing of '{gse}' is finished with success!")
        session.commit()


def status_parser(
    gse_status: GeoGseStatus, upload_result: ProjectProcessingStatus
) -> None:
    """
    Update status in SA object

    :param gse_status: gse status of project (Sqlalchemy object)
    :param upload_result: project processing status (status object)
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
    bedbase_config: Union[str, BedBaseAgent],
    outfolder: str = os.getcwd(),
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
    Upload bed files from GEO series to BedBase

    :param gse: GEO series number
    :param bedbase_config: path to bedbase configuration file, or bbagent object
    :param outfolder: working directory, where files will be downloaded, processed and statistics will be saved
    :param create_bedset: create bedset from bed files
    :param genome: reference genome to upload to database. If None, all genomes will be processed
    :param sa_session: opened session to the database
    :param gse_status_sa_model: sqlalchemy model for project status
    :param standardize_pep: standardize pep metadata using BEDMS
    :param rerun: rerun processing of the series
    :param overwrite: overwrite existing bedfiles
    :param overwrite_bedset: overwrite existing bedset
    :param use_skipper: use skipper to skip already processed logged locally. Skipper creates local log of processed
        and failed files.
    :param reinit_skipper: reinitialize skipper, if set to True, skipper will be reinitialized and all logs will be
    :param preload: pre - download files to the local folder (used for faster reproducibility)
    :param lite: lite mode, where skipping statistic processing for memory optimization and time saving
    :param max_file_size: maximum file size in bytes. Default: 20MB
    :param pypiper.PipelineManager pm: pypiper object
    :param r_service: RServiceManager object
    :return: None
    """
    if isinstance(bedbase_config, str):
        bedbase_config = BedBaseAgent(config=bedbase_config)
    if genome:
        genome = standardize_genome_name(genome)

    phc = PEPHubClient()
    os.makedirs(outfolder, exist_ok=True)

    project = phc.load_project(f"bedbase/{gse}:{DEFAULT_GEO_TAG}")

    if standardize_pep:
        project = pep_standardizer(project)

    project_status = ProjectProcessingStatus(number_of_samples=len(project.samples))
    uploaded_files = []
    gse_status_sa_model.number_of_files = len(project.samples)
    sa_session.commit()

    total_sample_number = len(project.samples)

    if use_skipper:
        skipper_obj = Skipper(output_path=outfolder, name=gse)
        if reinit_skipper:
            skipper_obj.reinitialize()
        _LOGGER.info(f"Skipper initialized for: '{gse}'")
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
        _LOGGER.info(f">> Processing {counter+1} / {total_sample_number}")
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
            f"Processing global_sample_id: '{required_metadata.pep.global_sample_id}' file: '{required_metadata.sample_name}' gse: '{gse}'"
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
            run_initial_qc(project_sample.file_url)
        except QualityException as err:
            _LOGGER.error(f"Processing of '{sample_gsm}' failed with error: {str(err)}")
            sample_status.status = STATUS.FAIL
            sample_status.error = str(err)
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
            download_file(project_sample.file_url, file_abs_path, no_fail=True)
        else:
            file_abs_path = required_metadata.file_path

        try:
            required_metadata.ref_genome = standardize_genome_name(
                required_metadata.ref_genome, file_abs_path
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
        _LOGGER.info(f"Creating bedset for: '{gse}'")

        experiment_metadata = project.config.get("experiment_metadata", {})

        run_bedbuncher(
            bedbase_config=bedbase_config,
            record_id=gse,
            bed_set=uploaded_files,
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
        _LOGGER.info(f"Skipping bedset creation for: '{gse}'")

    if stop_pipeline:
        pm.stop_pipeline()

    _LOGGER.info(f"Processing of '{gse}' is finished with success!")
    return project_status
