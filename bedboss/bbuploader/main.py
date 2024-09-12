import logging
import os
from typing import Literal, Union

import peppy
from bbconf import BedBaseAgent
from bbconf.db_utils import GeoGseStatus, GeoGsmStatus
from pephubclient import PEPHubClient
from pephubclient.models import SearchReturnModel
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from bedboss.bbuploader.constants import DEFAULT_GEO_TAG, PKG_NAME, STATUS
from bedboss.bbuploader.models import (
    BedBossMetadata,
    BedBossRequired,
    ProjectProcessingStatus,
)
from bedboss.bedboss import run_all
from bedboss.bedbuncher.bedbuncher import run_bedbuncher
from bedboss.exceptions import BedBossException
from bedboss.utils import standardize_genome_name

_LOGGER = logging.getLogger(PKG_NAME)
_LOGGER.setLevel(logging.DEBUG)


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
    rerun: bool = False,
    run_skipped=False,
    run_failed=True,
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
    :param rerun: rerun processing of the series
    :param run_skipped: rerun files that were skipped
    :param run_failed: rerun failed files
    """

    phc = PEPHubClient()
    os.makedirs(outfolder, exist_ok=True)

    bbagent = BedBaseAgent(config=bedbase_config)
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
    for gse_pep in pep_annotation_list.results:

        with Session(bbagent.config.db_engine.engine) as session:
            _LOGGER.info(f"Processing: '{gse_pep.name}'")

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
                )
            except Exception as err:
                _LOGGER.error(
                    f"Processing of '{gse_pep.name}' failed with error: {err}"
                )
                gse_status.status = STATUS.FAIL
                session.commit()
                continue

            status_parser(gse_status, upload_result)
            session.commit()

            count += 1
            if count >= download_limit:
                break


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
        ref_genome=standardize_genome_name(bed_sample.ref_genome),
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


def upload_gse(
    gse: str,
    bedbase_config: Union[str, BedBaseAgent],
    outfolder: str = os.getcwd(),
    create_bedset: bool = True,
    genome: str = None,
    rerun: bool = False,
    run_skipped=False,
    run_failed=True,
):
    """
    Upload bed files from GEO series to BedBase

    :param gse: GEO series number
    :param bedbase_config: path to bedbase configuration file, or bbagent object
    :param outfolder: working directory, where files will be downloaded, processed and statistics will be saved
    :param create_bedset: create bedset from bed files
    :param genome: reference genome to upload to database. If None, all genomes will be processed
    :param rerun: rerun processing of the series
    :param run_skipped: rerun files that were skipped
    :param run_failed: rerun failed files

    :return: None
    """
    bbagent = BedBaseAgent(config=bedbase_config)

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
                bedbase_config=bedbase_config,
                outfolder=outfolder,
                create_bedset=create_bedset,
                genome=genome,
                sa_session=session,
                gse_status_sa_model=gse_status,
            )
        except Exception as e:
            _LOGGER.error(f"Processing of '{gse}' failed with error: {e}")
            gse_status.status = STATUS.FAIL
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

    :return: None
    """
    if isinstance(bedbase_config, str):
        bedbase_config = BedBaseAgent(config=bedbase_config)
    if genome:
        genome = standardize_genome_name(genome)

    phc = PEPHubClient()
    os.makedirs(outfolder, exist_ok=True)

    project = phc.load_project(f"bedbase/{gse}:{DEFAULT_GEO_TAG}")

    project_status = ProjectProcessingStatus(number_of_samples=len(project.samples))
    uploaded_files = []
    gse_status_sa_model.number_of_files = len(project.samples)
    sa_session.commit()
    for project_sample in project.samples:

        sample_gsm = project_sample.get("sample_geo_accession", "").lower()

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

        try:
            file_digest = run_all(
                name=required_metadata.title,
                input_file=required_metadata.file_path,
                input_type=required_metadata.type,
                outfolder=os.path.join(outfolder, "outputs"),
                genome=required_metadata.ref_genome,
                bedbase_config=bedbase_config,
                narrowpeak=required_metadata.narrowpeak,
                other_metadata=required_metadata.pep.model_dump(),
                upload_pephub=True,
                upload_s3=True,
                upload_qdrant=True,
                force_overwrite=False,
            )
            uploaded_files.append(file_digest)
            sample_status.status = STATUS.SUCCESS
            project_status.number_of_processed += 1

        except BedBossException as exc:
            sample_status.status = STATUS.FAIL
            sample_status.error = str(exc)
            project_status.number_of_failed += 1

        sa_session.commit()

    if create_bedset and uploaded_files:

        _LOGGER.info(f"Creating bedset for: '{gse}'")
        run_bedbuncher(
            bedbase_config=bedbase_config,
            record_id=gse,
            bed_set=uploaded_files,
            output_folder=os.path.join(outfolder, "outputs"),
            name=gse,
            description=project.description,
            heavy=True,
            upload_pephub=True,
            upload_s3=True,
            no_fail=True,
            force_overwrite=False,
        )

    else:
        _LOGGER.info(f"Skipping bedset creation for: '{gse}'")

    _LOGGER.info(f"Processing of '{gse}' is finished with success!")
    return project_status


#
# if __name__ == "__main__":
#     # upload_gse(
#     #     # gse="gse246900",
#     #     # gse="gse247593",
#     #     # gse="gse241222",
#     #     #gse="gse266130",
#     #     gse="gse99178",
#     #     # gse="gse240325", # TODO: check if qc works
#     #     # gse="gse229592", # mice
#     #     bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#     #     outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
#     #     # genome="HG38",
#     #     # rerun=True,
#     #     run_failed=True,
#     #     run_skipped=True,
#     # )
#     upload_all(
#         bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#         outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
#         start_date="2024/01/21",
#         end_date="2024/08/28",
#         search_limit=2,
#         search_offset=0,
#         genome="GRCh38",
#         rerun=True,
#     )
# # upload_all(
# #     bedbase_config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
# #     outfolder="/home/bnt4me/virginia/repos/bbuploader/data",
# #     start_date="2024/01/01",
# #     # end_date="2024/03/28",
# #     search_limit=200,
# #     search_offset=0,
# #     genome="GRCh38",
# # )
