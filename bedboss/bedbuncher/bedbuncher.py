import logging
import os
from typing import List, Union

import pephubclient
import peppy
from bbconf import BedBaseAgent
from pephubclient.helpers import is_registry_path

from bedboss.exceptions import BedBossException

_LOGGER = logging.getLogger("bedboss")


def run_bedbuncher(
    bedbase_config: Union[str, BedBaseAgent],
    record_id: str,
    bed_set: List[str],
    output_folder: str,
    name: str = None,
    description: str = None,
    annotation: dict = None,
    upload_pephub: bool = False,
    upload_s3: bool = False,
    no_fail: bool = False,
    force_overwrite: bool = False,
    lite: bool = False,
) -> None:
    """
    Add bedset to the database

    :param bedbase_config: BedBaseConf object
    :param record_id: record identifier or name to be used in database
    :param name: name of the bedset
    :param output_folder: path to the output folder
    :param bed_set: Bedset object or list of bedfiles ids
    :param description: Bedset description
    :param annotation: Bedset annotation (author, source, summary, etc.)
    :param no_fail: whether to raise an error if bedset was not added to the database
    :param upload_pephub: whether to create a view in pephub
    :param upload_s3: whether to upload files to s3
    :param force_overwrite: whether to overwrite the record in the database
    :param lite: whether to run the pipeline in lite mode
    :return:
    """
    _LOGGER.info(f"Adding bedset {record_id} to the database")

    if isinstance(bedbase_config, str):
        bbagent = BedBaseAgent(bedbase_config)
    else:
        bbagent = bedbase_config

    if not record_id:
        raise BedBossException(
            "bedset_name was not provided correctly. Please provide it in pep name or as argument"
        )

    output_folder = os.path.join(
        output_folder,
        "bedsets",
    )

    bbagent.bedset.create(
        identifier=record_id,
        name=name,
        bedid_list=bed_set,
        statistics=True,
        description=description,
        upload_pephub=upload_pephub,
        upload_s3=upload_s3,
        local_path=output_folder,
        no_fail=no_fail,
        overwrite=force_overwrite,
        annotation=annotation,
        processed=not lite,
    )


def run_bedbuncher_form_pep(
    bedbase_config: str,
    bedset_pep: Union[str, peppy.Project],
    output_folder: str,
    bedset_name: str = None,
    upload_pephub: bool = False,
    upload_s3: bool = False,
    no_fail: bool = False,
    force_overwrite: bool = False,
) -> str:
    """
    Create bedset from pep and add it to the database

    :param bedbase_config: BedBaseConf object or path to the config file
    :param bedset_pep: path to the pep file or pephub registry path
    :param bedset_name: name of the bedset
    :param output_folder: path to the output folder
    :param upload_pephub: whether to create a view in pephub
    :param upload_s3: whether to upload files to s3
    :param no_fail: whether to raise an error if bedset was not added to the database
    :param force_overwrite: whether to overwrite the record in the database

    return bedset_name
    """
    if isinstance(bedset_pep, peppy.Project):
        pep_of_bed = bedset_pep
    elif isinstance(bedset_pep, str):
        if is_registry_path(bedset_pep):
            pep_of_bed = pephubclient.PEPHubClient().load_project(bedset_pep)
        else:
            pep_of_bed = peppy.Project(bedset_pep)
    else:
        raise ValueError(
            "bedset_pep should be either path to the pep file or pephub registry path"
        )

    bedfiles_list = [
        bedfile_id.get("record_identifier") or bedfile_id.sample_name
        for bedfile_id in pep_of_bed.samples
    ]

    run_bedbuncher(
        bedbase_config=bedbase_config,
        record_id=bedset_name or pep_of_bed.name,
        bed_set=bedfiles_list,
        output_folder=output_folder,
        name=bedset_name or pep_of_bed.name,
        description=pep_of_bed.description,
        upload_pephub=upload_pephub,
        upload_s3=upload_s3,
        no_fail=no_fail,
        force_overwrite=force_overwrite,
    )
    return bedset_name
