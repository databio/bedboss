import json
import logging
import os
import subprocess
from typing import List, Union

import pephubclient
import peppy
from bbconf import BedBaseAgent
from bbconf.models.base_models import FileModel
from bbconf.models.bedset_models import BedSetPlots
from geniml.bbclient import BBClient
from geniml.io import BedSet
from pephubclient.helpers import is_registry_path

from bedboss.exceptions import BedBossException

_LOGGER = logging.getLogger("bedboss")


def create_bed_list_file(bedset: BedSet, file_path: str) -> None:
    """
    Create a file with bed_set_list (Later this file is used in R script)

    :param bedset: bed_set object
    :param file_path: path to the file
    :return: None
    """
    list_of_samples = [sample.path for sample in bedset]

    with open(file_path, "w") as f:
        for sample in list_of_samples:
            f.write(sample + "\n")

    return None


def create_plots(
    bedset: List[str],
    output_folder: str,
) -> dict:
    """
    Create plots for a bedset (commonality region plot)

    :param bedset: list of bedfiles ids
    :param output_folder: path to the output folder
    :return: dict with information about crated plots
    """
    bbclient_obj = BBClient()

    bed_set_object = BedSet()
    for bed_id in bedset:
        bed_set_object.add(bbclient_obj.load_bed(bed_id))

    bedset_md5sum = bed_set_object.identifier

    # if output folder doesn't exist create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    bedset_list_path = os.path.join(output_folder, f"{bedset_md5sum}_bedset.txt")
    create_bed_list_file(bed_set_object, bedset_list_path)
    rscript_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bedbuncher",
        "tools",
        "bedsetStat.R",
    )
    assert os.path.exists(rscript_path), FileNotFoundError(
        f"'{rscript_path}' script not found"
    )

    json_file_path = os.path.join(output_folder, bedset_md5sum + ".json")
    command = (
        f"Rscript {rscript_path} --outputfolder={output_folder} "
        f"--bedfilelist={bedset_list_path} --id={bedset_md5sum} "
        f"--json={json_file_path}"
    )

    subprocess.run(command, shell=True)

    with open(json_file_path, "r", encoding="utf-8") as f:
        bedset_summary_info = json.loads(f.read())

    os.remove(bedset_list_path)
    os.remove(json_file_path)

    _LOGGER.info("Plots were created successfully and mediated files were removed")
    return bedset_summary_info["plots"][0]


def run_bedbuncher(
    bedbase_config: Union[str, BedBaseAgent],
    record_id: str,
    bed_set: List[str],
    output_folder: str,
    name: str = None,
    description: str = None,
    annotation: dict = None,
    heavy: bool = False,
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
    :param heavy: whether to use heavy processing (add all columns to the database).
        if False -> R-script won't be executed, only basic statistics will be calculated
    :param no_fail: whether to raise an error if bedset was not added to the database
    :param upload_pephub: whether to create a view in pephub
    :param upload_s3: whether to upload files to s3
    :param force_overwrite: whether to overwrite the record in the database
    :param lite: whether to run the pipeline in lite mode
    # TODO: force_overwrite is not working!!! Fix it!
    :return:
    """
    _LOGGER.info(f"Adding bedset { record_id} to the database")

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

    if heavy:
        _LOGGER.info("Heavy processing is True. Calculating plots...")
        plot_value = create_plots(
            bedset=bed_set,
            output_folder=output_folder,
        )
        plots = BedSetPlots(region_commonality=FileModel(**plot_value))
    else:
        _LOGGER.info("Heavy processing is False. Plots won't be calculated")
        plots = None

    bbagent.bedset.create(
        identifier=record_id,
        name=name,
        bedid_list=bed_set,
        statistics=True,
        description=description,
        upload_pephub=upload_pephub,
        upload_s3=upload_s3,
        plots=plots.model_dump(exclude_none=True, exclude_unset=True) if plots else {},
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
    heavy: bool = False,
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
    :param heavy: whether to use heavy processing (add all columns to the database).
        if False -> R-script won't be executed, only basic statistics will be calculated
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
        heavy=heavy,
        upload_pephub=upload_pephub,
        upload_s3=upload_s3,
        no_fail=no_fail,
        force_overwrite=force_overwrite,
    )
    return bedset_name


# if __name__ == "__main__":
#     run_bedbuncher(
#         "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#         record_id="test_bedset",
#         name="This is a name",
#         bed_set=[
#             "bbad85f21962bb8d972444f7f9a3a932",
#             "0dcdf8986a72a3d85805bbc9493a1302",
#         ],
#         output_folder="/home/bnt4me/virginia/",
#         description="This is a description",
#         upload_s3=True,
#         no_fail=True,
#         upload_pephub=True,
#         heavy=True,
#     )

# run_bedbuncher_form_pep(
#     "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#     bedset_name="pephub_test",
#     bedset_pep="khoroshevskyi/bedbunch:default",
#     upload_pephub=True,
# )
