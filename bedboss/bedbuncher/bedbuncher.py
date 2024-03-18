import bbconf
from geniml.io import BedSet
from bbconf import BedBaseConf
from bbconf.const import CFG_PATH_KEY, CFG_PATH_BEDBUNCHER_DIR_KEY
from geniml.bbclient import BBClient
from sqlmodel import select, func, Numeric, Float
import os
import json
import subprocess
from typing import Union, List
import peppy
import pephubclient
from pephubclient.helpers import is_registry_path
import logging
from ubiquerg import parse_registry_path

from bbconf.models import BedSetTableModel
from sqlalchemy.exc import IntegrityError

from bedboss.const import (
    BED_PEP_REGISTRY,
)

from bedboss.exceptions import BedBossException


_LOGGER = logging.getLogger("bedboss")


def create_view_in_pephub(bedfile_list: List[str], view_name: str) -> str:
    """
    Create a view in pephub

    :param bedfile_list: list of bedfiles ids
    :param view_name: name of the view

    :return: view_name
    """

    phc = pephubclient.PEPHubClient()
    reg_path_obj = parse_registry_path(BED_PEP_REGISTRY)
    bed_ids = bedfile_list

    phc.view.create(
        namespace=reg_path_obj["namespace"],
        name=reg_path_obj["item"],
        tag=reg_path_obj["tag"],
        view_name=view_name,
        sample_list=bed_ids,
    )
    return view_name


def calculate_bedset_statistics(bbc: BedBaseConf, bedset: List[str]) -> dict:
    """
    Calculate mean and standard deviation for each numeric column of bedfiles in bedset

    :param bbc: BedBase configuration object
    :param bedset: Bedset object or list of bedfiles ids

    :return: dict with mean and standard deviation for each
        {"sd": {"column_name": sd_value},
         "mean": {"column_name": mean_value}}
    """

    _LOGGER.info("Calculating bedset statistics...")
    if not isinstance(bedset, list):
        raise BedBossException(f"Input of bedset should be a list of bedfiles ids")

    numeric_columns = [
        column
        for column, value in bbc.bed.result_schemas.items()
        if value["type"] == "number"
    ]

    results_dict = {"mean": {}, "sd": {}}

    for column_name in numeric_columns:
        with bbc.bed.backend.session as s:
            mean_bedset_statement = select(
                func.round(
                    func.avg(getattr(bbc.BedfileORM, column_name)).cast(Numeric), 4
                ).cast(Float)
            ).where(bbc.BedfileORM.record_identifier.in_(bedset))
            sd_bedset_statement = select(
                func.round(
                    func.stddev(getattr(bbc.BedfileORM, column_name)).cast(Numeric), 4
                ).cast(Float)
            ).where(bbc.BedfileORM.record_identifier.in_(bedset))

            results_dict["mean"][column_name] = s.exec(mean_bedset_statement).one()
            results_dict["sd"][column_name] = s.exec(sd_bedset_statement).one()

    _LOGGER.info("Bedset statistics were calculated successfully")
    return results_dict


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
    bbc: BedBaseConf,
    bedset: BedSet,
) -> dict:
    """
    Create plots for a bedset (commonality region plot)

    :param bbc: BedBaseConf object
    :param bedset: Bedset object
    :return: dict with information about crated plots
    """
    bedset_md5sum = bedset.identifier

    output_folder = os.path.abspath(
        bbc.config[CFG_PATH_KEY][CFG_PATH_BEDBUNCHER_DIR_KEY]
    )
    # if output folder doesn't exist create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    bedset_list_path = os.path.join(output_folder, f"{bedset_md5sum}_bedset.txt")
    create_bed_list_file(bedset, bedset_list_path)
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
    bedbase_config: Union[BedBaseConf, str],
    record_id: str,
    bed_set: Union[BedSet, List[str]],
    genome: dict = None,
    description: str = None,
    heavy: bool = False,
    upload_pephub: bool = False,
    no_fail: bool = False,
    force_overwrite: bool = False,
) -> None:
    """
    Add bedset to the database

    :param bedbase_config: BedBaseConf object
    :param record_id: record identifier or name to be used in database
    :param bed_set: Bedset object or list of bedfiles ids
    :param genome: genome of the bedset
    :param description: Bedset description
    :param heavy: whether to use heavy processing (add all columns to the database).
        if False -> R-script won't be executed, only basic statistics will be calculated
    :param no_fail: whether to raise an error if bedset was not added to the database
    :param upload_pephub: whether to create a view in pephub
    :param force_overwrite: whether to overwrite the record in the database
    # TODO: force_overwrite is not working!!! Fix it!
    :return:
    """
    _LOGGER.info(f"Adding bedset { record_id} to the database")

    if isinstance(bedbase_config, str):
        bedbase_config = BedBaseConf(bedbase_config)

    if isinstance(bed_set, list):
        bbclient = BBClient()
        bed_set_object = BedSet()
        for bed_id in bed_set:
            bed_set_object.add(bbclient.load_bed(bed_id))
    else:
        bed_set_object = bed_set
        bed_set = [sample.identifier for sample in bed_set_object]

    if not record_id:
        raise BedBossException(
            "bedset_name was not provided correctly. Please provide it in pep name or as argument"
        )

    bedset_stats = calculate_bedset_statistics(bedbase_config, bed_set)

    result_dict = BedSetTableModel(
        name=record_id,
        md5sum=bed_set_object.identifier,
        description=description,
        genome=genome,
        bedset_standard_deviation=bedset_stats["sd"],
        bedset_means=bedset_stats["mean"],
        processed=heavy,
    )

    if heavy:
        _LOGGER.info("Heavy processing is True. Calculating plots...")
        plot_value = create_plots(
            bedbase_config,
            bedset=bed_set_object,
        )
        result_dict.region_commonality = plot_value
    else:
        _LOGGER.info("Heavy processing is False. Plots won't be calculated")

    bedbase_config.bedset.report(
        record_identifier=record_id,
        values=result_dict.model_dump(exclude_none=True),
        force_overwrite=True,
    )
    try:
        for sample in bed_set:
            bedbase_config.report_relationship(record_id, sample)
    except IntegrityError as e:
        if not no_fail:
            raise e
        _LOGGER.warning(f"Failed to add relationship to the database: {e}.")

    _LOGGER.info(
        f"Bedset {record_id} was added successfully to the database. "
        f"With following files: {', '.join([sample for sample in bed_set])}"
    )

    if upload_pephub:
        create_view_in_pephub(bed_set, record_id)
    else:
        _LOGGER.info(f"Bedset {record_id} was not uploaded to pephub")


def run_bedbuncher_form_pep(
    bedbase_config: Union[str, bbconf.BedBaseConf],
    bedset_pep: Union[str, peppy.Project],
    bedset_name: str = None,
    heavy: bool = False,
    upload_pephub: bool = False,
    no_fail: bool = False,
    force_overwrite: bool = False,
) -> str:
    """
    Create bedset from pep and add it to the database

    :param bedbase_config: BedBaseConf object or path to the config file
    :param bedset_pep: path to the pep file or pephub registry path
    :param bedset_name: name of the bedset
    :param heavy: whether to use heavy processing (add all columns to the database).
        if False -> R-script won't be executed, only basic statistics will be calculated
    :param upload_pephub: whether to create a view in pephub
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
        record_id=bedset_name,
        bed_set=bedfiles_list,
        heavy=heavy,
        upload_pephub=upload_pephub,
        no_fail=no_fail,
        force_overwrite=force_overwrite,
    )
    return bedset_name


# if __name__ == "__main__":
# run_bedbuncher(
#     "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#     record_id="test_24_03_14",
#     bed_set=["6c72563b25b74441f10894b71f9b40c5", "607821f77ab0af9bc09bb25163b4e861"],
#     no_fail=True,
#     upload_pephub=True,
# )

# run_bedbuncher_form_pep(
#     "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml",
#     bedset_name="pephub_test",
#     bedset_pep="khoroshevskyi/bedbunch:default",
#     upload_pephub=True,
# )
