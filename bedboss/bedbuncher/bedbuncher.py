from geniml.io import BedSet
from bbconf import BedBaseConf
from bbconf.const import CFG_PATH_KEY, CFG_PATH_BEDBUNCHER_DIR_KEY
from geniml.bbclient import BBClient
from sqlmodel import select, func, Numeric, Float
import os
import json
import subprocess
import peppy
import pephubclient
from pephubclient.helpers import is_registry_path
import logging

from bedboss.const import DEFAULT_BEDBASE_API_URL, DEFAULT_BEDBASE_CACHE_PATH


_LOGGER = logging.getLogger("bedboss")


def create_bedset_from_pep(
    pep: peppy.Project, bedbase_api: str, cache_folder: str = DEFAULT_BEDBASE_CACHE_PATH
) -> BedSet:
    """
    Create bedset from pep file, where sample_name is bed identifier

    :param pep:
    :param bedbase_api:
    :param cache_folder:
    :return:
    """
    new_bedset = BedSet()
    for bedfile_id in pep.samples:
        bedfile_object = BBClient(
            cache_folder=cache_folder,
            bedbase_api=bedbase_api,
        ).load_bed(bedfile_id.sample_name)
        new_bedset.add(bedfile_object)
    return new_bedset


def calculate_bedset_statistics(bbc: BedBaseConf, bedset: BedSet) -> dict:
    """
    Calculate mean and standard deviation for each numeric column of bedfiles in bedset

    :param bbc: BedBase configuration object
    :param bedset: Bedset object
    :return: dict with mean and standard deviation for each
        {"sd": {"column_name": sd_value},
         "mean": {"column_name": mean_value}}
    """

    numeric_columns = [
        column
        for column, value in bbc.bed.result_schemas.items()
        if value["type"] == "number"
    ]
    list_of_samples = [sample.identifier for sample in bedset]

    results_dict = {"mean": {}, "sd": {}}

    for column_name in numeric_columns:
        with bbc.bed.backend.session as s:
            mean_bedset_statement = select(
                func.round(
                    func.avg(getattr(bbc.BedfileORM, column_name)).cast(Numeric), 4
                ).cast(Float)
            ).where(bbc.BedfileORM.record_identifier.in_(list_of_samples))
            sd_bedset_statement = select(
                func.round(
                    func.stddev(getattr(bbc.BedfileORM, column_name)).cast(Numeric), 4
                ).cast(Float)
            ).where(bbc.BedfileORM.record_identifier.in_(list_of_samples))

            results_dict["mean"][column_name] = s.exec(mean_bedset_statement).one()
            results_dict["sd"][column_name] = s.exec(sd_bedset_statement).one()

    return results_dict

    # # Another way to do it, but it's slower:
    # results_dict = {}
    # results = bbc.bed.retrieve(record_identifier=list_of_samples, result_identifier=int_col)["records"]
    # for sample in results:
    #     for stat_value_dict in sample.values():
    #         for key, value in stat_value_dict.items():
    #             if key in results_dict:
    #                 results_dict[key].append(value)
    #             else:
    #                 results_dict[key] = [value]


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
    bedset_name: str,
) -> dict:
    """
    Create plots for a bedset (commonality region plot)

    :param bbc: BedBaseConf object
    :param bedset: Bedset object
    :param bedset_name: bed_set name
    :return: dict with information about crated plots
    """
    bedset_md5sum = bedset.bedset_identifier

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
    return bedset_summary_info["plots"][0]


def add_bedset_to_database(
    bbc: BedBaseConf,
    record_id: str,
    bed_set: BedSet,
    bedset_name: str,
    genome: dict = None,
    description: str = None,
    heavy: bool = False,
) -> None:
    """
    Add bedset to the database

    :param bbc: BedBaseConf object
    :param record_id: record identifier to be used in database
    :param bed_set: Bedset object
    :param bedset_name: Bedset name
    :param genome: genome of the bedset
    :param description: Bedset description
    :param heavy: whether to use heavy processing (add all columns to the database).
        if False -> R-script won't be executed, only basic statistics will be calculated
    :return:
    """
    if not bedset_name:
        raise ValueError(
            "bedset_name was not provided correctly. Please provide it in pep name or as argument"
        )

    bed_set_stats = calculate_bedset_statistics(bbc, bed_set)
    result_dict = {
        "name": bedset_name,
        "md5sum": bed_set.bedset_identifier,
        "description": description,
        "genome": genome,
        "bedset_standard_deviation": bed_set_stats["sd"],
        "bedset_means": bed_set_stats["mean"],
        "processed": heavy,
    }

    if heavy:
        plot_value = create_plots(bbc, bedset=bed_set, bedset_name=record_id)
        result_dict["region_commonality"] = plot_value
    else:
        _LOGGER.warning("Heavy processing is False. Plots won't be calculated")

    bbc.bedset.report(
        record_identifier=record_id,
        values=result_dict,
        force_overwrite=True,
    )
    for sample in bed_set:
        bbc.report_relationship(record_id, sample.identifier)


def run_bedbuncher(
    bedbase_config: str,
    bedset_pep: str,
    bedset_name: str = None,
    bedbase_api: str = DEFAULT_BEDBASE_API_URL,
    cache_path: str = DEFAULT_BEDBASE_CACHE_PATH,
    heavy: bool = False,
    *args,
    **kwargs,
) -> None:
    """
    Create bedset using file with a list of bedfiles

    :param bedbase_config: bed base configuration file path
    :param bedset_name: name of the bedset, can be provided here or as pep name
    :param bedset_pep: bedset pep path or pephub registry path containing bedset pep
    :param bedbase_api: bedbase api url [DEFAULT: http://localhost:8000/api]
    :param cache_path: path to the cache folder [DEFAULT: ./bedbase_cache]
    :param heavy: whether to use heavy processing (add all columns to the database).
        if False -> R-script won't be executed, only basic statistics will be calculated
    :return: None
    """

    bbc = BedBaseConf(bedbase_config)
    if is_registry_path(bedset_pep):
        pep_of_bed = pephubclient.PEPHubClient().load_project(bedset_pep)
        bedset_record_id = bedset_pep
    else:
        pep_of_bed = peppy.Project(bedset_pep)
        bedset_record_id = os.path.basename(bedset_pep)

    bedset = create_bedset_from_pep(
        pep=pep_of_bed, bedbase_api=bedbase_api, cache_folder=cache_path
    )

    if not pep_of_bed.config.get("genome"):
        _LOGGER.warning(
            f"Genome for bedset {bedset_name or pep_of_bed.get('name')} was not provided."
        )
    if not pep_of_bed.get("description"):
        _LOGGER.warning(
            f"Description for bedset {bedset_name or pep_of_bed.get('name')} was not provided."
        )

    add_bedset_to_database(
        bbc,
        record_id=bedset_record_id,
        bed_set=bedset,
        bedset_name=bedset_name or pep_of_bed.get("name"),
        genome=dict(pep_of_bed.config.get("genome", {})),
        description=pep_of_bed.description or "",
        heavy=heavy,
    )
    _LOGGER.info(
        f"bedset {bedset_name or pep_of_bed.get('name')} was added successfully to the database"
    )
    return None
