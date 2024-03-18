import logging
from typing import List
from bbconf import BedBaseConf
from pipestat.const import RECORD_IDENTIFIER

from geniml.bbclient import BBClient
from geniml.region2vec import Region2VecExModel

_LOGGER = logging.getLogger("bedboss")

AVAILABLE_GENOMES = ["hg38"]
DEFAULT_GENOME = "hg38"

# TODO: should be moved to the bedbase configuration?


def get_unindexed_bed_files(bbc: BedBaseConf, genome: str = DEFAULT_GENOME) -> List[str]:
    """
    Get list of unindexed bed files from the bedbase

    :param BedBaseConf bbc: bedbase configuration
    :param str genome: genome assembly to be indexed
    :return: list of record_identifiers of unindexed bed files
    """
    result_list = bbc.bed.select_records(
        columns=[RECORD_IDENTIFIER],
        filter_conditions=[
            {"key": ["upload_status", "qdrant"], "operator": "eq", "value": False},
            {"key": ["genome", "alias"], "operator": "eq", "value": genome},
        ],
    )
    return [result.get(RECORD_IDENTIFIER) for result in result_list["records"]]


def add_to_qdrant(
    bedbase_config: str,
    genome: str = "hg38",
) -> None:
    """
    Add unindexed bed files to qdrant

    :param str bedbase_config: path to the bedbase configuration file
    :param str bedbase_api: URL of the Bedbase API
    :return: None
    """

    agent = BedBaseConf(config_path=bedbase_config)
    list_of_record_ids = get_unindexed_bed_files(agent, genome=genome)

    if len(list_of_record_ids) == 0:
        _LOGGER.info("No unindexed bed files found")
        return None

    region_to_vec_obj = Region2VecExModel(agent.region2vec_model)

    bb_client = BBClient()

    for record_id in list_of_record_ids:
        bedfile_object = bb_client.load_bed(record_id)

        agent.add_bed_to_qdrant(
            bed_id=record_id,
            bed_file=bedfile_object,
            payload={"description": "test"},
            region_to_vec=region_to_vec_obj,
        )

    return None

if __name__ == "__main__":
    add_to_qdrant("/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml", genome="hg19")