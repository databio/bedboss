import logging
from typing import List
from bbconf import BedBaseConf
from geniml.bbclient import BBClient
from geniml.region2vec import Region2VecExModel

from bedboss.const import DEFAULT_BEDBASE_API_URL

_LOGGER = logging.getLogger("bedboss")


def get_unindexed_bed_files(bbc: BedBaseConf) -> List[str]:
    """
    Get list of unindexed bed files from the bedbase
    :return: list of record_identifiers of unindexed bed files
    """
    result_list = bbc.bed.backend.select_txt(
        columns=["record_identifier"],
        filter_templ="""added_to_qdrant = false and (genome->>'alias') = 'hg38'""",
    )
    return [result[0] for result in result_list]


def add_to_qdrant(
    bedbase_config: str,
    bedbase_api: str = DEFAULT_BEDBASE_API_URL,
    **kwargs,
) -> None:
    """
    Add unindexed bed files to qdrant

    :param bedbase_config: path to the bedbase configuration file
    :param bedbase_api: URL of the Bedbase API
    :return: None
    """
    # get list of bed files
    bbc = BedBaseConf(config_path=bedbase_config)
    list_of_record_ids = get_unindexed_bed_files(bbc)

    if len(list_of_record_ids) == 0:
        _LOGGER.info("No unindexed bed files found")
        return None

    region_to_vec_obj = Region2VecExModel("databio/r2v-ChIP-atlas-hg38")

    for record_id in list_of_record_ids:
        bedfile_object = BBClient(
            cache_folder="~/bedbase_cache", bedbase_api=bedbase_api
        ).load_bed(record_id)

        bbc.add_bed_to_qdrant(
            bed_id=record_id,
            bed_file=bedfile_object,
            payload={"description": "test"},
            region_to_vec=region_to_vec_obj,
        )

        bbc.bed.report(
            record_identifier=record_id,
            values={"added_to_qdrant": True},
            force_overwrite=True,
        )

    return None
