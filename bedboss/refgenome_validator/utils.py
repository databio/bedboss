from typing import Union
from geniml.io import RegionSet
import logging

_LOGGER = logging.getLogger("bedboss")


def get_bed_chrom_info(bedfile: Union[str, RegionSet]) -> dict:
    """
    Open bed file and find all of the chromosomes and the max length of each.

    :param bedfile: RegionSet object or path to bed file
    returns dict: returns dictionary where keys are chrom names and values are the max end position of that chromosome.
    """
    if isinstance(bedfile, RegionSet):
        df = bedfile.to_pandas()
    else:
        df = RegionSet(bedfile).to_pandas()

    max_end_for_each_chrom = df.groupby(0)[2].max()
    return max_end_for_each_chrom.to_dict()
