from typing import Union
import pandas as pd
from geniml.io.utils import is_gzipped
import logging


_LOGGER = logging.getLogger("bedboss")


def _read_gzipped_file(file_path: str) -> pd.DataFrame:
    """
    !! Copy from geniml!
    Read a gzipped file into a pandas dataframe

    :param file_path: path to gzipped file
    :return: pandas dataframe
    """
    return _read_file_pd(
        file_path,
        sep="\t",
        compression="gzip",
        header=None,
        engine="pyarrow",
    )


def _read_file_pd(*args, **kwargs) -> pd.DataFrame:
    """
    !! Copy from geniml!
    Read bed file into a pandas DataFrame, and skip header rows if needed

    :return: pandas dataframe
    """
    max_rows = 5
    row_count = 0
    while row_count <= max_rows:
        try:
            df = pd.read_csv(*args, **kwargs, skiprows=row_count)
            if row_count > 0:
                _LOGGER.info(
                    f"Skipped {row_count} rows while standardization. File: '{args}'"
                )
            df = df.dropna(axis=1)
            return df
        except (pd.errors.ParserError, pd.errors.EmptyDataError) as _:
            if row_count <= max_rows:
                row_count += 1
        # if can't open file after 5 attempts try to open it with gzip
    return _read_gzipped_file(*args)


def get_bed_chrom_info(bedfile: str) -> dict:
    """
    Attempt to open it and read it to find all of the chromosomes and the max length of each.

    :param bedfile: bedfilepath
    returns dict: returns dictionary where keys are chrom names and values are the max end position of that chromosome.
    """
    if is_gzipped(bedfile):
        df = _read_gzipped_file(bedfile)
    else:
        df = _read_file_pd(bedfile, sep="\t", header=None, engine="pyarrow")

    max_end_for_each_chrom = df.groupby(0)[2].max()
    return max_end_for_each_chrom.to_dict()
