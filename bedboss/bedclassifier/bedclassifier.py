import logging
from typing import Optional, Union

import pandas as pd

from bedboss.exceptions import BedTypeException
from bedboss.models import BedClassificationOutput, DATA_FORMAT

_LOGGER = logging.getLogger("bedboss")


def get_bed_classification(
    bed: Union[str, pd.DataFrame],
    no_fail: Optional[bool] = True,
) -> BedClassificationOutput:
    """
    Get the BED file classification as a Pydantic object.

    :param bed: path to the bed file OR a dataframe
    :param no_fail: should the function (and pipeline) continue if this function fails to parse BED file
    :return BedClassificationOutput object
    """
    #    column format for bed12
    #    string chrom;       "Reference sequence chromosome or scaffold"
    #    uint   chromStart;  "Start position in chromosome"
    #    uint   chromEnd;    "End position in chromosome"
    #    string name;        "Name of item."
    #    uint score;          "Score (0-1000)"
    #    char[1] strand;     "+ or - for strand"
    #    uint thickStart;   "Start of where display should be thick (start codon)"
    #    uint thickEnd;     "End of where display should be thick (stop codon)"
    #    uint reserved;     "Used as itemRgb as of 2004-11-22"
    #    int blockCount;    "Number of blocks"
    #    int[blockCount] blockSizes; "Comma separated list of block sizes"
    #    int[blockCount] chromStarts; "Start positions relative to chromStart"

    def _read_bed_file(filepath: str, skiprows: int = 0) -> Optional[pd.DataFrame]:
        """
        Helper function to read BED file with error handling.
        :param str file_path: path to the bed file
        :param int skip_rows: how many rows to skip during reading
        :return pd.DataFrame
        """
        try:
            df = pd.read_csv(
                filepath, sep="\t", header=None, low_memory=False, skiprows=skiprows
            )
            if skiprows > 0:
                _LOGGER.info(f"Skipped {skiprows} rows to parse bed file {filepath}")
            return df
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(
                    filepath,
                    sep="\t",
                    header=None,
                    nrows=4,
                    skiprows=skiprows,
                    encoding="utf-16",
                )
                if skiprows > 0:
                    _LOGGER.info(
                        f"Skipped {skiprows} rows to parse bed file {filepath}"
                    )
                return df
            except (pd.errors.ParserError, pd.errors.EmptyDataError):
                return None
        except (pd.errors.ParserError, pd.errors.EmptyDataError):
            return None

    if isinstance(bed, str):
        max_rows = 5
        for row_count in range(max_rows + 1):
            df = _read_bed_file(bed, row_count)
            if df is not None:
                break
        else:
            if no_fail:
                _LOGGER.warning(
                    f"Unable to parse bed file {bed}, setting data_format = unknown_data_format"
                )
                return BedClassificationOutput(
                    bed_compliance="unknown_bed_compliance",
                    data_format="unknown_data_format",
                    compliant_columns=0,
                    non_compliant_columns=0,
                )
            else:
                raise BedTypeException(
                    reason=f"Data format could not be determined for {bed}"
                )
    elif isinstance(bed, pd.DataFrame):
        df = bed
    else:
        if no_fail:
            return BedClassificationOutput(
                bed_compliance="unknown_bed_compliance",
                data_format="unknown_data_format",
                compliant_columns=0,
                non_compliant_columns=0,
            )
        else:
            raise BedTypeException(reason="Input is not a string or dataframe.")

    df = df.dropna(axis=1)
    num_cols = len(df.columns)
    compliant_columns = 0
    bed_format_named = DATA_FORMAT.UCSC_BED
    relaxed = False

    def _check_column(col_index: int, checks: list) -> bool:
        """
        Helper function to perform column checks.

        :param col_index: index of the column
        :param checks: list of check functions

        :return: True if all checks pass, False otherwise

        """
        for check in checks:
            if not check(df[col_index]):
                return False
        return True

    # regex patterns for 255,255,255 or 0 for colors: ([0, 255], [0, 255], [0, 255]) | 0
    REGEX_COLORS = r"^(?:\d|[1-9]\d|1\d{2}|2[0-4]\d|25[0-5])(?:,(?:\d|[1-9]\d|1\d{2}|2[0-4]\d|25[0-5])){0,2}$"

    column_checks = {
        0: [lambda col: col.astype(str).str.match(r"[A-Za-z0-9_]{1,255}").all()],
        1: [lambda col: col.dtype == "int" and (col >= 0).all()],
        2: [lambda col: col.dtype == "int" and (col >= 0).all()],
        3: [lambda col: col.astype(str).str.match(r"[\x20-\x7e]{1,255}").all()],
        4: [
            lambda col: col.dtype == "int" and col.between(0, 1000).all(),
        ],
        5: [lambda col: col.isin(["+", "-", "."]).all()],
        6: [lambda col: col.dtype == "int" and (col >= 0).all()],
        7: [lambda col: col.dtype == "int" and (col >= 0).all()],
        8: [lambda col: col.astype(str).str.match(REGEX_COLORS).all()],
        9: [lambda col: col.dtype == "int"],
        10: [
            lambda col: col.astype(str).str.match(r"^(0(,\d+)*|\d+(,\d+)*)?,?$").all()
        ],
        11: [
            lambda col: col.astype(str).str.match(r"^(0(,\d+)*|\d+(,\d+)*)?,?$").all()
        ],
        12: [lambda col: pd.api.types.is_float_dtype(col.dtype) or (col == -1).all()],
        13: [lambda col: col.dtype == "int" and col.iloc[0] != -1],
    }

    for col_index in range(num_cols):
        checks = column_checks.get(col_index, [])
        if _check_column(col_index, checks) and col_index < 12:
            compliant_columns += 1

        elif (
            col_index == 4 and df[col_index].dtype == "int" and df[col_index].all() >= 0
        ):

            compliant_columns += 1
            relaxed = True

        else:
            nccols = num_cols - compliant_columns
            if col_index >= 6:
                if (
                    num_cols == 10
                    and col_index == 6
                    and _check_column(6, column_checks[12])
                    and _check_column(7, column_checks[12])
                    and _check_column(8, column_checks[12])
                    and _check_column(9, column_checks[9])
                ):
                    bed_format_named = (
                        DATA_FORMAT.ENCODE_NARROWPEAK_RS
                        if relaxed
                        else DATA_FORMAT.ENCODE_NARROWPEAK
                    )
                    return BedClassificationOutput(
                        bed_compliance=f"bed{compliant_columns}+{nccols}",
                        data_format=bed_format_named,
                        compliant_columns=compliant_columns,
                        non_compliant_columns=nccols,
                    )
                elif num_cols == 9 and col_index == 6:
                    if (
                        _check_column(6, column_checks[12])
                        and _check_column(7, column_checks[12])
                        and _check_column(8, column_checks[12])
                    ):
                        bed_format_named = (
                            DATA_FORMAT.ENCODE_BROADPEAK_RS
                            if relaxed
                            else DATA_FORMAT.ENCODE_BROADPEAK
                        )
                        return BedClassificationOutput(
                            bed_compliance=f"bed{compliant_columns}+{nccols}",
                            data_format=bed_format_named,
                            compliant_columns=compliant_columns,
                            non_compliant_columns=nccols,
                        )
                    elif (
                        _check_column(6, column_checks[12])
                        and _check_column(7, column_checks[12])
                        and _check_column(8, column_checks[13])
                    ):
                        bed_format_named = (
                            DATA_FORMAT.ENCODE_RNA_ELEMENTS_RS
                            if relaxed
                            else DATA_FORMAT.ENCODE_RNA_ELEMENTS
                        )
                        return BedClassificationOutput(
                            bed_compliance=f"bed{compliant_columns}+{nccols}",
                            data_format=bed_format_named,
                            compliant_columns=compliant_columns,
                            non_compliant_columns=nccols,
                        )
                elif (
                    num_cols == 15
                    and col_index == 12
                    and _check_column(12, column_checks[12])
                    and _check_column(13, column_checks[12])
                    and _check_column(14, column_checks[12])
                ):
                    bed_format_named = (
                        DATA_FORMAT.ENCODE_GAPPEDPEAK_RS
                        if relaxed
                        else DATA_FORMAT.ENCODE_GAPPEDPEAK
                    )
                    return BedClassificationOutput(
                        bed_compliance=f"bed{compliant_columns}+{nccols}",
                        data_format=bed_format_named,
                        compliant_columns=compliant_columns,
                        non_compliant_columns=nccols,
                    )
            bed_format_named = (
                DATA_FORMAT.BED_LIKE_RS if relaxed else DATA_FORMAT.BED_LIKE
            )
            if relaxed and nccols == 0:
                bed_format_named = DATA_FORMAT.UCSC_BED_RS
            return BedClassificationOutput(
                bed_compliance=f"bed{compliant_columns}+{nccols}",
                data_format=bed_format_named,
                compliant_columns=compliant_columns,
                non_compliant_columns=nccols,
            )

    bed_format_named = DATA_FORMAT.UCSC_BED_RS if relaxed else bed_format_named
    return BedClassificationOutput(
        bed_compliance=f"bed{compliant_columns}+0",
        data_format=bed_format_named,
        compliant_columns=compliant_columns,
        non_compliant_columns=0,
    )
