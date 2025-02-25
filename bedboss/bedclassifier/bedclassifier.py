import logging
from typing import Optional, Tuple, Union

import pandas as pd
import pandas.errors

from bedboss.exceptions import BedTypeException

_LOGGER = logging.getLogger("bedboss")


def get_bed_classification(
    bed: Union[str, pd.DataFrame],
    no_fail: Optional[bool] = True,
    strict_score: Optional[bool] = True,
) -> Tuple[str, str, int, int]:
    """
    Get the BED file classification as a tuple (bed_compliance, data_format) e.g. (bed6+4, encode_narrowpeak)

    :param bed: path to the bed file OR a dataframe
    :param no_fail: should the function (and pipeline) continue if this function fails to parse BED file
    :param strict_score: defaults to True which applies strict score specification where scores must be between 0 and 1000.
    :return bed_compliance: tuple[option ["bed{bedtype}+{n}", "unknown_data_format", compliant_columns, nccols], option [ucsc_bed, encode_narrowpeak, encode_broadpeak, encode_rna_elements, encode_gappedpeak, unknown_data_format]]
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

    df = None

    max_rows = 5
    row_count = 0

    if isinstance(bed, str):
        while row_count <= max_rows:
            try:
                df = pd.read_csv(
                    bed, sep="\t", header=None, low_memory=False, skiprows=row_count
                )
                if row_count > 0:
                    _LOGGER.info(f"Skipped {row_count} rows to parse bed file {bed}")
                break
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(
                        bed,
                        sep="\t",
                        header=None,
                        nrows=4,
                        skiprows=row_count,
                        encoding="utf-16",
                    )
                    if row_count > 0:
                        _LOGGER.info(
                            f"Skipped {row_count} rows to parse bed file {bed}"
                        )
                    break
                except (pandas.errors.ParserError, pandas.errors.EmptyDataError) as e:
                    if row_count <= max_rows:
                        row_count += 1
                    else:
                        if no_fail:
                            _LOGGER.warning(
                                f"Unable to parse file {bed}, due to error {e}, setting data_format = unknown_data_format"
                            )
                            return (
                                "unknown_bed_compliance",
                                "unknown_data_format",
                                0,
                                0,
                            )
                        else:
                            raise BedTypeException(
                                reason=f"Data format could not be determined due to CSV parse error {e}"
                            )
            except (pandas.errors.ParserError, pandas.errors.EmptyDataError) as e:
                if row_count <= max_rows:
                    row_count += 1
                else:
                    if no_fail:
                        _LOGGER.warning(
                            f"Unable to parse bed file {bed}, due to error {e}, setting data_format = unknown_data_format"
                        )
                        return (
                            "unknown_bed_compliance",
                            "unknown_data_format",
                            0,
                            0,
                        )
                    else:
                        raise BedTypeException(
                            reason=f"Data format could not be determined due to CSV parse error {e}"
                        )
    elif isinstance(bed, pd.DataFrame):
        df = bed

    if df is not None:
        df = df.dropna(axis=1)
        num_cols = len(df.columns)
        compliant_columns = 0

        bed_format_named = "ucsc_bed"
        relaxed = False

        for col in df:
            if col <= 2:
                if col == 0:
                    if df[col].dtype == "O":
                        compliant_columns += 1
                    elif df[col].dtype == "int" or df[col].dtype == "float":
                        compliant_columns += 1
                    else:
                        if no_fail:
                            _LOGGER.warning(
                                f"Data format could not be determined at column {0} with data type: {df[col].dtype}"
                            )
                            return (
                                "unknown_bed_compliance",
                                "unknown_data_format",
                                0,
                                0,
                            )
                        else:
                            raise BedTypeException(
                                reason=f"Data format could not be determined at column {0} with data type: {df[col].dtype}"
                            )

                else:
                    if df[col].dtype == "int" and (df[col] >= 0).all():
                        compliant_columns += 1
                    else:
                        if no_fail:
                            _LOGGER.warning(
                                f"Data format could not be determined at column {col} with data type: {df[col].dtype}"
                            )
                            return (
                                "unknown_bed_compliance",
                                "unknown_data_format",
                                0,
                                0,
                            )
                        else:
                            raise BedTypeException(
                                reason=f"Data format could not be determined at column 0 with data type: {df[col].dtype}"
                            )
            else:
                if col == 3:
                    if df[col].dtype == "O":
                        compliant_columns += 1
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0:
                            bed_format_named = "bed_like"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                elif col == 4:
                    if df[col].dtype == "int" and df[col].between(0, 1000).all():
                        compliant_columns += 1
                    elif df[col].dtype == "int" and df[col].all() >= 0:
                        compliant_columns += 1
                        relaxed = True
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0:
                            bed_format_named = "bed_like"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                elif col == 5:
                    if df[col].isin(["+", "-", "."]).all():
                        compliant_columns += 1
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0 and relaxed:
                            bed_format_named = "bed_like_rs"
                        elif relaxed and nccols == 0:
                            bed_format_named = "bed_rs"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                elif 6 <= col <= 8:
                    if df[col].dtype == "int" and (df[col] >= 0).all():
                        compliant_columns += 1
                    elif num_cols == 10:
                        # This is a catch to see if this is actually a narrowpeak file that is unnamed
                        if col == 6 and all(
                            [
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                                (df[col + 3].dtype == "int" or df[col + 3][0] == -1),
                            ]
                        ):
                            nccols = num_cols - compliant_columns
                            if relaxed:
                                bed_format_named = "encode_narrowpeak_rs"
                            else:
                                bed_format_named = "encode_narrowpeak"
                            return (
                                f"bed{compliant_columns}+{nccols}",
                                bed_format_named,
                                compliant_columns,
                                nccols,
                            )
                        else:
                            nccols = num_cols - compliant_columns
                            if nccols > 0 and relaxed:
                                bed_format_named = "bed_like_rs"
                            elif relaxed and nccols == 0:
                                bed_format_named = "bed_rs"
                            return (
                                f"bed{compliant_columns}+{nccols}",
                                bed_format_named,
                                compliant_columns,
                                nccols,
                            )

                    elif num_cols == 9:
                        # This is a catch to see if this is actually a broadpeak file that is unnamed
                        if all(
                            [
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                            ]
                        ):
                            nccols = num_cols - compliant_columns
                            if relaxed:
                                bed_format_named = "encode_broadpeak_rs"
                            else:
                                bed_format_named = "encode_broadpeak"
                            return (
                                f"bed{compliant_columns}+{nccols}",
                                bed_format_named,
                                compliant_columns,
                                nccols,
                            )

                        elif all(
                            [
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "int" and df[col + 2][0] != -1),
                            ]
                        ):
                            nccols = num_cols - compliant_columns
                            if relaxed:
                                bed_format_named = "encode_rna_elements_rs"
                            else:
                                bed_format_named = "encode_rna_elements"
                            return (
                                f"bed{compliant_columns}+{nccols}",
                                bed_format_named,
                                compliant_columns,
                                nccols,
                            )
                        else:
                            nccols = num_cols - compliant_columns
                            if nccols > 0 and relaxed:
                                bed_format_named = "bed_like_rs"
                            elif relaxed and nccols == 0:
                                bed_format_named = "bed_rs"
                            return (
                                f"bed{compliant_columns}+{nccols}",
                                bed_format_named,
                                compliant_columns,
                                nccols,
                            )
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0 and relaxed:
                            bed_format_named = "bed_like_rs"
                        elif relaxed and nccols == 0:
                            bed_format_named = "bed_rs"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                elif col == 9:
                    if df[col].dtype == "int":
                        compliant_columns += 1
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0 and relaxed:
                            bed_format_named = "bed_like_rs"
                        elif relaxed and nccols == 0:
                            bed_format_named = "bed_rs"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                elif col == 10 or col == 11:
                    if df[col].str.match(r"^(0(,\d+)*|\d+(,\d+)*)?,?$").all():
                        compliant_columns += 1
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0 and relaxed:
                            bed_format_named = "bed_like_rs"
                        elif relaxed and nccols == 0:
                            bed_format_named = "bed_rs"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                elif 12 <= col <= 14:
                    if (
                        col == 12
                        and num_cols == 15
                        and all(
                            [
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                            ]
                        )
                    ):
                        nccols = num_cols - compliant_columns
                        if relaxed:
                            bed_format_named = "encode_gappedpeak_rs"
                        else:
                            bed_format_named = "encode_gappedpeak"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                    else:
                        nccols = num_cols - compliant_columns
                        if nccols > 0 and relaxed:
                            bed_format_named = "bed_like_rs"
                        elif relaxed and nccols == 0:
                            bed_format_named = "bed_rs"
                        return (
                            f"bed{compliant_columns}+{nccols}",
                            bed_format_named,
                            compliant_columns,
                            nccols,
                        )
                else:
                    nccols = num_cols - compliant_columns
                    if nccols > 0 and relaxed:
                        bed_format_named = "bed_like_rs"
                    elif relaxed and nccols == 0:
                        bed_format_named = "bed_rs"
                    return (
                        f"bed{compliant_columns}+{nccols}",
                        bed_format_named,
                        compliant_columns,
                        nccols,
                    )

        # This is to catch any files that are assigned a bed number but don't adhere to the above conditions
        return f"bed{compliant_columns}+0", bed_format_named, compliant_columns, 0

    else:
        return "unknown_bed_compliance", "unknown_data_format", 0, 0
