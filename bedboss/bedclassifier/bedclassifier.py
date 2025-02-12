import logging
from typing import Optional, Tuple, Union

import pandas as pd
import pandas.errors

from bedboss.exceptions import BedTypeException

_LOGGER = logging.getLogger("bedboss")


def get_bed_type(bed: Union[str, pd.DataFrame], no_fail: Optional[bool] = True) -> Tuple[str, str]:
    """
    get the bed file type (ex. bed3, bed3+n )
    standardize chromosomes if necessary:
    filter the input file to contain only the standard chromosomes,
    remove regions on ChrUn chromosomes

    :param bed: path to the bed file OR a dataframe
    :param no_fail: should the function (and pipeline) continue if this function fails to parse BED file
    :return bedtype: tuple[option ["bed{bedtype}+{n}", "unknown_bedtype"], option [ucsc_bed, encode_narrowpeak, encode_broadpeak, encode_rna_elements,encode_gappedpeak,unknown_bedtype]]
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
                df = pd.read_csv(bed, sep="\t", header=None, low_memory=False, skiprows=row_count)
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
                        _LOGGER.info(f"Skipped {row_count} rows to parse bed file {bed}")
                    break
                except (pandas.errors.ParserError, pandas.errors.EmptyDataError) as e:
                    if row_count <= max_rows:
                        row_count += 1
                    else:
                        if no_fail:
                            _LOGGER.warning(
                                f"Unable to parse bed file {bed}, due to error {e}, setting bed_type = unknown_bedtype"
                            )
                            return "unknown_bedtype", "unknown_bedtype"
                        else:
                            raise BedTypeException(
                                reason=f"Bed type could not be determined due to CSV parse error {e}"
                            )
            except (pandas.errors.ParserError, pandas.errors.EmptyDataError) as e:
                if row_count <= max_rows:
                    row_count += 1
                else:
                    if no_fail:
                        _LOGGER.warning(
                            f"Unable to parse bed file {bed}, due to error {e}, setting bed_type = unknown_bedtype"
                        )
                        return "unknown_bedtype", "unknown_bedtype"
                    else:
                        raise BedTypeException(
                            reason=f"Bed type could not be determined due to CSV parse error {e}"
                        )
    elif isinstance(bed, pd.DataFrame):
        df = bed

    if df is not None:
        df = df.dropna(axis=1)
        num_cols = len(df.columns)
        bedtype = 0

        bed_type_named = "ucsc_bed"

        for col in df:
            if col <= 2:
                if col == 0:
                    if df[col].dtype == "O":
                        bedtype += 1
                    elif df[col].dtype == "int" or df[col].dtype == "float":
                        bedtype += 1
                    else:
                        if no_fail:
                            _LOGGER.warning(
                                f"Bed type could not be determined at column {0} with data type: {df[col].dtype}"
                            )
                            return "unknown_bedtype", "unknown_bedtype"
                        else:
                            raise BedTypeException(
                                reason=f"Bed type could not be determined at column {0} with data type: {df[col].dtype}"
                            )

                else:
                    if df[col].dtype == "int" and (df[col] >= 0).all():
                        bedtype += 1
                    else:
                        if no_fail:
                            _LOGGER.warning(
                                f"Bed type could not be determined at column {col} with data type: {df[col].dtype}"
                            )
                            return "unknown_bedtype", "unknown_bedtype"
                        else:
                            raise BedTypeException(
                                reason=f"Bed type could not be determined at column 0 with data type: {df[col].dtype}"
                            )
            else:
                if col == 3:
                    if df[col].dtype == "O":
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                elif col == 4:
                    if df[col].dtype == "int" and df[col].between(0, 1000).all():
                        bedtype += 1
                    elif num_cols == 10 and df[col].dtype == "int" and df[col].min() > 0 and all(                            [
                                (df[6].dtype == "float" or df[6][0] == -1),
                                (df[7].dtype == "float" or df[7][0] == -1),
                                (df[8].dtype == "float" or df[8][0] == -1),
                                (df[9].dtype == "int" or df[9][0] == -1),
                            ]):

                            # This might be a narrowPeak with non-standard scores (e.g. greater than 1000)
                            bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                elif col == 5:
                    if df[col].isin(["+", "-", "."]).all():
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                elif 6 <= col <= 8:
                    if df[col].dtype == "int" and (df[col] >= 0).all():
                        bedtype += 1
                    elif num_cols == 10:
                        # This is a catch to see if this is actually a narrowpeak file that is unnamed
                        if col == 6 and all(
                            [
                                (df[col-2].between(0, 1000).all()),
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                                (df[col + 3].dtype == "int" or df[col + 3][0] == -1),
                            ]
                        ):
                            n = num_cols - bedtype
                            bed_type_named = "encode_narrowpeak"
                            return f"bed{bedtype}+{n}", bed_type_named
                        elif col == 6 and all(                            [
                                (df[col-2].min()>0),
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                                (df[col + 3].dtype == "int" or df[col + 3][0] == -1),
                            ]):
                                n = num_cols - bedtype
                                bed_type_named = "ns_narrowpeak"
                                return f"bed{bedtype}+{n}", bed_type_named
                        else:
                            n = num_cols - bedtype
                            return f"bed{bedtype}+{n}", bed_type_named

                    elif num_cols == 9:
                        # This is a catch to see if this is actually a broadpeak file that is unnamed
                        if all(
                            [
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                            ]
                        ):
                            n = num_cols - bedtype
                            bed_type_named = "encode_broadpeak"
                            return f"bed{bedtype}+{n}", bed_type_named

                        elif all(
                            [
                                (df[col].dtype == "float" or df[col][0] == -1),
                                (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                                (df[col + 2].dtype == "int" and df[col + 2][0] != -1),
                            ]
                        ) :
                            n = num_cols - bedtype
                            bed_type_named = "encode_rna_elements"
                            return f"bed{bedtype}+{n}", bed_type_named
                        else:
                            n = num_cols - bedtype
                            return f"bed{bedtype}+{n}", bed_type_named
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                elif col == 9:
                    if df[col].dtype == "int":
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                elif col == 10 or col == 11:
                    if df[col].str.match(r"^(\d+(,\d+)*)?$").all():
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                elif 12 <= col <= 14:
                    if col == 12 and num_cols == 15 and all(
                        [
                            (df[col].dtype == "float" or df[col][0] == -1),
                            (df[col + 1].dtype == "float" or df[col + 1][0] == -1),
                            (df[col + 2].dtype == "float" or df[col + 2][0] == -1),
                        ]
                    ):
                        n = num_cols - bedtype
                        bed_type_named = "encode_gappedpeak"
                        return f"bed{bedtype}+{n}", bed_type_named
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}", bed_type_named
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}", bed_type_named

        # This is to catch any files that are assigned a bed number but don't adhere to the above conditions
        return f"bed{bedtype}+0", bed_type_named

    else:
        return "unknown_bedtype", "unknown_bedtype"
