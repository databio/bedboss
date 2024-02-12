import gzip
import logging
import os
import shutil
from typing import Optional

import pypiper
import pandas as pd

from bedboss.const import STANDARD_CHROM_LIST

_LOGGER = logging.getLogger("bedboss")


class BedClassifier:
    """
    This will take the input of either a .bed or a .bed.gz and classify the type of BED file.

    Types:
    BED, BED2 - BED12, narrowPeak, broadPeak
    UnknownType

    """

    def __init__(
        self,
        input_file: str,
        output_dir: Optional[str] = None,
        bed_digest: Optional[str] = None,
        input_type: Optional[str] = None,
        pm: pypiper.PipelineManager = None,
        report_to_database: Optional[bool] = False,
    ):
        # Raise Exception if input_type is given and it is NOT a BED file
        # Raise Exception if the input file cannot be resolved
        self.input_file = input_file
        self.bed_digest = bed_digest
        self.input_type = input_type

        self.abs_bed_path = os.path.abspath(self.input_file)
        self.file_name = os.path.splitext(os.path.basename(self.abs_bed_path))[0]
        self.file_extension = os.path.splitext(self.abs_bed_path)[-1]

        # we need this only if unzipping a file
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(self.abs_bed_path), "temp_processing"
        )
        # Use existing Pipeline Manager or Construct New one
        # Want to use Pipeline Manager to log work AND cleanup unzipped gz files.
        if pm is not None:
            self.pm = pm
        else:
            self.logs_dir = os.path.join(self.output_dir, "logs")
            self.pm = pypiper.PipelineManager(
                name="bedclassifier", outfolder=self.logs_dir, recover=True
            )

        if self.file_extension == ".gz":
            if ".bed" not in self.file_name:
                unzipped_input_file = os.path.join(
                    self.output_dir, self.file_name + ".bed"
                )
            else:
                unzipped_input_file = os.path.join(self.output_dir, self.file_name)

            with gzip.open(self.input_file, "rb") as f_in:
                with open(unzipped_input_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            self.input_file = unzipped_input_file
            self.pm.clean_add(unzipped_input_file)

        bed_type = get_bed_type(self.input_file)

        if self.input_type is not None:
            if bed_type != self.input_type:
                _LOGGER.warning(
                    f"BED file classified as different type than given input: {bed_type} vs {self.input_type}"
                )

        else:
            self.input_file = bed_type


def get_bed_type(bed: str, standard_chrom: Optional[str] = None) -> str:
    """
    get the bed file type (ex. bed3, bed3+n )
    standardize chromosomes if necessary:
    filter the input file to contain only the standard chromosomes,
    remove regions on ChrUn chromosomes

    :param bed: path to the bed file
    :param standard_chrom:
    :return bed type
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

    # Use chunksize to read only a few lines of the BED file (We don't need all of it)
    df = pd.read_csv(bed, sep="\t", header=None, chunksize=4)
    df = df.dropna(axis=1)

    # standardizing chromosome
    # remove regions on ChrUn chromosomes
    if standard_chrom:
        _LOGGER.info("Standardizing chromosomes...")
        df = df[df.loc[:, 0].isin(STANDARD_CHROM_LIST)]
        df.to_csv(bed, compression="gzip", sep="\t", header=False, index=False)

    num_cols = len(df.columns)
    bedtype = 0

    # TODO add logic for narrow and broadpeak
    for col in df:
        if col <= 2:
            if col == 0:
                if df[col].dtype == "O":
                    bedtype += 1
                else:
                    return None
            else:
                if df[col].dtype == "int" and (df[col] >= 0).all():
                    bedtype += 1
                else:
                    return None
        else:
            if col == 3:
                if df[col].dtype == "O":
                    bedtype += 1
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"
            elif col == 4:
                if df[col].dtype == "int" and df[col].between(0, 1000).all():
                    bedtype += 1
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"
            elif col == 5:
                if df[col].isin(["+", "-", "."]).all():
                    bedtype += 1
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"
            elif 6 <= col <= 8:
                if df[col].dtype == "int" and (df[col] >= 0).all():
                    bedtype += 1
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"
            elif col == 9:
                if df[col].dtype == "int":
                    bedtype += 1
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"
            elif col == 10 or col == 11:
                if df[col].str.match(r"^(\d+(,\d+)*)?$").all():
                    bedtype += 1
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"
            else:
                n = num_cols - bedtype
                return f"bed{bedtype}+{n}"
