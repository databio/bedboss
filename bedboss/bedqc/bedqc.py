import logging
import os
import subprocess
import tempfile

import pypiper

from bedboss.const import MAX_FILE_SIZE, MAX_REGION_NUMBER, MIN_REGION_WIDTH
from bedboss.exceptions import QualityException

_LOGGER = logging.getLogger("bedboss")


def bedqc(
    bedfile: str,
    outfolder: str,
    max_file_size: int = MAX_FILE_SIZE,
    max_region_number: int = MAX_REGION_NUMBER,
    min_region_width: int = MIN_REGION_WIDTH,
    pm: pypiper.PipelineManager = None,
) -> bool:
    """
    Perform quality checks on a BED file.

    :param bedfile: path to the bed file
    :param outfolder: path to the folder where to store information about pipeline and logs
    :param max_file_size: Maximum file size threshold to pass the quality check.
    :param max_region_number: Maximum number of regions threshold to pass the quality check.
    :param min_region_width: Minimum region width threshold to pass the quality check.
    :param pm: Pypiper object for managing pipeline operations.
    :return: True if the file passes the quality check.
    :raises QualityException: if the file does not pass the quality
    """
    _LOGGER.info("Running bedqc...")

    output_file = os.path.join(outfolder, "failed_qc.csv")
    bedfile_name = os.path.basename(bedfile)
    input_extension = os.path.splitext(bedfile_name)[1]

    # file_exists = os.path.isfile(bedfile)
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)

    # to execute bedqc from inside Python (without using cli) Pypiper is set to default:
    if not pm:
        pm = pypiper.PipelineManager(
            name="bedQC-pipeline", outfolder=outfolder, recover=True, multi=True
        )

    detail = []

    # check number of regions
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bedqc",
        "est_line.sh",
    )
    assert os.path.exists(script_path), FileNotFoundError(
        f"'{script_path}' script not found"
    )

    if input_extension == ".gz":
        file = os.path.join(outfolder, next(tempfile._get_candidate_names()))
        pm.clean_add(file)
        cmd = "zcat " + bedfile + " > " + file
        pm.run(cmd, file)
    else:
        file = bedfile

    cmd = f"bash {script_path} {file} "

    if int(pm.checkprint(cmd)) > max_region_number:
        detail.append("File contains more than 5 million regions.")

    # check file size
    if os.path.getsize(bedfile) >= max_file_size:
        detail.append("File size is larger than 2G.")

    # check mean region width
    awk_command = r"""
    {
    diff += $3 - $2
    }
    END {
    print diff/NR
    }
    """

    if (
        float(subprocess.check_output(["awk", awk_command, file], text=True).split()[0])
        < min_region_width
    ):
        detail.append(f"Mean region width is less than {min_region_width} bp.")

    if len(detail) > 0:
        _LOGGER.info("file_name: ", bedfile_name)
        if os.path.exists(output_file):
            with open(output_file, "a") as f:
                f.write(f"{bedfile_name}\t{detail} \n")
        else:
            with open(output_file, "w") as f:
                f.write("file_name\tdetail \n")
                f.write(f"{bedfile_name}\t{detail} \n")

        raise QualityException(f"{str(detail)}")

    _LOGGER.info(f"File ({file}) has passed Quality Control!")
    return True
