import logging
import os
import tempfile
import pypiper
import subprocess

from bedboss.const import MAX_FILE_SIZE, MAX_REGION_SIZE, MIN_REGION_WIDTH
from bedboss.exceptions import QualityException


_LOGGER = logging.getLogger("bedboss")


def bedqc(
    bedfile: str,
    outfolder: str,
    max_file_size: int = MAX_FILE_SIZE,
    max_region_size: int = MAX_REGION_SIZE,
    min_region_width: int = MIN_REGION_WIDTH,
    pm: pypiper.PipelineManager = None,
) -> bool:
    """
    Main pipeline function
    :param bedfile: path to the bed file
    :param outfolder: path to the folder where to store information about pipeline and logs
    :param max_file_size: maximum file size
    :param max_region_size: maximum region size
    :param min_region_width: min region width
    :param pm: pypiper object
    :return: True if file passed Quality check
    """
    _LOGGER.info("Running bedqc...")

    output_file = os.path.join(outfolder, "flagged_bed.csv")
    bedfile_name = os.path.basename(bedfile)
    input_extension = os.path.splitext(bedfile_name)[1]

    if not pm:
        pm = pypiper.PipelineManager(name="bedQC-pipeline", outfolder=outfolder, recover=True)

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

    if (
        int(pm.checkprint(cmd))
        > max_region_size
    ):
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
                f.write(f"file_name\tdetail \n")
                f.write(f"{bedfile_name}\t{detail} \n")

        pm.stop_pipeline()
        raise QualityException(f"{str(detail)}")

    pm.stop_pipeline()

    _LOGGER.info(f"File ({file}) has passed Quality Control!")
    return True
