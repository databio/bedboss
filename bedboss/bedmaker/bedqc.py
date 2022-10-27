#!/usr/bin/env python3

from argparse import ArgumentParser
import os
import sys
import tempfile
import pypiper
import subprocess
import logmuse

from .const import MAX_FILE_SIZE, MAX_REGION_SIZE, MIN_REGION_WIDTH

_LOGGER = logmuse.init_logger(name="bedqc")


def _get_file_size(file_name: str) -> int:
    """
    Get file in size in given unit like KB, MB or GB
    :param file_name: path to the bed file
    :return: size of the file
    """
    size = os.path.getsize(file_name)
    return size


def run_bedqc(
    bedfile: str,
    outfolder: str,
    max_file_size: int = MAX_FILE_SIZE,
    max_region_size: int = MAX_REGION_SIZE,
    min_region_width: int = MIN_REGION_WIDTH,
) -> list:
    """
    Main pipeline function
    :param bedfile: path to the bed file
    :param outfolder: path to the folder where store information about pipeline
    :param max_file_size: maximum file size
    :param max_region_size: maximum region size
    :param min_region_width: min region width
    :return: True if file passed Quality check
    """
    output_file = os.path.join(outfolder, "flagged_bed.csv")
    bedfile_name = os.path.basename(bedfile)
    input_extension = os.path.splitext(bedfile_name)[1]

    pm = pypiper.PipelineManager(name="bedQC-pipeline", outfolder=outfolder)

    detail = []

    # check number of regions
    # run script
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bedmaker",
        "est_line.sh",
    )
    assert os.path.exists(script_path), FileNotFoundError(
        f"'{script_path}' script not found"
    )

    if input_extension == ".gz":
        file = os.path.join(outfolder, next(tempfile._get_candidate_names()))
    else:
        file = bedfile

    pm.clean_add(file)
    cmd = "zcat " + bedfile + " > " + file
    pm.run(cmd, file)

    cmd = f"bash {script_path} {file} "

    if pm.run(cmd, lock_name=next(tempfile._get_candidate_names())) > max_region_size:
        detail.append("File contains more than 5 million regions.")

    # check file size
    if _get_file_size(bedfile) >= max_file_size:
        detail.append("File size is larger than 2G.")

    # check mean region widith
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
        print("file_name: ", bedfile_name)
        if os.path.exists(output_file):
            with open(output_file, "a") as f:
                f.write(f"{bedfile_name}\t{detail} \n")
        else:
            with open(output_file, "w") as f:
                f.write(f"file_name\tdetail \n")
                f.write(f"{bedfile_name}\t{detail} \n")
    pm.stop_pipeline()

    return detail


class QualityException(Exception):
    """Exception, when quoality of the bed file didn't pass."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: reason why quality control wasn't successful
        """
        super(QualityException, self).__init__(reason)


def main():
    """
    Execution bedqc from cmd
    """
    args = _parse_cmdl(sys.argv[1:])
    bedfile = args.bedfile
    outfolder = args.outfolder

    run_bedqc(bedfile, outfolder)


def _parse_cmdl(cmdl):
    """
    parser
    """
    parser = ArgumentParser(description="A pipeline for bed file QC.")

    parser.add_argument(
        "--bedfile", help="a full path to bed file to process", required=True
    )
    parser.add_argument(
        "--outfolder", help="a full path to output folder", required=True
    )

    parser = pypiper.add_pypiper_args(
        parser, groups=["pypiper", "common", "looper", "ngs"]
    )

    return parser.parse_args(cmdl)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
