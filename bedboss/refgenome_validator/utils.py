from typing import Union, List
from geniml.io import RegionSet
import subprocess
import logging

_LOGGER = logging.getLogger("bedboss")


def get_bed_chrom_info(bedfile: Union[str, RegionSet]) -> dict:
    """
    Determine chrom lengths for bed file

    :param bedfile: RegionSet object or path to bed file
    returns dict: returns dictionary where keys are chrom names and values are the max end position of that chromosome.
    """
    if isinstance(bedfile, RegionSet):
        df = bedfile.to_pandas()
    else:
        df = RegionSet(bedfile).to_pandas()

    max_end_for_each_chrom = df.groupby(0)[2].max()
    return max_end_for_each_chrom.to_dict()


def run_igd_command(command):
    """
    Run IGD via a subprocess, this is a temp implementation until Rust IGD python bindings are finished.
    """

    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout
    else:
        _LOGGER.error(f"Error running command: {result.stderr}")
        return None


def parse_IGD_output(output_str) -> Union[None, List[dict]]:
    """
    Parses IGD terminal output into a list of dicts
    Args:
      output_str: The output string from IGD

    Returns:
      A list of dictionaries, where each dictionary represents a record.
    """

    try:
        lines = output_str.splitlines()
        data = []
        for line in lines:
            if line.startswith("index"):
                continue  # Skip the header line
            elif line.startswith("Total"):
                break  # Stop parsing after the "Total" line
            else:
                fields = line.split()
                record = {
                    "index": int(fields[0]),
                    "number_of_regions": int(fields[1]),
                    "number_of_hits": int(fields[2]),
                    "file_name": fields[3],
                }
                data.append(record)
        return data
    except Exception:
        return None
