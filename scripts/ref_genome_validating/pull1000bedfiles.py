import gzip
import argparse
import logging
import os
import shutil

import pipestat
import pypiper
from typing import Optional

from bedboss.bedclassifier import get_bed_type
from bedboss.exceptions import BedTypeException
from geofetch import Finder, Geofetcher

_LOGGER = logging.getLogger("bedboss")


MAX_BEDFILES = 500
RETRIEVE_BED_FILES = False  # get newer bedfiles?


def main(filter, species):
    """
    1. Pull ~ 1000 files for 3 different species for testing.

    2. Cache the bed files for excluded ranges work.

    """

    if not filter or not species:
        print("Must supply filter!")

    else:
        print("Hello World")
        # Make sure to have the IDE ignore these folders!!!!

        data_output_path = os.path.abspath("data")
        results_path = os.path.abspath("results")
        logs_dir = os.path.join(results_path, "logs")

        # print(data_output_path,results_path, logs_dir)

        # Homo Sapiens

        # filter = "\.(bed|narrowPeak|broadPeak)\."  #  regex filter
        # human_filter = "((bed) OR narrow peak) AND Homo sapiens[Organism]"
        # human_data_path = os.path.join(data_output_path, "homosapiens")
        # pull_1000_bedfiles(filter=human_filter, data_output_path=human_data_path)
        species_output_path = os.path.join(data_output_path, species)
        pull_1000_bedfiles(filter=filter, data_output_path=species_output_path)


def pull_1000_bedfiles(filter, data_output_path):
    # make directory if it does not exist
    try:
        os.makedirs(data_output_path, exist_ok=True)
    except OSError:
        print(f"Directory already exists, skipping...")

    print(filter)
    print(data_output_path)

    # Generate bed file list
    gse_obj = Finder(filters=filter, retmax=MAX_BEDFILES)
    gse_list = gse_obj.get_gse_by_date(
        start_date="2016/08/01", end_date="2020/08/01"
    )  #  1095 is three years worth
    text_file_path = os.path.join(data_output_path, "bedfileslist.txt")
    gse_obj.generate_file(text_file_path)

    # geofetcher_obj = Geofetcher(
    #     filter_size="25MB",
    #     data_source="samples",
    #     geo_folder=data_output_path,
    #     metadata_folder=data_output_path,
    #     processed=True,
    #     max_soft_size="20MB",
    #     discard_soft=True,
    # )
    #
    # geofetched = geofetcher_obj.get_projects(
    #     input=os.path.join(data_output_path, "bedfileslist.txt"), just_metadata=False
    # )

    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull 1000 bedfiles")
    parser.add_argument("-f", "--filter", type=str, required=True, help="Filter string")
    parser.add_argument("-s", "--species", type=str, required=True, help="species")
    args = parser.parse_args()
    main(args.filter, args.species)
