from typing import Optional, Union
import os
import pandas as pd
import subprocess

from bedboss.exceptions import ValidatorException
from bedboss.refgenome_validator import GenomeModel

try:
    IGD_LOCATION = os.environ["IGD_LOCATION"]
except:
    # Local installation of C version of IGD
    IGD_LOCATION = f"/home/drc/GITHUB/igd/IGD/bin/igd"


class Validator:
    """

    This is primary class for creating a compatibility vector
    An object of this class is to be created once and then used for the entirety of a pipeline,e.g. Bedboss.

    """

    def __init__(
        self, genome_models: list[GenomeModel], igd_path: Optional[str] = None
    ):
        """
        Initialization method

        :param list[GenomeModels] genome_models: this is a list of GenomeModels that will be checked against a bed file
        :param str igd_path: path to a local IGD file containing ALL excluded ranges intervals

        """

        if isinstance(genome_models, str):
            genome_models = list(genome_models)
        if not isinstance(genome_models, list):
            raise ValidatorException(
                reason="A list of GenomeModels must be provided to initialize the Validator class"
            )

        self.genome_models = genome_models

        self.igd_path = igd_path

        # this will be a list of dictionary info with length of genome_models
        self.compatibility_list = []

    def calculate_chrom_stats(
        self, bed_chrom_sizes: dict, genome_chrom_sizes: dict
    ) -> dict:
        """
        Given two dicts of chroms (key) and their sizes (values)
        determine overlap and sequence fit

        Calculates Stats associated with comparison of chrom names, chrom lengths, and sequence fits

        :param dict bed_chrom_sizes: dict of a bedfile's chrom size
        :param dict genome_chrom_sizes: dict of a GenomeModel's chrom sizes

        return dict: returns a dictionary with information on Query vs Model, e.g. chrom names QueryvsModel
        """

        # Layer 1: Check names and Determine XS (Extra Sequences) via Calculation of Recall/Sensitivity
        # Q = Query, M = Model
        name_stats = {}
        q_and_m = 0  # how many chrom names are in the query and the genome model?
        q_and_not_m = (
            0  # how many chrom names are in the query but not in the genome model?
        )
        not_q_and_m = (
            0  # how many chrom names are not in the query but are in the genome model?
        )
        passed_chrom_names = True  # does this bed file pass the first layer of testing?

        query_keys_present = []  # These keys are used for seq fit calculation
        for key in list(bed_chrom_sizes.keys()):
            if key not in genome_chrom_sizes:
                q_and_not_m += 1
            if key in genome_chrom_sizes:
                q_and_m += 1
                query_keys_present.append(key)
        for key in list(genome_chrom_sizes.keys()):
            if key not in bed_chrom_sizes:
                not_q_and_m += 1

        # Calculate the Jaccard Index for Chrom Names
        bed_chrom_set = set(list(bed_chrom_sizes.keys()))
        genome_chrom_set = set(list(genome_chrom_sizes.keys()))
        chrom_intersection = bed_chrom_set.intersection(genome_chrom_set)
        chrom_union = bed_chrom_set.union(chrom_intersection)
        chrom_jaccard_index = len(chrom_intersection) / len(chrom_union)

        # Alternative Method for Calculating Jaccard_index for binary classification
        # JI = TP/(TP+FP+FN)
        jaccard_binary = q_and_m / (q_and_m + not_q_and_m + q_and_not_m)

        # What is our threshold for passing layer 1?
        if q_and_not_m > 1:
            passed_chrom_names = False

        # Calculate sensitivity for chrom names
        # defined as XS -> Extra Sequences
        sensitivity = q_and_m / (q_and_m + q_and_not_m)

        # Assign Stats
        name_stats["XS"] = sensitivity
        name_stats["q_and_m"] = q_and_m
        name_stats["q_and_not_m"] = q_and_not_m
        name_stats["not_q_and_m"] = not_q_and_m
        name_stats["jaccard_index"] = chrom_jaccard_index
        name_stats["jaccard_index_binary"] = jaccard_binary
        name_stats["passed_chrom_names"] = passed_chrom_names

        # Layer 2:  Check Lengths, but only if layer 1 is passing
        if passed_chrom_names:
            length_stats = {}

            chroms_beyond_range = False
            num_of_chrm_beyond = 0
            num_chrm_within_bounds = 0

            for key in list(bed_chrom_sizes.keys()):
                if key in genome_chrom_sizes:
                    if bed_chrom_sizes[key] > genome_chrom_sizes[key]:
                        num_of_chrm_beyond += 1
                        chroms_beyond_range = True
                    else:
                        num_chrm_within_bounds += 1

            # Calculate recall/sensitivity for chrom lengths
            # defined as OOBR -> Out of Bounds Range
            sensitivity = num_chrm_within_bounds / (
                num_chrm_within_bounds + num_of_chrm_beyond
            )
            length_stats["OOBR"] = sensitivity

            length_stats["beyond_range"] = chroms_beyond_range
            length_stats["num_of_chrm_beyond"] = num_of_chrm_beyond

            # Naive calculation, seq fit (below) may be better metric
            length_stats["percentage_bed_chrm_beyond"] = (
                100 * num_of_chrm_beyond / len(bed_chrom_set)
            )
            length_stats["percentage_genome_chrm_beyond"] = (
                100 * num_of_chrm_beyond / len(genome_chrom_set)
            )

        else:
            length_stats = {}
            length_stats["beyond_range"] = None
            length_stats["num_of_chrm_beyond"] = None

        # Layer 3 Calculate Sequence Fit if any query chrom names were present
        if len(query_keys_present) > 0:
            seq_fit_stats = {}
            bed_sum = 0
            ref_genome_sum = 0
            for chr in query_keys_present:
                bed_sum += int(genome_chrom_sizes[chr])
            for chr in genome_chrom_sizes:
                ref_genome_sum += int(genome_chrom_sizes[chr])

            sequence_fit = bed_sum / ref_genome_sum
            seq_fit_stats["sequence_fit"] = sequence_fit
        else:
            seq_fit_stats = {}
            seq_fit_stats["sequence_fit"] = None

        return {
            "chrom_name_stats": name_stats,
            "chrom_length_stats": length_stats,
            "seq_fit_stats": seq_fit_stats,
        }

    def get_igd_overlaps(self, bedfile: str) -> Union[dict[str, dict], dict[str, None]]:
        """
        This is the third layer compatibility check.
        It runs helper functions to execute an igd search query across an Integrated Genome Database.

        It returns a dict of dicts containing keys (file names) and values (number of overlaps). Or if no overlaps are found,
        it returns an empty dict.

        Currently for this function to work, the user must install the C version of IGD and have created a local igd file
        for the Excluded Ranges Bedset:
        https://github.com/databio/IGD
        https://bedbase.org/bedset/excluderanges

        """

        # Construct an IGD command to run as subprocess
        igd_command = IGD_LOCATION + f" search {self.igd_path} -q {bedfile}"

        returned_stdout = run_igd_command(igd_command)

        # print(f"DEBUG: {returned_stdout}")

        if not returned_stdout:
            return {"igd_stats": None}

        igd_overlap_data = parse_IGD_output(returned_stdout)

        if not igd_overlap_data:
            return {
                "igd_stats": {}
            }  # None tells us if the bed file never made it to layer 3 or perhaps igd errord, empty dict tells us that there were no overlaps found
        else:
            overlaps_dict = {}
            for datum in igd_overlap_data:
                if "file_name" in datum and "number_of_hits" in datum:
                    overlaps_dict.update({datum["file_name"]: datum["number_of_hits"]})

        return {"igd_stats": overlaps_dict}

    def determine_compatibility(
        self, bedfile: str, ref_filter: Optional[list[str]] = None
    ) -> Union[list[dict], None]:
        """
        Given a bedfile, determine compatibility with reference genomes (GenomeModels) created at Validator initialization.

        :param str bedfile: path to bedfile on disk
        :param list[str] ref_filter: list of ref genome aliases to filter on.
        :return list[dict]: a list of dictionaries where each element of the array represents a compatibility dictionary
                            for each refgenome model.
        """

        if ref_filter:
            # Before proceeding filter out unwanted reference genomes to assess
            for genome_model in self.genome_models:
                if genome_model.genome_alias in ref_filter:
                    self.genome_models.remove(genome_model)

        bed_chrom_info = get_bed_chrom_info(
            bedfile
        )  # for this bed file determine the chromosome lengths

        if not bed_chrom_info:
            # if there is trouble parsing the bed file, return None
            return None

        model_compat_stats = {}
        final_compatibility_list = []
        for genome_model in self.genome_models:
            # First and Second Layer of Compatibility
            model_compat_stats[genome_model.genome_alias] = self.calculate_chrom_stats(
                bed_chrom_info, genome_model.chrom_sizes
            )
            # Fourth Layer is to run IGD but only if layer 1 and layer 2 have passed
            if (
                model_compat_stats[genome_model.genome_alias]["chrom_name_stats"][
                    "passed_chrom_names"
                ]
                and not model_compat_stats[genome_model.genome_alias][
                    "chrom_length_stats"
                ]["beyond_range"]
            ):
                model_compat_stats[genome_model.genome_alias].update(
                    self.get_igd_overlaps(bedfile)
                )
            else:
                model_compat_stats[genome_model.genome_alias].update(
                    {"igd_stats": None}
                )

            # Once all stats are collected, process them and add compatibility rating
            processed_stats = self.process_compat_stats(
                model_compat_stats[genome_model.genome_alias], genome_model.genome_alias
            )

            final_compatibility_list.append(processed_stats)

        return final_compatibility_list

    def process_compat_stats(self, compat_stats: dict, genome_alias: str) -> dict:
        """
        Given compatibility stats for a specific ref genome determine the compatibility tier

        Tiers Definition ----

        Tier1: Excellent compatibility, 0 pts
        Tier2: Good compatibility, may need some processing, 1-3 pts
        Tier3: Bed file needs processing to work (shifted hg38 to hg19?), 4-6 pts
        Tier4: Poor compatibility, 7-9 pts

        :param dict compat_stats: dicitionary containing unprocessed compat stats
        :param str genome_alias:
        :return dict : containing both the original stats and the final compatibility rating for the reference genome
        """

        # Set up processed stats dict, it will add an extra key to the original dict
        # with the final rating
        processed_stats = {}
        processed_stats[genome_alias] = {}
        processed_stats[genome_alias].update(compat_stats)

        # Currently will proceed with discrete buckets, however, we could do a point system in the future
        final_compat_rating = {"tier_ranking": 1, "assigned_points": 0}
        points_rating = 0  # we will add points increasing the level and then sort into various tiers at the end

        # Check extra sequences sensitivity and assign points based on how sensitive the outcome is
        # sensitivity = 1 is considered great and no points should be assigned
        if compat_stats["chrom_name_stats"]["XS"] < 1:
            points_rating += 3
        if compat_stats["chrom_name_stats"]["XS"] < 0.7:
            points_rating += 1
        if compat_stats["chrom_name_stats"]["XS"] < 0.5:
            points_rating += 1
        if compat_stats["chrom_name_stats"]["XS"] < 0.3:
            points_rating += 1

        if compat_stats["chrom_name_stats"]["passed_chrom_names"]:
            # Check OOBR and assign points based on sensitivity
            # only assessed if no extra chroms in query bed file
            if compat_stats["chrom_length_stats"]["OOBR"] < 1:
                points_rating += 3
            if compat_stats["chrom_length_stats"]["OOBR"] < 0.7:
                points_rating += 1
            if compat_stats["chrom_length_stats"]["OOBR"] < 0.5:
                points_rating += 1
            if compat_stats["chrom_length_stats"]["OOBR"] < 0.3:
                points_rating += 1
        else:
            # Do nothing here, points have already been added when Assessing XS if it is not == 1
            pass

        # Check Sequence Fit - comparing lengths in queries vs lengths of queries in ref genome vs not in ref genome
        if compat_stats["seq_fit_stats"]["sequence_fit"]:
            # since this is only on keys present in both, ratio should always be less than 1
            # Should files be penalized here or actually awarded but only if the fit is really good?
            if compat_stats["seq_fit_stats"]["sequence_fit"] < 0.90:
                points_rating += 1
            if compat_stats["seq_fit_stats"]["sequence_fit"] < 0.60:
                points_rating += 1
            if compat_stats["seq_fit_stats"]["sequence_fit"] < 0.60:
                points_rating += 1

        else:
            # if no chrom names were found during assessment
            points_rating += 4

        # Run analysis on igd_stats
        # WIP, currently only showing IGD stats for informational purposes
        if compat_stats["igd_stats"] and compat_stats["igd_stats"] != {}:
            self.process_igd_stats(compat_stats["igd_stats"])

        final_compat_rating["assigned_points"] = points_rating

        # Now Assign Tier Based on Points

        if points_rating == 0:
            final_compat_rating["tier_ranking"] = 1
        elif 1 <= points_rating <= 3:
            final_compat_rating["tier_ranking"] = 2
        elif 4 <= points_rating <= 6:
            final_compat_rating["tier_ranking"] = 3
        elif 7 <= points_rating:
            final_compat_rating["tier_ranking"] = 4
        else:
            print(
                f"Catching points discrepancy,points = {points_rating}, assigning to Tier 4"
            )
            final_compat_rating["tier_ranking"] = 4

        processed_stats[genome_alias].update({"compatibility": final_compat_rating})

        return processed_stats

    def process_igd_stats(self, igd_stats: dict):
        """
        Placeholder to process IGD Stats and determine if it should impact tier rating
        """
        pass


# ----------------------------
# Helper Functions
def get_bed_chrom_info(bed_file_path: str) -> Union[dict, None]:
    """
    Given a path to a Bedfile. Attempt to open it and read it to find all of the chromosomes and the max length of each.
    """

    # Right now this assumes this is atleast a 3 column bedfile
    # This also assumes the bed file has been unzipped

    try:
        df = pd.read_csv(bed_file_path, sep="\t", header=None, skiprows=5)

        max_end_for_each_chrom = df.groupby(0)[2].max()

        # return max end position for each chromosome
        return max_end_for_each_chrom.to_dict()
    except Exception:
        return None


def run_igd_command(command):
    """Run IGD via a subprocess, this is a temp implementation until Rust IGD python bindings are finished."""

    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout
    else:
        print(f"Error running command: {result.stderr}")
        return None


def parse_IGD_output(output_str) -> Union[None, list[dict]]:
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
