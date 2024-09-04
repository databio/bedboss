import pandas as pd


class Validator:
    """
    Initialize validator class

    1. filter out incompatible genomes
    2. for remaining, determine tiers of compatibility


    """

    def __init__(self, genome_models):
        # TODO ensure these are lists

        self.genome_models = genome_models

        self.compatibility_list = (
            []
        )  # this will be a list of dictionary info with length of genome_models

    def compare_chrom_names_lengths(
        self, bed_chrom_sizes: dict, genome_chrom_sizes: dict
    ) -> dict:
        """
        Given two dicts of chroms (key) and their sizes (values)
        determine overlap

        :param dict bed_chrom_sizes: dict of a bedfile's chrom size
        :param dict genome_chrom_sizes: dict of a GenomeModel's chrom sizes

        return dict: returns a dictionary with information on Query vs Model, e.g. chrom names QueryvsModel
        """

        # Layer 1: Check names
        # Define Three separate counts
        # Q = Query, M = Model
        name_stats = {}
        q_and_m = 0
        q_and_not_m = 0
        not_q_and_m = 0
        passed_chrom_names = True

        for key in list(bed_chrom_sizes.keys()):
            if key not in genome_chrom_sizes:
                q_and_not_m += 1
            if key in genome_chrom_sizes:
                q_and_m += 1
        for key in list(genome_chrom_sizes.keys()):
            if key not in bed_chrom_sizes:
                not_q_and_m += 1

        # What is our threshold for passing layer 1?
        if q_and_not_m > 1:
            passed_chrom_names = False

        name_stats["q_and_m"] = q_and_m
        name_stats["q_and_not_m"] = q_and_not_m
        name_stats["not_q_and_m"] = not_q_and_m
        name_stats["passed_chrom_names"] = passed_chrom_names

        # Layer 2:  Check Lengths, but only if layer 1 is passing
        if passed_chrom_names:
            length_stats = {}

            chroms_beyond_range = False
            num_of_chrm_beyond = 0

            for key in list(bed_chrom_sizes.keys()):
                if key in genome_chrom_sizes:
                    if bed_chrom_sizes[key] > genome_chrom_sizes[key]:
                        num_of_chrm_beyond += 1
                        chroms_beyond_range = True

            length_stats["beyond_range"] = chroms_beyond_range
            length_stats["num_of_chrm_beyond"] = num_of_chrm_beyond
        else:
            length_stats = {}
            length_stats["beyond_range"] = None
            length_stats["num_of_chrm_beyond"] = None

        return {"chrom_name_stats": name_stats, "chrom_length_stats": length_stats}

    def determine_compatibility(
        self, bedfile: str, ref_filter: list[str]
    ) -> list[dict]:
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
                if genome_model.alias in ref_filter:
                    self.genome_models.remove(genome_model)

        compatibility_list = []

        bed_chrom_info = get_bed_chrom_info(
            bedfile
        )  # for this bed file determine the chromosome lengths

        for genome_model in self.genome_models:
            model_compat_info = {}
            model_compat_info[genome_model.alias] = self.compare_chrom_names_lengths(
                bed_chrom_info, genome_model.chrom_sizes
            )

        pass


def get_bed_chrom_info(bed_file_path: str):
    """
    Given a path to a Bedfile. Attempt to open it and read it to find all of the chromosomes and the max length of each.
    """

    # TODO In bed classifier we skip a few rows just in case there is header information there...

    # Right now this assumes this is atleast a 3 column bedfile
    df = pd.read_csv(bed_file_path, sep="\t", header=None)

    max_end_for_each_chrom = df.groupby(0)[2].max()

    # return max end position for each chromosome
    return max_end_for_each_chrom.to_dict()
