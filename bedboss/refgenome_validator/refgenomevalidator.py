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

    def predict_parent(self):
        """
        Predicts from a mutually exclusive list of reference genomes

        """

        # For mutually exclusive items, we can quickly check, chrom sizes and chrom names and see if there are incompatible
        # genome models and truncate the list at the very beginning

        predicted_parents = []

        for bed in self.bedfiles:
            # We may be doing this somewhere else and should just pass this in...
            bed_chrom_info = get_bed_chrom_info(bed)

            for genome in self.genome_models:
                exclude_result = self.compare_chrom_size(
                    bed_chrom_info, genome.chrom_sizes
                )
                if not exclude_result:
                    # keep the genome
                    predicted_parents.append(genome.alias)
                else:
                    # Can we for sure exclude this? if so delete it
                    # Do we need to deep copy this or will it cause issues?
                    self.genome_models.remove(genome)

        predicted_parent = "hg38"

        return predicted_parent

    def modify_compatibility_matrix(self):
        """

        modifys the data frame storing compatibility matrix of entries

        """

        pass

    def compare_chrom_names_lengths(
        self, bed_chrom_sizes: dict, genome_chrom_sizes: dict
    ):
        """
        Given two dicts of chroms (key) and their sizes (values)
        determine overlap

        :param dict bed_chrom_sizes: dict of a bedfile's chrom size
        :param dict genome_chrom_sizes: dict of a GenomeModel's chrom sizes

        return dict: returns a dictionary with information on Query vs Model, e.g. chrom names QueryvsModel
        """

        # Check names
        # Define Three separate counts
        # Q = Query, M = Model
        name_stats = {}
        q_and_m = None
        q_and_not_m = None
        not_q_and_m = None

        name_stats = {}

        extra_chroms = False
        chroms_beyond_range = False
        exclude = False

        for key in list(bed_chrom_sizes.keys()):
            if key not in genome_chrom_sizes:
                extra_chroms = True
            elif key in genome_chrom_sizes:
                if genome_chrom_sizes[key] < bed_chrom_sizes[key]:
                    chroms_beyond_range = True

        if extra_chroms or chroms_beyond_range:
            exclude = True
            return exclude

        return exclude

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
