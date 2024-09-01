import pandas as pd


class Validator:
    """
    Initialize validator class

    1. filter out incompatible genomes
    2. for remaining, determine tiers of compatibility


    """

    def __init__(self, bedfiles, genome_models):
        # TODO ensure these are lists

        self.bedfiles = bedfiles
        self.genome_models = genome_models

        self.compatibility_matrix = pd.DataFrame(
            index=self.bedfiles, columns=self.genome_models
        )  # non mutually exclusive  validation, i.e.

        self.predicted_parent = self.predict_parent()

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

    def compare_chrom_size(self, bed_chrom_sizes, genome_chrom_sizes):
        """
        Given two dicts of chroms (key) and their sizes (values)
        determine overlap
        """

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


def get_bed_chrom_info(bed_file_path: str):
    """
    Given a path to a Bedfile. Attempt to open it and read it to find all of the chromosomes and the max length of each.
    """

    # In bed classifier we skip a few rows just in case there is header information there...

    # Right now this assumes this is atleast a 3 column bedfile
    df = pd.read_csv(bed_file_path, sep="\t", header=None)

    max_end_for_each_chrom = df.groupby(0)[2].max()

    # return max end position for each chromosome
    return max_end_for_each_chrom.to_dict()
