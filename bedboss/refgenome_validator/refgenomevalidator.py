import pandas as pd


# Validator


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

        predicted_parent = "hg38"

        return predicted_parent

    def modify_compatibility_matrix(self):
        """

        modifys the data frame storing compatibility matrix of entries

        """

        pass
