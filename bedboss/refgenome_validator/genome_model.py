from typing import Union, Dict


class GenomeModel:
    """
        Initialize genome model

    A class representing a reference genome. You feed it a reference genome. It retrieves chrom sizes (from refgenie),
    and then it provides some helper functions, intended for use with reference genome validation.

    """

    def __init__(
        self,
        genome_alias: str,
        chrom_sizes_file: Union[str, Dict[str, int]],
        genome_digest: Union[str, None] = None,
        # common_aliases: Optional[List] = None,
        # exclude_ranges_names: Optional[List] = None,
    ):
        self._genome_alias = genome_alias
        self.chrom_sizes_file = chrom_sizes_file
        self._genome_digest = genome_digest
        self._chrom_sizes = self.get_chrom_sizes()
        # self.common_aliases = common_aliases  # What are the other names for the other this reference genomes
        # self.excluded_ranges_names = exclude_ranges_names  # Which bed file digests from the excluded ranges are associated with this reference genome?

    @property
    def genome_alias(self):
        return self._genome_digest  # TODO: update everything

    @property
    def genome_digest(self):
        return self._genome_digest

    @property
    def chrom_sizes(self):
        return self._chrom_sizes

    def get_chrom_sizes(self) -> dict:
        """
        Obtains chrom sizes via refgenie (using a refgenconf.refgenconf.RefGenConf object)

        :return dict: dictionary containing chroms(keys) and lengths(values)
        """

        if isinstance(self.chrom_sizes_file, str):
            chrom_sizes_path = self.chrom_sizes_file

            chrom_sizes = {}

            with open(chrom_sizes_path, "r") as f:
                for line in f:
                    chrom, size = line.strip().split("\t")
                    chrom_sizes[chrom] = int(size)
        else:
            # TODO: needs to be validated
            chrom_sizes = self.chrom_sizes_file

        return chrom_sizes

    def filter_excluded_ranges(self, bed_list, igd_hit_matrix):
        """
        BED List of Excluded Ranges files associated with this reference genome.
        These will be manually curated from the Excluded ranges BedSet on BedBase



        """

        # We will probably have a singular .igd database that we will simply compare the bed file to, so this should probably
        # just filter results in a way to determine if there were any hits/not hits for this particular genome

        raise NotImplementedError()
