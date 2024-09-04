from typing import Optional, List

import refgenconf


class GenomeModel:
    """
        Initialize genome model
    A class representing a reference genome. You feed it a reference genome. It retrieves chrom sizes (from refgenie),
    and then it provides some helper functions, intended for use with reference genome validation.

    """

    def __init__(
        self,
        genome_alias: Optional[str] = None,
        common_aliases: Optional[List] = None,
        refgenomeconf: Optional[refgenconf.refgenconf.RefGenConf] = None,
        exclude_ranges_names: Optional[List] = None,
    ):
        self.genome_alias = genome_alias
        self.common_aliases = common_aliases  # What are the other names for the other this reference genomes
        self.rgc = refgenomeconf
        self.chrom_sizes = self.get_chrom_sizes()
        self.excluded_ranges_names = exclude_ranges_names  # Which bed file digests from the excluded ranges are associated with this reference genome?

    def get_chrom_sizes(self) -> dict:
        """
        Obtains chrom sizes via refgenie (using a refgenconf.refgenconf.RefGenConf object)

        :return dict: dictionary containing chroms(keys) and lengths(values)
        """

        chrom_sizes_path = self.rgc.seek(
            genome_name=self.genome_alias,
            asset_name="fasta",
            tag_name="default",
            seek_key="chrom_sizes",
        )

        chrom_sizes = {}

        with open(chrom_sizes_path, "r") as f:
            for line in f:
                chrom, size = line.strip().split("\t")
                chrom_sizes[chrom] = int(size)

        return chrom_sizes

    def filter_excluded_ranges(self, bed_list, igd_hit_matrix):
        """
        BED List of Excluded Ranges files associated with this reference genome.
        These will be manually curated from the Excluded ranges BedSet on BedBase



        """

        # We will probably have a singular .igd database that we will simply compare the bed file to, so this should probably
        # just filter results in a way to determine if there were any hits/not hits for this particular genome

        pass
