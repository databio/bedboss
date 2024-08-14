from typing import Optional, List

import refgenconf


class GenomeModel:
    """
    Initialize genome model


    """

    def __init__(
        self,
        genome_alias: Optional[str] = None,
        refgenomeconf: Optional[refgenconf.refgenconf.RefGenConf] = None,
    ):
        self.genome_alias = genome_alias
        self.rgc = refgenomeconf
        self.chrom_sizes = self.get_chrom_sizes()

        self.excluded_ranges = self.get_excluded_ranges()

        pass

    def get_chrom_sizes(self):
        # read chromsizes file

        chrom_sizes_path = self.rgc.seek(
            genome_name=self.genome_alias,
            asset_name="fasta",
            tag_name="default",
            seek_key="chrom_sizes",
        )
        print(chrom_sizes_path)

        chrom_sizes = {}

        with open(chrom_sizes_path, "r") as f:
            for line in f:
                chrom, size = line.strip().split("\t")
                chrom_sizes[chrom] = int(size)

        return chrom_sizes

    def get_excluded_ranges(self):
        # given an alias or digest, can we grab the excluded ranges files from BEDBASE?

        # return path or list of paths of IGD databases for this particular genome alias?

        pass
