import logging
import os
from pathlib import Path

from bedboss.const import PKG_NAME
from bedboss.exceptions import BedBossException
from bedboss.refgenome_validator.genome_model import GenomeModel
from bedboss.refgenome_validator.refgenie_chrom_sizes import chrom_sizes_dict_for

_LOGGER = logging.getLogger(PKG_NAME)


def write_chrom_sizes_file(
    genome_alias: str,
    out_dir: str | Path,
    genome_models: list[GenomeModel] | None = None,
) -> str:
    """
    Materialize a tab-separated ``<genome>.chrom.sizes`` file on disk.

    The chrom sizes come from the same seqcol data the reference-genome
    validator loads (see ``chrom_sizes_dict_for``). This is needed only for the
    WIG -> bigWig step, because ``wigToBigWig`` is an external UCSC binary that
    requires a chrom.sizes *file*. BigBed does not use this — it passes the
    chrom-sizes dict straight to ``RegionSet.to_bigbed``.

    Args:
        genome_alias: Standardized genome alias (e.g. ``hg38``).
        out_dir: Directory to write the chrom.sizes file into.
        genome_models: Optional list of already-loaded GenomeModels to reuse.

    Returns:
        Path to the written chrom.sizes file.

    Raises:
        BedBossException: If chrom sizes for the genome are not available.
    """
    chrom_sizes = chrom_sizes_dict_for(genome_alias, genome_models=genome_models)
    if not chrom_sizes:
        raise BedBossException(
            f"Chrom sizes not found for genome '{genome_alias}'. "
            "Cannot create chrom.sizes file."
        )

    os.makedirs(out_dir, exist_ok=True)
    chrom_sizes_path = os.path.join(out_dir, f"{genome_alias}.chrom.sizes")
    with open(chrom_sizes_path, "w") as f:
        for name, length in chrom_sizes.items():
            f.write(f"{name}\t{length}\n")

    _LOGGER.info(f"Wrote chrom.sizes file: {chrom_sizes_path}")
    return chrom_sizes_path
