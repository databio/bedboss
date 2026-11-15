import logging
import os

import matplotlib.pyplot as plt
import seaborn as sns
from gtars.genomic_distributions import calc_gc_content
from gtars.models import GenomeAssembly, RegionSet
from matplotlib.ticker import MaxNLocator

from bedboss.refgenie_manager import get_fasta_path

_LOGGER = logging.getLogger("bedboss")

assembly_objects = {}


def get_genome_assembly_obj(genome: str) -> GenomeAssembly | None:
    """
    Get assembly object for a genome.

    The genome FASTA is retrieved via refgenie 1.0, and is only available for
    the canonical statistics genomes (hg38, hg19, mm10).

    Args:
        genome: Genome name.

    Returns:
        Assembly object, or None if the FASTA is not available for the genome.
    """
    if genome in assembly_objects:
        return assembly_objects[genome]

    fasta_file = get_fasta_path(genome)
    if fasta_file is None:
        return None
    try:
        assembly_objects[genome] = GenomeAssembly(fasta_file)
        return assembly_objects[genome]
    except Exception as e:
        _LOGGER.error(f"Could not get assembly object for {genome}: {e}")
        return None


def calculate_gc_content(bedfile: RegionSet, genome: str) -> list[float] | None:
    """
    Calculate GC content for a bed file.

    Args:
        bedfile: RegionSet object.
        genome: Genome name.

    Returns:
        List of GC contents, or None if assembly cannot be loaded.
    """

    assembly_obj = get_genome_assembly_obj(genome)
    if assembly_obj is None:
        return None

    gc_contents = calc_gc_content(
        bedfile,
        assembly_obj,
        ignore_unk_chroms=True,
    )
    return gc_contents


def create_gc_plot(
    bed_id: str, gc_contents: list[float], outfolder: str, gc_mean: float
) -> dict:
    """
    Create a GC content plot.

    Args:
        bed_id: Bed ID.
        gc_contents: List of GC contents.
        outfolder: Path to output folder.
        gc_mean: Mean GC content.

    Returns:
        Dict with plot metadata (name, title, thumbnail_path, path).
    """
    plt.rcParams["font.size"] = 10
    plt.figure(figsize=(8, 8))
    sns.kdeplot(gc_contents, linewidth=0.8, color="black")
    plt.gca().xaxis.set_major_locator(MaxNLocator(nbins=5))

    plt.axvline(
        gc_mean,
        color="r",
        linestyle="--",
        linewidth=0.8,
        label=f"Mean: {gc_mean:.2f}",
    )
    sns.despine()
    plt.xlabel("GC Content")
    plt.legend()

    plt.title("GC Content Distribution")

    pdf_path = os.path.join(outfolder, f"{bed_id}_gccontent.pdf")
    png_path = os.path.join(outfolder, f"{bed_id}_gccontent.png")

    plt.savefig(pdf_path)
    plt.savefig(png_path)

    return {
        "name": "gccontent",
        "title": "GC Content Distribution",
        "thumbnail_path": png_path,
        "path": pdf_path,
    }
