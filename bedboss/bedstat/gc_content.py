import logging
import os
from typing import List, Union

import matplotlib.pyplot as plt
import seaborn as sns
from gdrs import GenomeAssembly, calc_gc_content
from matplotlib.ticker import MaxNLocator
from refgenconf import RefgenconfError
from yacman.exceptions import UndefinedAliasError

from bedboss.bedmaker.utils import get_rgc

_LOGGER = logging.getLogger("bedboss")

assembly_objects = {}


def get_genome_fasta_file(genome: str, rfg_config: str = None) -> str:
    """
    Get genome fasta file with Refgenie.

    :param genome: genome name
    :param rfg_config: path to refgenie config file
    """
    rgc = get_rgc(rfg_config=rfg_config)

    try:
        # get local path to the chrom.sizes asset
        fasta_file = rgc.seek(
            genome_name=genome,
            asset_name="fasta",
            tag_name="default",
            seek_key="fasta",
        )
        _LOGGER.info(f"fasta path: {fasta_file}")
    except (UndefinedAliasError, RefgenconfError):
        # if no local chrom.sizes file found, pull it first
        _LOGGER.info("Could not determine path to chrom.sizes asset, pulling")
        rgc.pull(genome=genome, asset="fasta", tag="default")
        fasta_file = rgc.seek(
            genome_name=genome,
            asset_name="fasta",
            tag_name="default",
            seek_key="fasta",
        )
    return fasta_file


def get_genome_assembly_obj(
    genome: str, rfg_config: str = None
) -> Union[GenomeAssembly, None]:
    """
    Get assembly object for a genome.

    :param genome: genome name
    :param rfg_config: path to refgenie config file

    :return: assembly object
    """
    if genome in assembly_objects:
        return assembly_objects[genome]
    else:
        try:
            assembly_objects[genome] = GenomeAssembly(
                get_genome_fasta_file(genome, rfg_config=rfg_config)
            )
            return assembly_objects[genome]
        except Exception as e:
            _LOGGER.error(f"Could not get assembly object for {genome}: {e}")
            return None


def calculate_gc_content(
    bedfile: str, genome: str, rfg_config: str = None
) -> Union[List[float], None]:
    """
    Calculate GC content for a bed file.

    :param bedfile: path to bed file
    :param genome: genome name
    :param rfg_config: path to refgenie config file

    :return: list of GC contents
    """

    assembly_obj = get_genome_assembly_obj(genome, rfg_config=rfg_config)
    if assembly_obj is None:
        return None

    gc_contents = calc_gc_content(
        bedfile,
        assembly_obj,
        ignore_unk_chroms=True,
    )
    return gc_contents


def create_gc_plot(
    bed_id: str, gc_contents: List[float], outfolder: str, gc_mean: float
) -> dict:
    """
    Create a GC content plot.

    :param bed_id: bed ID
    :param gc_contents: list of GC contents
    :param outfolder: path to output file
    :param gc_mean: mean GC content

    :return str: path to output file
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
