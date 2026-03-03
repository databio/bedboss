import logging
from typing import List, Union

from gtars.models import GenomeAssembly, RegionSet
from gtars.genomic_distributions import calc_gc_content
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

    :return: path to fasta file
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
    bedfile: RegionSet, genome: str, rfg_config: str = None
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
