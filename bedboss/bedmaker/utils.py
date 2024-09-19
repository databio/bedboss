import logging
import os
from pathlib import Path
from typing import Union

from refgenconf import CFG_ENV_VARS, CFG_FOLDER_KEY
from refgenconf import RefGenConf as RGC
from refgenconf import RefgenconfError, select_genome_config
from yacman.exceptions import UndefinedAliasError

from bedboss.const import DEFAULT_REFGENIE_PATH, REFGENIE_ENV_VAR

_LOGGER = logging.getLogger("bedboss")


def get_chrom_sizes(genome: str, rfg_config: Union[str, Path]) -> str:
    """
    Get chrom.sizes file with Refgenie.

    :param genome: genome name
    :return str: path to chrom.sizes file for the genome
    """

    _LOGGER.info("Determining path to chrom.sizes asset via Refgenie.")
    # initializing refginie config file
    rgc = get_rgc(rfg_config=rfg_config)

    try:
        # get local path to the chrom.sizes asset
        chrom_sizes = rgc.seek(
            genome_name=genome,
            asset_name="fasta",
            tag_name="default",
            seek_key="chrom_sizes",
        )
        _LOGGER.info(chrom_sizes)
    except (UndefinedAliasError, RefgenconfError):
        # if no local chrom.sizes file found, pull it first
        _LOGGER.info("Could not determine path to chrom.sizes asset, pulling")
        rgc.pull(genome=genome, asset="fasta", tag="default")
        chrom_sizes = rgc.seek(
            genome_name=genome,
            asset_name="fasta",
            tag_name="default",
            seek_key="chrom_sizes",
        )

    _LOGGER.info(f"Determined path to chrom.sizes asset: {chrom_sizes}")

    return chrom_sizes


def get_rgc(rfg_config: Union[str, Path] = None) -> RGC:
    """
    Get refgenie config file.

    :return str: rfg_config file path
    :return RGC: refgenie config object
    """
    if not rfg_config:
        _LOGGER.info("Creating refgenie genome config file...")
        cwd = os.getenv(REFGENIE_ENV_VAR, DEFAULT_REFGENIE_PATH)
        rfg_config = os.path.join(cwd, "genome_config.yaml")

    # get path to the genome config; from arg or env var if arg not provided
    refgenie_cfg_path = select_genome_config(filename=rfg_config, check_exist=False)

    if not refgenie_cfg_path:
        raise OSError(
            "Could not determine path to a refgenie genome configuration file. "
            f"Use --rfg-config argument or set the path with '{CFG_ENV_VARS}'."
        )
    if isinstance(refgenie_cfg_path, str) and not os.path.exists(refgenie_cfg_path):
        # file path not found, initialize a new config file
        _LOGGER.info(
            f"File '{refgenie_cfg_path}' does not exist. "
            "Initializing refgenie genome configuration file."
        )
        rgc = RGC(entries={CFG_FOLDER_KEY: os.path.dirname(refgenie_cfg_path)})
        rgc.initialize_config_file(filepath=refgenie_cfg_path)
    else:
        _LOGGER.info(
            "Reading refgenie genome configuration file from file: "
            f"{refgenie_cfg_path}"
        )
        rgc = RGC(filepath=refgenie_cfg_path)

    return rgc
