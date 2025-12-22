# functions for tokenization of bed files
import logging
import os
from typing import Union

from bbconf.bbagent import BedBaseAgent
from geniml.bbclient import BBClient
from geniml.bbclient.const import DEFAULT_CACHE_FOLDER
from gtars.tokenizers import Tokenizer

from bedboss.exceptions import BedBossException

_LOGGER = logging.getLogger("bedboss")


def tokenize_bed_file(
    universe: str,
    bed: str,
    cache_folder: Union[str, os.PathLike] = DEFAULT_CACHE_FOLDER,
    add_to_db: bool = False,
    config: str = None,
    overwrite: bool = False,
) -> None:
    """
    Tokenize all bed file and add to the local cache

    :param universe: universe name to which the bed file will be tokenized
    :param bed: bed file to be tokenized
    :param cache_folder: path to the cache folder
    :param add_to_db: flag to add tokenized bed file to the bedbase database [config should be provided if True]
    :param config: path to the bedbase config file
    :param overwrite: flag to overwrite the existing tokenized bed file

    :return: None
    """
    bbc = BBClient(cache_folder=cache_folder or DEFAULT_CACHE_FOLDER)

    tokenizer = Tokenizer(bbc.seek(universe))
    rs = bbc.load_bed(bed)

    tokens = tokenizer.encode(tokenizer.tokenize(rs))
    # tokens - list[1 ,2 ,3,4 ]

    bbc.cache_tokens(universe, bed, tokens)
    _LOGGER.info(f"Tokenized bed file '{bed}' added to the cache")

    if add_to_db:
        if not config:
            BedBossException(
                "Config file is required to add tokenized bed file to the database"
            )

        bbagent = BedBaseAgent(config=config)
        bbagent.bed.add_tokenized(
            bed_id=bed, universe_id=universe, token_vector=tokens, overwrite=overwrite
        )
        _LOGGER.info(f"Tokenized bed file '{bed}' added to the database")


def delete_tokenized(
    universe: str,
    bed: str,
    config: str = None,
) -> None:
    """
    Delete tokenized bed file from the database

    :param universe: universe name to which the bed file will be tokenized
    :param bed: bed file to be tokenized
    :param config: path to the bedbase config file

    :return: None
    """
    bba = BedBaseAgent(config=config)

    bba.bed.delete_tokenized(bed_id=bed, universe_id=universe)
