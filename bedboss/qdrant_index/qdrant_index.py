import logging

from bbconf.bbagent import BedBaseAgent

_LOGGER = logging.getLogger("bedboss")


def add_to_qdrant(config: str) -> None:
    """
    Reindex all bed files in qdrant

    :param config: path to the config file
    """
    agent = BedBaseAgent(config=config)
    agent.bed.reindex_qdrant()
