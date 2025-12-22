import logging

from bbconf.bbagent import BedBaseAgent

_LOGGER = logging.getLogger("bedboss")


def add_to_qdrant(config: str, batch: int = 100, purge: bool = False) -> None:
    """
    Reindex all bed files in qdrant

    :param config: path to the config file
    """
    agent = BedBaseAgent(config=config)
    agent.bed.reindex_qdrant(batch=batch, purge=purge)


def reindex_semantic_search(
    config: str, purge: bool = False, batch: int = 1000
) -> None:
    """
    Reindex semantic search in qdrant

    :param config: path to the config file.
    :param purge: whether to purge the existing index before reindexing.
    :param batch: number of items that will be uploaded to qdrant in one batch.

    :return: None
    """

    agent = BedBaseAgent(config=config)
    agent.bed.reindex_hybrid_search(batch=batch, purge=purge)
    _LOGGER.info("Semantic search reindexing completed.")
