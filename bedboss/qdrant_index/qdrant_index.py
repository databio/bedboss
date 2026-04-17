import logging

_LOGGER = logging.getLogger("bedboss")


def add_to_qdrant(config: str, batch: int = 100, purge: bool = False) -> None:
    """
    Reindex all bed files in qdrant.

    Args:
        config: Path to the config file.
        batch: Number of items to upload in one batch.
        purge: Whether to purge the existing index before reindexing.
    """
    from bbconf.bbagent import BedBaseAgent

    agent = BedBaseAgent(config=config)
    agent.bed.reindex_qdrant(batch=batch, purge=purge)


def reindex_semantic_search(
    config: str, purge: bool = False, batch: int = 1000
) -> None:
    """
    Reindex semantic search in qdrant.

    Args:
        config: Path to the config file.
        purge: Whether to purge the existing index before reindexing.
        batch: Number of items that will be uploaded to qdrant in one batch.
    """
    from bbconf.bbagent import BedBaseAgent

    agent = BedBaseAgent(config=config)
    agent.bed.reindex_hybrid_search(batch=batch, purge=purge)
    _LOGGER.info("Semantic search reindexing completed.")
