"""Thin wrapper around refgenie 1.0 for the assets bedboss needs at runtime.

Currently this is only the genome FASTA used for GC-content statistics, and
only for the small set of canonical genomes we compute statistics for
(``CANONICAL_GENOME_DIGESTS`` / ``STATS_SUPPORTED_GENOMES``).

refgenie is configured the native way: a ``Refgenie()`` client reads its own
config from the environment (``REFGENIE_HOME_PATH``, default ``~/.refgenie``),
auto-initializes its SQLite database and genome folders on first use, and
subscribes to ``https://api.refgenie.org``. No bedboss-specific config file and
no manual ``refgenie init`` is required.
"""

import logging
from functools import lru_cache

from refgenie import GenomeDigest, Refgenie

from bedboss.const import CANONICAL_GENOME_DIGESTS, PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)

FASTA_ASSET = "fasta"


@lru_cache(maxsize=1)
def get_refgenie() -> Refgenie:
    """
    Return a process-wide refgenie 1.0 client.

    The client reads refgenie's own configuration from the environment and
    initializes the local database/genome folders on first construction.
    """
    _LOGGER.info("Initializing refgenie client...")
    return Refgenie()


def get_fasta_path(genome_alias: str) -> str | None:
    """
    Get the local path to a canonical genome's FASTA, pulling it if missing.

    Args:
        genome_alias: Standardized genome alias (e.g. ``hg38``).

    Returns:
        Absolute path to the FASTA file, or ``None`` if the genome is not one of
        the canonical genomes we support for statistics.
    """
    digest = CANONICAL_GENOME_DIGESTS.get(genome_alias)
    if digest is None:
        _LOGGER.info(
            f"Genome '{genome_alias}' is not a supported statistics genome; "
            "skipping FASTA retrieval."
        )
        return None

    rgc = get_refgenie()
    genome_digest = GenomeDigest(digest)

    try:
        return rgc.asset.seek(
            genome_digest,
            FASTA_ASSET,
            seek_key_name=FASTA_ASSET,
            force_exists=True,
            abs_path=True,
        )
    except Exception:
        _LOGGER.info(
            f"FASTA for '{genome_alias}' ({digest}) not found locally; "
            "pulling from refgenie..."
        )
        rgc.transfer.pull(FASTA_ASSET, genome=genome_digest, force_large=True)
        return rgc.asset.seek(
            genome_digest,
            FASTA_ASSET,
            seek_key_name=FASTA_ASSET,
            force_exists=True,
            abs_path=True,
        )
