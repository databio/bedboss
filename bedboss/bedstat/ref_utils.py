"""Reference file resolution for gtars genomicdist.

Handles auto-fetching and pre-compilation of GTF annotations, chrom.sizes,
and open signal matrices via refgenie with seqcol API fallback.
"""

import logging
import os
import subprocess
from typing import Union

from bedboss.const import HOME_PATH

_LOGGER = logging.getLogger("bedboss")


def _get_chrom_sizes_seqcol(genome: str) -> Union[str, None]:
    """Fallback: fetch chrom.sizes via seqcol API when refgenie doesn't have it.

    Resolves genome name to a seqcol digest via the refgenie /v4/genomes
    endpoint, then fetches chromosome names + lengths directly.
    """
    import requests
    from refget.clients import SequenceCollectionClient

    refgenie_api = "https://api.refgenie.org"
    cache_dir = os.path.join(HOME_PATH, "chrom_sizes")

    chrom_sizes_path = os.path.join(cache_dir, f"{genome}.chrom.sizes")
    if os.path.exists(chrom_sizes_path):
        _LOGGER.info(f"Chrom sizes (seqcol cache): {chrom_sizes_path}")
        return chrom_sizes_path

    # Resolve genome name -> seqcol digest
    try:
        resp = requests.get(
            f"{refgenie_api}/v4/genomes",
            params={"limit": 1000},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        _LOGGER.warning(f"seqcol fallback: failed to query genome list: {e}")
        return None

    genome_lower = genome.lower()
    digest = None
    fallback_digest = None
    for entry in resp.json().get("items", []):
        for alias in entry.get("aliases", []):
            alias_lower = alias.lower()
            if alias_lower == f"{genome_lower}-refgenie":
                digest = entry["digest"]
                break
            if not fallback_digest and alias_lower.startswith(genome_lower):
                fallback_digest = entry["digest"]
        if digest:
            break
    digest = digest or fallback_digest

    if not digest:
        _LOGGER.warning(f"seqcol fallback: no digest found for '{genome}'")
        return None

    try:
        client = SequenceCollectionClient(urls=[f"{refgenie_api}/seqcol"])
        os.makedirs(cache_dir, exist_ok=True)
        client.write_chrom_sizes(digest, chrom_sizes_path)
        _LOGGER.info(f"Chrom sizes (seqcol): {chrom_sizes_path}")
        return chrom_sizes_path
    except Exception as e:
        _LOGGER.warning(f"seqcol fallback: failed to fetch chrom.sizes: {e}")
        return None


def get_chrom_sizes_path(genome: str, rfg_config=None) -> Union[str, None]:
    """Get a chrom.sizes file for a genome.

    Tries refgenie local seek first, then seqcol API (lightweight ~50KB),
    then refgenie pull as last resort (pulls full FASTA asset ~3GB).
    """
    from refgenconf import RefgenconfError
    from yacman.exceptions import UndefinedAliasError

    from bedboss.bedmaker.utils import get_rgc

    # 1. Check local refgenie cache (instant)
    rgc = get_rgc(rfg_config=rfg_config)
    try:
        return rgc.seek(
            genome_name=genome,
            asset_name="fasta",
            tag_name="default",
            seek_key="chrom_sizes",
        )
    except (UndefinedAliasError, RefgenconfError):
        pass

    # 2. Try seqcol API (lightweight — fetches only chrom.sizes, ~50KB)
    _LOGGER.info(f"chrom.sizes not local for {genome}, trying seqcol API")
    result = _get_chrom_sizes_seqcol(genome)
    if result:
        return result

    # 3. Last resort: pull full FASTA asset from refgenie (~3GB)
    _LOGGER.info(f"seqcol failed for {genome}, pulling FASTA asset from refgenie")
    try:
        rgc.pull(genome=genome, asset="fasta", tag="default")
        return rgc.seek(
            genome_name=genome,
            asset_name="fasta",
            tag_name="default",
            seek_key="chrom_sizes",
        )
    except Exception:
        _LOGGER.warning(f"Could not fetch chrom.sizes for {genome}")

    return None


def get_gda_path(genome: str, rfg_config=None) -> Union[str, None]:
    """Get a GDA (GenomicDist Annotation) binary for a genome.

    Pulls the Ensembl GTF from refgenie and pre-compiles it to a GDA .bin
    using ``gtars prep``. Falls back to the raw .gtf.gz if compilation fails.
    """
    from refgenconf import RefgenconfError
    from yacman.exceptions import UndefinedAliasError

    from bedboss.bedmaker.utils import get_rgc

    _LOGGER.info(f"Getting GDA annotation for genome: {genome}")
    rgc = get_rgc(rfg_config=rfg_config)

    try:
        gtf_path = rgc.seek(
            genome_name=genome,
            asset_name="ensembl_gtf",
            tag_name="default",
            seek_key="ensembl_gtf",
        )
    except (UndefinedAliasError, RefgenconfError):
        _LOGGER.info(f"ensembl_gtf not local for {genome}, pulling from refgenie")
        try:
            rgc.pull(genome=genome, asset="ensembl_gtf", tag="default")
            gtf_path = rgc.seek(
                genome_name=genome,
                asset_name="ensembl_gtf",
                tag_name="default",
                seek_key="ensembl_gtf",
            )
        except Exception as e:
            _LOGGER.warning(f"Could not fetch GTF for {genome}: {e}")
            return None

    gda_bin_path = gtf_path + ".gda.bin"

    # Return pre-compiled GDA binary if it already exists
    if os.path.exists(gda_bin_path):
        _LOGGER.info(f"GDA annotation (pre-compiled): {gda_bin_path}")
        return gda_bin_path

    # Pre-compile to GDA .bin
    _LOGGER.info(f"Pre-compiling GDA: {gtf_path}")
    result = subprocess.run(
        ["gtars", "prep", "--gtf", gtf_path, "-o", gda_bin_path],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and os.path.exists(gda_bin_path):
        _LOGGER.info(f"GDA annotation (pre-compiled): {gda_bin_path}")
        return gda_bin_path
    else:
        _LOGGER.warning(f"gtars prep (GDA) failed, using raw GTF: {result.stderr}")

    return gtf_path


def get_osm_path_with_precompile(
    osm_path: str,
) -> str:
    """Pre-compile an open signal matrix to .bin if not already done.

    Returns the .bin path if compilation succeeds, otherwise the original path.
    """
    osm_bin_path = osm_path + ".bin"

    if os.path.exists(osm_bin_path):
        _LOGGER.info(f"Open Signal Matrix (pre-compiled): {osm_bin_path}")
        return osm_bin_path

    _LOGGER.info(f"Pre-compiling signal matrix: {osm_path}")
    result = subprocess.run(
        ["gtars", "prep", "--signal-matrix", osm_path, "-o", osm_bin_path],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and os.path.exists(osm_bin_path):
        _LOGGER.info(f"Open Signal Matrix (pre-compiled): {osm_bin_path}")
        return osm_bin_path
    else:
        _LOGGER.warning(f"gtars prep failed, using raw file: {result.stderr}")

    return osm_path
