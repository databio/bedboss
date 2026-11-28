import logging
import os
import urllib.request

from bedboss.bbuploader.constants import DEFAULT_GEO_TAG, PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


def build_gse_identifier(gse: str, geo_tag: str) -> str:
    """
    Build GSE identifier for database storage that includes the tag.

    For backward compatibility, 'samples' tag uses plain GSE name.

    Args:
        gse: GEO series number (e.g., GSE123456).
        geo_tag: GEO tag ('samples' or 'series').

    Returns:
        Identifier for database (e.g., 'GSE123456' or 'GSE123456:series').
    """
    if geo_tag == DEFAULT_GEO_TAG:
        return gse
    return f"{gse}:{geo_tag}"


def download_file(
    file_url: str,
    local_file_path: str,
    force: bool = False,
    no_fail: bool = False,
) -> None:
    """
    Download a file from an FTP/HTTP url, skipping the download if the file
    already exists locally unless ``force`` is set.

    Args:
        file_url: Downloading url.
        local_file_path: Path to the local file (including filename).
        force: Re-download and overwrite even if the file already exists.
        no_fail: If True, log and continue instead of raising when the download fails.
    """
    if not force and os.path.isfile(local_file_path):
        _LOGGER.info(f"File '{local_file_path}' already exists. Skipping downloading.")
        return
    _LOGGER.info(f"Downloading file: '{file_url}' to: '{local_file_path}'")
    try:
        urllib.request.urlretrieve(file_url, local_file_path)
    except Exception as e:
        _LOGGER.error(f"File download failed: {e}")
        if not no_fail:
            raise
        _LOGGER.error("File download failed. Continuing anyway (no_fail=True).")


def create_geo_sub_name(name: str) -> str:
    """
    Build the GEO-style grouping subfolder for a GEO accession.

    Mirrors GEO's FTP layout, where the last three digits of the accession are
    replaced with 'nnn' (e.g. GSM1234567 -> gsm1234nnn, GSE310476 -> gse310nnn).
    Accessions with three or fewer digits keep their 3-letter prefix and collapse
    to '<prefix>nnn' (e.g. GSE123 -> gsennn, GSE1 -> gsennn). Adapted from
    geopephub utils.

    Args:
        name: GEO accession (GSM or GSE), case-insensitive.

    Returns:
        GEO-style subfolder name (lowercased), or 'unknown' if name is empty.
    """
    name = name.strip().lower()
    if not name:
        return "unknown"
    if len(name) <= 6:  # 3-letter prefix + up to 3 digits
        return f"{name[:3]}nnn"
    return f"{name[:-3]}nnn"


def middle_underscored(s: str) -> str:
    """
    Return substring between the first and last underscore.

    If there are fewer than 3 underscores, return the original string unchanged.

    Args:
        s: Input string.

    Returns:
        Substring between first and last underscore, or original string.
    """
    if s.count("_") < 3:
        return s
    parts = s.split("_")
    return "_".join(parts[1:-1])
