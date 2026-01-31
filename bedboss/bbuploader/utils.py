import logging
import os
import urllib.request

from bedboss.bbuploader.constants import DEFAULT_GEO_TAG, PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


def build_gse_identifier(gse: str, geo_tag: str) -> str:
    """
    Build GSE identifier for database storage that includes the tag.
    For backward compatibility, 'samples' tag uses plain GSE name.

    :param gse: GEO series number (e.g., GSE123456)
    :param geo_tag: GEO tag ('samples' or 'series')
    :return: identifier for database (e.g., 'GSE123456' or 'GSE123456:series')
    """
    if geo_tag == DEFAULT_GEO_TAG:
        return gse
    return f"{gse}:{geo_tag}"


def download_file(file_url: str, local_file_path: str, force: bool = False) -> None:
    """
    Download file using ftp url

    :param file_url: downloading url
    :param local_file_path: path to the file or file name
    :param force: Rewrite if file exists
    :return: None
    """
    if force or not os.path.isfile(local_file_path):
        _LOGGER.info(f"Downloading file: '{file_url}' to: '{local_file_path}'")
        urllib.request.urlretrieve(file_url, local_file_path)
    else:
        _LOGGER.info(f"File {local_file_path} already exists. Skipping downloading.")


def create_gsm_sub_name(name: str) -> str:
    """
    Create gse subfolder name. e.g.
        gse123456 -> gsm123nnn
        gse123 -> gsennn
        gse1234-> gse1nnn
        gse1 -> gsennn

    ! This function was copied from geopephub utils

    :param name: gse name
    :return: gse subfolder name
    """

    len_name = len(name)

    if len_name <= 6:
        return """gsmnnn"""
    else:
        # return name[:6] + "n" * (len_name - 6)
        return name[:-3] + "n" * 3


def middle_underscored(s: str) -> str:
    """
    Return substring between the first and last underscore.
    If there are fewer than 3 underscores, return the original string unchanged.

    :param s: input string
    :return: substring between first and last underscore or original string
    """

    if s.count("_") < 3:
        return s
    parts = s.split("_")
    return "_".join(parts[1:-1])
