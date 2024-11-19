import logging
import os
import urllib.request

from bedboss.bbuploader.constants import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


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
