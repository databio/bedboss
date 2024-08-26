import logging
import os
import urllib.request

from bedboss.bbuploader.constants import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


# This function is not used in the code anymore
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
