import logging
import os
from pathlib import Path
from typing import Union

import pypiper

from bedboss.bedstat.backends import StatBackend
from bedboss.const import (
    HOME_PATH,
    OPEN_SIGNAL_FOLDER_NAME,
    OPEN_SIGNAL_URL,
    OS_HG19,
    OS_HG38,
    OS_MM10,
)
from bedboss.exceptions import OpenSignalMatrixException
from bedboss.utils import download_file

_LOGGER = logging.getLogger("bedboss")

SCHEMA_PATH_BEDSTAT = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "pep_schema.yaml"
)


def get_osm_path(genome: str, out_path: str = None) -> Union[str, None]:
    """
    By providing genome name download Open Signal Matrix

    :param genome: genome assembly
    :param out_path: working directory, where osm should be saved. If None, current working directory will be used
    :return: path to the Open Signal Matrix
    """
    _LOGGER.info("Getting Open Signal Matrix file path...")
    if genome == "hg19" or genome == "GRCh37":
        osm_name = OS_HG19
    elif genome == "hg38" or genome == "GRCh38":
        osm_name = OS_HG38
    elif genome == "mm10" or genome == "GRCm38":
        osm_name = OS_MM10
    else:
        raise OpenSignalMatrixException(
            "For this genome open Signal Matrix was not found."
        )
    if not out_path:
        osm_folder = os.path.join(HOME_PATH, OPEN_SIGNAL_FOLDER_NAME)
    else:
        osm_folder = os.path.join(out_path, OPEN_SIGNAL_FOLDER_NAME)

    osm_path = os.path.join(osm_folder, osm_name)
    if not os.path.exists(osm_path):
        os.makedirs(osm_folder, exist_ok=True)
        download_file(
            url=f"{OPEN_SIGNAL_URL}{osm_name}",
            path=osm_path,
            no_fail=True,
        )
    _LOGGER.info(f"Open Signal Matrix file path: {osm_path}")
    return osm_path


def bedstat(
    bedfile: str,
    genome: str,
    outfolder: str,
    backend: StatBackend,
    bed_digest: str = None,
    ensdb: str = None,
    open_signal_matrix: str = None,
    just_db_commit: bool = False,
    rfg_config: Union[str, Path] = None,
    pm: pypiper.PipelineManager = None,
) -> dict:
    """
    Run bedstat pipeline — compute statistics for a BED file using the
    provided analysis backend.

    The backend is passed in as an instance so callers can reuse it across
    many files (amortizing R service startup, FASTA loads, etc.) without
    this function managing backend lifetime. Build one via
    :func:`bedboss.bedstat.backends.build_backend` at the top of a batch
    and call :meth:`StatBackend.cleanup` (or use the backend as a context
    manager) when done.

    :param str bedfile: the full path to the bed file to process
    :param str genome: genome assembly of the sample
    :param str outfolder: The folder for storing the pipeline results.
    :param StatBackend backend: statistics backend instance (reused across
        calls; lifetime managed by the caller)
    :param str bed_digest: the digest of the bed file. Defaults to None.
    :param str ensdb: a full path to the ensdb gtf file
    :param str open_signal_matrix: a full path to the openSignalMatrix
    :param bool just_db_commit: if True, skip computation and just read existing results
    :param str rfg_config: path to the refgenie config file
    :param pm: pypiper object
    :return: dict with statistics and plots metadata
    """
    # Resolve open signal matrix
    if not open_signal_matrix or not os.path.exists(open_signal_matrix):
        try:
            open_signal_matrix = get_osm_path(genome)
        except OpenSignalMatrixException:
            _LOGGER.warning(
                f"Open Signal Matrix was not found for {genome}. Skipping..."
            )

    return backend.compute(
        bedfile=bedfile,
        genome=genome,
        outfolder=outfolder,
        bed_digest=bed_digest,
        ensdb=ensdb,
        open_signal_matrix=open_signal_matrix,
        just_db_commit=just_db_commit,
        rfg_config=rfg_config,
        pm=pm,
    )
