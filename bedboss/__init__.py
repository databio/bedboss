""" Package-level data """

import logging

import coloredlogs
import logmuse

from bedboss._version import __version__

__package_name__ = "bedboss"

__author__ = [
    "Oleksandr Khoroshevskyi",
    "Michal Stolarczyk",
    "Ognen Duzlevski",
    "Jose Verdezoto",
    "Bingjie Xue",
    "Donald Campbell",
]
__email__ = "khorosh@virginia.edu"

__all__ = [
    "__version__",
    "__package_name__",
    "__author__",
]

_LOGGER = logmuse.init_logger("bedboss")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BEDBOSS] %(message)s",
)

_LOGGER_PIPESTAT = logging.getLogger("pipestat")
coloredlogs.install(
    logger=_LOGGER_PIPESTAT,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [PIPESTAT] %(message)s",
)

_LOGGER_GENIML = logging.getLogger("geniml")
coloredlogs.install(
    logger=_LOGGER_GENIML,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [GENIML] %(message)s",
)

_LOGGER_BBCONF = logging.getLogger("bbconf")
coloredlogs.install(
    logger=_LOGGER_BBCONF,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBCONF] %(message)s",
)

_LOGGER_BBCONF = logging.getLogger("pephubclient")
coloredlogs.install(
    logger=_LOGGER_BBCONF,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [PEPHUBCLIENT] %(message)s",
)
