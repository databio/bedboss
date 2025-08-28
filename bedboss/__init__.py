"""Package-level data"""

import logging

import coloredlogs

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

_LOGGER = logging.getLogger("bedboss")
_LOGGER.propagate = False
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BEDBOSS] %(message)s",
)

_LOGGER_PIPESTAT = logging.getLogger("pipestat")
_LOGGER_PIPESTAT.propagate = False
coloredlogs.install(
    logger=_LOGGER_PIPESTAT,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [PIPESTAT] %(message)s",
)

_LOGGER_GENIML = logging.getLogger("geniml")
_LOGGER_GENIML.propagate = False
coloredlogs.install(
    logger=_LOGGER_GENIML,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [GENIML] %(message)s",
)

_LOGGER_BBCONF = logging.getLogger("bbconf")
_LOGGER_BBCONF.propagate = False
coloredlogs.install(
    logger=_LOGGER_BBCONF,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBCONF] %(message)s",
)

_LOGGER_PHC = logging.getLogger("pephubclient")
_LOGGER_PHC.propagate = False
coloredlogs.install(
    logger=_LOGGER_PHC,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [PEPHUBCLIENT] %(message)s",
)

_LOGGER_REF_CONF = logging.getLogger("refgenconf")
_LOGGER_REF_CONF.propagate = False
coloredlogs.install(
    logger=_LOGGER_PHC,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [REFGENCONF] %(message)s",
)
