""" Package-level data """
import logmuse
import coloredlogs


# from bedboss.bedqc.bedqc import bedqc
# from bedboss.bedmaker.bedmaker import BedMaker
# from bedboss.bedstat.bedstat import bedstat
from bedboss._version import __version__
from bedboss.bedboss import (
    run_all,
    insert_pep,
    bedqc,
    BedMaker,
    bedstat,
    run_bedbuncher,
)


__package_name__ = "bedboss"

__author__ = [
    "Oleksandr Khoroshevskyi",
    "Michal Stolarczyk",
    "Ognen Duzlevski",
    "Jose Verdezoto",
    "Bingjie Xue",
]
__email__ = "khorosh@virginia.edu"

__all__ = [
    "__version__",
    "__package_name__",
    "__author__",
    "bedboss",
    "bedqc",
    "bedmaker",
    "BedMaker",
    "bedstat",
    "run_all",
    "insert_pep",
    "run_bedbuncher",
]

_LOGGER = logmuse.init_logger("bedboss")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)
