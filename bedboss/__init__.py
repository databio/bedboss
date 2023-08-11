""" Package-level data """
from bedboss import *
from bedboss.bedqc import bedqc
from bedboss.bedmaker import bedmaker
from bedboss.bedstat import bedstat
from bedboss._version import __version__
import logmuse

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
    "bedqc",
    "bedmaker",
    "bedstat",
]

logmuse.init_logger(__version__)
