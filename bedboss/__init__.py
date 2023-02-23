""" Package-level data """
from bedboss import *
from bedboss.bedqc import bedqc
from bedboss.bedmaker import bedmaker
from bedboss.bedstat import bedstat

__version__ = "0.1.0-dev1"
__package_name__ = "bedboss"
__author__ = [
    "Michal Stolarczyk",
    "Ognen Duzlevski",
    "Jose Verdezoto",
    "Bingjie Xue",
    "Oleksandr Khoroshevskyi",
]
__email__ = "khorosh@virginia.edu"

__all__ = ["__version__", "__package_name__", "__author__", "bedqc", "bedmaker", "bedstat"]
