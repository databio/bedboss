"""Package-level data"""

from importlib.metadata import version

__version__ = version(__package__)
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
