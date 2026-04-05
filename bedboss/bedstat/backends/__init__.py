from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.backends.gtars_backend import GtarsStatBackend
from bedboss.bedstat.backends.gtars_py_backend import GtarsPyStatBackend
from bedboss.bedstat.backends.r_backend import RStatBackend
from bedboss.const import BACKEND_GTARS, BACKEND_GTARS_PY, BACKEND_R

__all__ = [
    "StatBackend",
    "RStatBackend",
    "GtarsStatBackend",
    "GtarsPyStatBackend",
    "create_backend",
]


def create_backend(name: str, **kwargs) -> StatBackend:
    """Create a statistics computation backend by name.

    :param name: Backend name (BACKEND_R, BACKEND_GTARS, BACKEND_GTARS_PY)
    :param kwargs: Backend-specific keyword arguments
    :return: StatBackend instance
    """
    if name == BACKEND_R:
        return RStatBackend(**kwargs)
    elif name == BACKEND_GTARS:
        return GtarsStatBackend(**kwargs)
    elif name == BACKEND_GTARS_PY:
        return GtarsPyStatBackend(**kwargs)
    else:
        raise ValueError(
            f"Unknown analysis backend: {name!r}. Use 'r', 'gtars', or 'gtars-py'."
        )
