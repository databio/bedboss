from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.backends.r_backend import RStatBackend
from bedboss.const import BACKEND_GTARS, BACKEND_R

__all__ = ["StatBackend", "RStatBackend", "create_backend"]


def create_backend(name: str, **kwargs) -> StatBackend:
    """Create a statistics computation backend by name.

    :param name: Backend name (BACKEND_R or BACKEND_GTARS)
    :param kwargs: Backend-specific keyword arguments
    :return: StatBackend instance
    """
    if name == BACKEND_R:
        return RStatBackend(**kwargs)
    elif name == BACKEND_GTARS:
        raise NotImplementedError("gtars backend not yet available. Install via PR 2a.")
    else:
        raise ValueError(f"Unknown analysis backend: {name!r}. Use 'r' or 'gtars'.")
