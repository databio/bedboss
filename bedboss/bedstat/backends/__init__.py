from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.backends.r_backend import RStatBackend

__all__ = ["StatBackend", "RStatBackend", "create_backend"]


def create_backend(name: str, **kwargs) -> StatBackend:
    """Create a statistics computation backend by name.

    :param name: Backend name ("r" or "gtars")
    :param kwargs: Backend-specific keyword arguments
    :return: StatBackend instance
    """
    if name == "r":
        return RStatBackend(**kwargs)
    elif name == "gtars":
        raise NotImplementedError(
            "gtars backend not yet available. Install via PR 2a."
        )
    else:
        raise ValueError(f"Unknown analysis backend: {name!r}. Use 'r' or 'gtars'.")
