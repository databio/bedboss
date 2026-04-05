from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.backends.r_backend import RStatBackend
from bedboss.const import BACKEND_GTARS, BACKEND_R

__all__ = ["StatBackend", "RStatBackend", "create_backend", "build_backend"]


def create_backend(name: str, **kwargs) -> StatBackend:
    """Create a statistics computation backend by name.

    Low-level factory: pass backend-specific kwargs directly. Most callers
    should use :func:`build_backend` instead, which handles backend-specific
    prerequisites (e.g. starting an RServiceManager for the R backend).

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


def build_backend(name: str) -> StatBackend:
    """Build a backend with any backend-specific prerequisites attached.

    High-level constructor used by batch orchestrators. Handles the lifetime
    of backend-internal resources (e.g. starts an RServiceManager for the R
    backend so the single R process is reused across all files in a batch).

    Callers are responsible for calling :meth:`StatBackend.cleanup` when
    done to release backend-held resources. See `StatBackend` as a context
    manager for automatic cleanup.

    :param name: Backend name (BACKEND_R or BACKEND_GTARS)
    :return: StatBackend instance ready for batch processing
    """
    if name == BACKEND_R:
        from bedboss.bedstat.r_service import RServiceManager

        return create_backend(name, r_service=RServiceManager())
    return create_backend(name)
