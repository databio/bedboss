"""Package-level data"""

import coloredlogs
import logmuse

from bedboss.bbuploader.constants import PKG_NAME

_LOGGER = logmuse.init_logger(PKG_NAME)
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBUPLOADER] %(message)s",
)
