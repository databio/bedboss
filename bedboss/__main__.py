import sys

import logging

from bedboss.cli import app
from bedboss.const import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


def main():
    app(prog_name=PKG_NAME)


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
