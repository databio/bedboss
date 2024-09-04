import sys

import logmuse

from bedboss.cli import app

_LOGGER = logmuse.init_logger("bedboss")


def main():
    app(prog_name="bedboss")


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
