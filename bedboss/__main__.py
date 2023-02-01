import sys
import logmuse

from bedboss.cli import main


_LOGGER = logmuse.init_logger("bedboss")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
