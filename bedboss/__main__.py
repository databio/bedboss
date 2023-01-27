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

# if __name__ == "__main__":
#     try:
#         run_bedboss(sample_name="strff",
#             input_file="strff",
#             input_type="strff",
#             output_folder="strff",
#             genome="strff",
#             bedbase_config="strff",)
#
#     except KeyboardInterrupt:
#         print("Pipeline aborted.")
#         sys.exit(1)
