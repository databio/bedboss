import os
import pytest
from tempfile import TemporaryDirectory

from bedboss.bedclassifier import BedClassifier, get_bed_type


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"
FILE_PATH_UNZIPPED = f"{HG19_CORRECT_DIR}/hg19_example1.bed"


@pytest.mark.skip(reason="Illegal seek during teardown.")
def test_classification():
    with TemporaryDirectory() as d:
        bedclass = BedClassifier(input_file=FILE_PATH, output_dir=d)


def test_get_bed_type():
    bedtype = get_bed_type(bed=FILE_PATH_UNZIPPED)
    assert bedtype == "bed6+3"


@pytest.mark.skip(reason="Not implemented")
def test_from_PEPhub_beds():
    """"""
    # TODO implement testing from pephub
    pass


# def test_manual_dir_beds():
#     """This test is currently just for local manual testing"""
#     # local_dir = "/home/drc/Downloads/test_beds_BED_classifier/"
#     # local_dir = "/home/drc/Downloads/individual_beds/"
#     local_dir = "/home/drc/Downloads/only_narrowpeaks/"
#     output_dir = "/home/drc/Downloads/BED_CLASSIFIER_OUTPUT/"
#
#     for root, dirs, files in os.walk(local_dir):
#         for file in files:
#             print(file)
#             file_path = os.path.join(root, file)
#             print(file_path)
#             bedclass = BedClassifier(
#                 input_file=file_path, output_dir=output_dir, bed_digest=file
#             )
#             print("\nDEBUG BEDCLASS\n")
#             print(bedclass.bed_type)
#             print("+++++++++++++++++++")


# if __name__ == "__main__":
#     test_get_bed_type()
#     test_classification()
# test_manual_dir_beds()
