import os

import pytest

from bedboss.bedclassifier.bedclassifier import get_bed_classification

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"
FILE_PATH_UNZIPPED = f"{HG19_CORRECT_DIR}/hg19_example1.bed"

SIMPLE_EXAMPLES_DIR = os.path.join(FILE_DIR, "data", "bed", "simpleexamples")
BED1 = f"{SIMPLE_EXAMPLES_DIR}/bed1.bed"
BED2 = f"{SIMPLE_EXAMPLES_DIR}/bed2.bed"
BED3 = f"{SIMPLE_EXAMPLES_DIR}/bed3.bed"

BED_4_PLUS_6 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_4plus6.bed"
BED_6_PLUS_4 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_6plus4.bed"
BED_7_PLUS_3 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_7plus3.bed"
BED_10_PLUS_0 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_10plus0.bed"
BED_12_PLUS_0 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_12plus0.bed"
BED_12_PLUS_3 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_12plus3.bed"
BED_NARROWPEAK = f"{SIMPLE_EXAMPLES_DIR}/test_nrwpk.bed"
BED_NONSTRICT_NARROWPEAK = f"{SIMPLE_EXAMPLES_DIR}/test_ns_nrwpk.bed"  # this has values greater than 1000 in (col 5)
BED_RNA_ELEMENTS = f"{SIMPLE_EXAMPLES_DIR}/test_rna_elements.bed"
BED_BROADPEAK = f"{SIMPLE_EXAMPLES_DIR}/test_brdpk.bed"


class TestBedClassifier:
    def test_classification(
        self,
    ):
        bedclass = get_bed_classification(bed=FILE_PATH)

    def test_get_bed_classification(
        self,
    ):
        bedtype = get_bed_classification(bed=FILE_PATH_UNZIPPED)
        assert bedtype == ("bed6+3", "encode_broadpeak")

    @pytest.mark.parametrize(
        "values",
        [
            (BED1, ("bed6+4", "encode_narrowpeak")),
            (BED2, ("bed6+3", "encode_broadpeak")),
            (BED3, ("bed6+2", "ucsc_bed")),
            (BED_4_PLUS_6, ("bed4+6", "ucsc_bed")),
            (BED_6_PLUS_4, ("bed6+4", "ucsc_bed")),
            (BED_7_PLUS_3, ("bed7+3", "ucsc_bed")),
            (BED_10_PLUS_0, ("bed10+0", "ucsc_bed")),
            (BED_12_PLUS_0, ("bed12+0", "ucsc_bed")),
            (BED_12_PLUS_3, ("bed12+3", "ucsc_bed")),
            (BED_NARROWPEAK, ("bed6+4", "encode_narrowpeak")),
            (
                BED_NONSTRICT_NARROWPEAK,
                ("bed4+6", "ucsc_bed"),
            ),  # has score greater than 1000
            (BED_RNA_ELEMENTS, ("bed6+3", "encode_rna_elements")),
            (BED_BROADPEAK, ("bed6+3", "encode_broadpeak")),
        ],
    )
    def test_get_bed_classifications(self, values):
        # bed1 is encode narrowpeak
        # bed2 is encode broadpeak
        # bed 3 is encode bed6+ (6+2)
        # the others are variations on the columns and ensuring they classify correctly

        bedclass = get_bed_classification(bed=values[0])
        assert bedclass == values[1]

    @pytest.mark.skip(reason="Not implemented")
    def test_from_PEPhub_beds(
        self,
    ):
        """"""
        # TODO implement testing from pephub
        pass


#
# def test_manual_dir_beds():
#     """This test is currently just for local manual testing"""
#     local_dir = "/home/drc/Downloads/test_beds_BED_classifier/"
#     # local_dir = "/home/drc/Downloads/individual_beds/"
#     # local_dir = "/home/drc/Downloads/only_narrowpeaks/"
#     output_dir = "/home/drc/Downloads/BED_CLASSIFIER_OUTPUT/"
#     # local_dir = "/home/drc/Downloads/encode_beds/bedfiles/"
#     # output_dir = "/home/drc/Downloads/encode_beds/output/"
#     # local_dir = "/home/drc/Downloads/single_encode_beds/bedfiles/"
#     # output_dir = "/home/drc/Downloads/single_encode_beds/output/"
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
#
#
# #
# if __name__ == "__main__":
#     # test_get_bed_type()
#     # test_classification()
#     test_manual_dir_beds()
