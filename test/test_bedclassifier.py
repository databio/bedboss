import os

import pytest

from bedboss.bedclassifier import (
    get_bed_classification,
)
from bedboss.models import BedClassificationOutput, DATA_FORMAT


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"
FILE_PATH_UNZIPPED = f"{HG19_CORRECT_DIR}/hg19_example1.bed"

SIMPLE_EXAMPLES_DIR = os.path.join(FILE_DIR, "data", "bed", "simpleexamples")
BED1 = f"{SIMPLE_EXAMPLES_DIR}/bed1.bed"
BED2 = f"{SIMPLE_EXAMPLES_DIR}/bed2.bed"
BED3 = f"{SIMPLE_EXAMPLES_DIR}/bed3.bed"
BED_4_PLUS_5 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_4plus5.bed"
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
BED_7_01 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_7_01.bed"
BED_7_02 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_7_02.bed"
BED_7_03 = f"{SIMPLE_EXAMPLES_DIR}/test_bed_7_03.bed"
BED_GAPPED_PEAK = f"{SIMPLE_EXAMPLES_DIR}/example.gappedPeak.gz"
BED_GAPPED_PEAK_RS = f"{SIMPLE_EXAMPLES_DIR}/example_2.gappedPeak.gz"


class TestBedClassifier:
    def test_classification(
        self,
    ):
        bedclass = get_bed_classification(bed=FILE_PATH)

    def test_get_bed_classification(
        self,
    ):
        bedtype = get_bed_classification(bed=FILE_PATH_UNZIPPED)
        assert bedtype == BedClassificationOutput(
            bed_compliance="bed6+3",
            data_format=DATA_FORMAT.ENCODE_BROADPEAK,
            compliant_columns=6,
            non_compliant_columns=3,
        )

    @pytest.mark.parametrize(
        "values",
        [
            (
                BED1,
                BedClassificationOutput(
                    bed_compliance="bed6+4",
                    data_format=DATA_FORMAT.ENCODE_NARROWPEAK,
                    compliant_columns=6,
                    non_compliant_columns=4,
                ),
            ),
            (
                BED2,
                BedClassificationOutput(
                    bed_compliance="bed6+3",
                    data_format=DATA_FORMAT.ENCODE_BROADPEAK,
                    compliant_columns=6,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED3,
                BedClassificationOutput(
                    bed_compliance="bed6+2",
                    data_format=DATA_FORMAT.BED_LIKE,
                    compliant_columns=6,
                    non_compliant_columns=2,
                ),
            ),
            (
                BED_4_PLUS_5,
                BedClassificationOutput(
                    bed_compliance="bed6+3",
                    data_format=DATA_FORMAT.ENCODE_BROADPEAK_RS,
                    compliant_columns=6,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED_4_PLUS_6,
                BedClassificationOutput(
                    bed_compliance="bed6+4",
                    data_format=DATA_FORMAT.BED_LIKE_RS,
                    compliant_columns=6,
                    non_compliant_columns=4,
                ),
            ),
            (
                BED_6_PLUS_4,
                BedClassificationOutput(
                    bed_compliance="bed6+4",
                    data_format=DATA_FORMAT.BED_LIKE,
                    compliant_columns=6,
                    non_compliant_columns=4,
                ),
            ),
            (
                BED_7_PLUS_3,
                BedClassificationOutput(
                    bed_compliance="bed7+3",
                    data_format=DATA_FORMAT.BED_LIKE,
                    compliant_columns=7,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED_7_01,
                BedClassificationOutput(
                    bed_compliance="bed7+0",
                    data_format=DATA_FORMAT.UCSC_BED_RS,
                    compliant_columns=7,
                    non_compliant_columns=0,
                ),
            ),
            (
                BED_7_02,
                BedClassificationOutput(
                    bed_compliance="bed7+0",
                    data_format=DATA_FORMAT.UCSC_BED,
                    compliant_columns=7,
                    non_compliant_columns=0,
                ),
            ),
            (
                BED_7_03,
                BedClassificationOutput(
                    bed_compliance="bed6+1",
                    data_format=DATA_FORMAT.BED_LIKE_RS,
                    compliant_columns=6,
                    non_compliant_columns=1,
                ),
            ),
            (
                BED_10_PLUS_0,
                BedClassificationOutput(
                    bed_compliance="bed10+0",
                    data_format=DATA_FORMAT.UCSC_BED,
                    compliant_columns=10,
                    non_compliant_columns=0,
                ),
            ),
            (
                BED_12_PLUS_0,
                BedClassificationOutput(
                    bed_compliance="bed12+0",
                    data_format=DATA_FORMAT.UCSC_BED,
                    compliant_columns=12,
                    non_compliant_columns=0,
                ),
            ),
            (
                BED_12_PLUS_3,
                BedClassificationOutput(
                    bed_compliance="bed12+3",
                    data_format=DATA_FORMAT.BED_LIKE,
                    compliant_columns=12,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED_NARROWPEAK,
                BedClassificationOutput(
                    bed_compliance="bed6+4",
                    data_format=DATA_FORMAT.ENCODE_NARROWPEAK,
                    compliant_columns=6,
                    non_compliant_columns=4,
                ),
            ),
            (
                BED_NONSTRICT_NARROWPEAK,
                BedClassificationOutput(
                    bed_compliance="bed6+4",
                    data_format=DATA_FORMAT.ENCODE_NARROWPEAK_RS,
                    compliant_columns=6,
                    non_compliant_columns=4,
                ),
            ),
            (
                BED_RNA_ELEMENTS,
                BedClassificationOutput(
                    bed_compliance="bed6+3",
                    data_format=DATA_FORMAT.ENCODE_RNA_ELEMENTS,
                    compliant_columns=6,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED_BROADPEAK,
                BedClassificationOutput(
                    bed_compliance="bed6+3",
                    data_format=DATA_FORMAT.ENCODE_BROADPEAK,
                    compliant_columns=6,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED_GAPPED_PEAK,
                BedClassificationOutput(
                    bed_compliance="bed12+3",
                    data_format=DATA_FORMAT.ENCODE_GAPPEDPEAK,
                    compliant_columns=12,
                    non_compliant_columns=3,
                ),
            ),
            (
                BED_GAPPED_PEAK_RS,
                BedClassificationOutput(
                    bed_compliance="bed12+3",
                    data_format=DATA_FORMAT.ENCODE_GAPPEDPEAK_RS,
                    compliant_columns=12,
                    non_compliant_columns=3,
                ),
            ),
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
