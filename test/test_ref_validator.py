import os

from bedboss.refgenome_validator import ReferenceValidator


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"


def test_main():
    dict_result = ReferenceValidator().determine_compatibility(
        FILE_PATH,
        concise=True,
    )

    assert dict_result
