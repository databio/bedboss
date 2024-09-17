import os

from bedboss.refgenome_validator import ReferenceValidator


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"


def test_main():
    ff = ReferenceValidator().determine_compatibility(
        FILE_PATH,
        concise=True,
    )
    # ff = ReferenceValidator().determine_compatibility(
    #     "/home/bnt4me/.bbcache/bedfiles/3/2/GSE244926_mm39_LPx6_oligofile.bed.gz",
    #     concise=True,
    # )
    ff
