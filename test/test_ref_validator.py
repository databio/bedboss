import os

from bedboss.refgenome_validator.main import ReferenceValidator

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"


def test_main():
    # dict_result = ReferenceValidator().determine_compatibility(
    #     FILE_PATH,
    #     concise=True,
    # )
    dict_result = ReferenceValidator().determine_compatibility(
        "/home/bnt4me/.bbcache/bedfiles/0/7/0740332b148a613342603e2e483f53e5.bed.gz",
        concise=True,
    )
    # result = ReferenceValidator().predict(
    #     # "/home/bnt4me/.bbcache/bedfiles/0/4/04c46b96264ef40bca93f03b10345da5.bed.gz",
    #     "/home/bnt4me/.bbcache/bedfiles/1/9/19ed879232e44812b1ee35b57792b924.bed.gz",
    # )
    # result

    assert dict_result
