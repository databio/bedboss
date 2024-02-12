import os
from tempfile import TemporaryDirectory

from bedboss.bedclassifier import BedClassifier, get_bed_type


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"
FILE_PATH_UNZIPPED = f"{HG19_CORRECT_DIR}/hg19_example1.bed"


def test_classification():
    with TemporaryDirectory() as d:
        bedclass = BedClassifier(input_file=FILE_PATH, output_dir=d)
        print("DEBUG BEDCLASS\n")
        print(bedclass.bed_type)


def test_get_bed_type():
    bedtype = get_bed_type(bed=FILE_PATH_UNZIPPED)
    print("DEBUG BEDTYPE\n")
    print(bedtype)


if __name__ == "__main__":
    print("DEBUG FROM MAIN")
    test_get_bed_type()
    test_classification()
