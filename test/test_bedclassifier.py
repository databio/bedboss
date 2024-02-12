import os
from tempfile import TemporaryDirectory

from bedboss.bedclassifier import BedClassifier


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"


def test_classification():
    with TemporaryDirectory() as d:
        bedclass = BedClassifier(input_file=FILE_PATH, output_dir=d)
