from bedboss import bedstat, bedmaker, run_all
from bedboss.bedqc import bedqc
import os
import subprocess
import pytest

FILE_PATH = "./test/data/bed/hg19/correct/hg19_example1.bed"
BEDBASE_CONFIG = "./test/bedbase_cofig_test.yaml"

def test_dependencies():
    test_dep_return_code = subprocess.run(["./test_dependencies/run_test.sh"], shell=True)

    assert 1 > test_dep_return_code.returncode

@pytest.mark.parametrize(
    "bedfile",
    [
        (
            FILE_PATH,
        ),
    ]
)
def test_just_qc(bedfile, tmpdir):
    bedqc.bedqc(
        bedfile="./test/data/bed/hg19/correct/hg19_example1.bed",
        outfolder="./test/bedqc",
    )
    assert True

class TestAllBedboss:
    """
    Testing overall functionality (smoke test)
    """
    @pytest.mark.parametrize(
        "input_file, genome, sample_name, input_type, bedbase_config",
        [
            (
                FILE_PATH,
                "hg19",
                "small_test",
                "bed",
                BEDBASE_CONFIG
            ),
        ]
    )
    def test_this_code(self, input_file, genome, sample_name, input_type, bedbase_config, tmpdir):
        print("Hi")
        run_all(
            input_file=input_file,
            genome=genome,
            sample_name=sample_name,
            input_type=input_type,
            bedbase_config=bedbase_config,
            output_folder=tmpdir,
            no_db_commit=True,
        )

        print(os.listdir())
        assert True
