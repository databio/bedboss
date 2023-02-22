from bedboss.bedboss import main
from bedboss.bedqc import bedqc
import os
import subprocess
import pytest

# FILE_PATH = "/home/bnt4me/virginia/repos/bedbase_all/bedboss/test/data/bed/hg19/correct/hg19_example1.bed"
FILE_PATH = "/home/bnt4me/virginia/repos/bedbase_all/bedboss/test/data/bed/hg19/correct/sample1.bed.gz"
BEDBASE_CONFIG = "/home/bnt4me/virginia/repos/bedbase_all/bedboss/test/bedbase_config_test.yaml"
DEPENDENCIES_TEST_SCRIPT = "./test_dependencies/run_test.sh"


def test_dependencies():
    test_dep_return_code = subprocess.run([DEPENDENCIES_TEST_SCRIPT], shell=True)

    assert 1 > test_dep_return_code.returncode


@pytest.mark.parametrize(
    "bedfile",
    [
        FILE_PATH,
    ],
)
def test_qc(bedfile, tmpdir):
    qc_passed = main(
        {
            "pipeline": "qc",
            "bedfile": bedfile,
            "outfolder": tmpdir,
        }
    )
    assert qc_passed


@pytest.mark.parametrize(
    "bedfile",
    [
        FILE_PATH,
    ],
)
def test_make(bedfile, tmpdir):
    main(
        {
            "pipeline": "make",
            "input_file": bedfile,
            "sample_name": "test",
            "input_type": "bed",
            "genome": "hg19",
            "output_bed": os.path.join(tmpdir, "bed"),
            "output_bigbed": os.path.join(tmpdir, "bigbed"),
            "outfolder": tmpdir,
        }
    )
    assert os.path.isfile(os.path.join(tmpdir, "bed", "sample1.bed.gz"))
    assert os.path.isfile(os.path.join(tmpdir, "bigbed", "sample1.bigBed"))

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
def test_boss(input_file, genome, sample_name, input_type, bedbase_config, tmpdir):
    main({
        "pipeline": "all",
        "input_file": input_file,
        "genome": genome,
        "sample_name": sample_name,
        "input_type": input_type,
        "bedbase_config": bedbase_config,
        "output_folder": tmpdir,
        "no_db_commit": True,
        "outfolder": tmpdir,
    }
    )

    print(os.listdir(os.path.join(tmpdir, "output", "bedstat_output", "c557c915a9901ce377ef724806ff7a2c")))
    assert True
