from bedboss.bedboss import main
import os
import subprocess
import pytest
from bbconf import BedBaseConf

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
HG19_CORRECT_DIR = os.path.join(FILE_DIR, "test_data", "bed", "hg19", "correct")
FILE_PATH = f"{HG19_CORRECT_DIR}/sample1.bed.gz"
BIGBED_PATH = os.path.join(
    FILE_DIR, "data", "bigbed", "hg19", "correct", "sample1.bigBed"
)

BEDBASE_CONFIG = os.path.join(FILE_DIR, "test_dependencies", "bedbase_config_test.yaml")
DEPENDENCIES_TEST_SCRIPT = f"{FILE_DIR}/bash_requirements_test.sh"


def test_dependencies():
    # Make sure bedToBigBed etc is in your PATH.
    print("Testing dependencies...")
    key = "PATH"
    value = os.getenv(key)
    test_dep_return_code = subprocess.run([DEPENDENCIES_TEST_SCRIPT], shell=True)
    assert 1 > test_dep_return_code.returncode


def db_setup():
    # Check if the database is setup
    try:
        BedBaseConf(BEDBASE_CONFIG)
    except Exception as err:
        print(f"Error: {err}")
        BedBaseConf(BEDBASE_CONFIG)
        return False
    return True


pytest_db_skip_reason = "Database is not set up... To run this test, set up the database. Go to test/README.md for more information."


@pytest.mark.parametrize(
    "bedfile",
    [
        FILE_PATH,
    ],
)
def test_qc(bedfile, tmpdir):
    qc_passed = main(
        {
            "command": "qc",
            "bedfile": bedfile,
            "outfolder": tmpdir,
        }
    )
    assert qc_passed is None


@pytest.mark.parametrize(
    "bedfile",
    [
        FILE_PATH,
    ],
)
def test_make(bedfile, tmpdir):
    main(
        {
            "command": "make",
            "input_file": bedfile,
            "sample_name": "test",
            "input_type": "bed",
            "genome": "hg19",
            "output_bed": os.path.join(tmpdir, "bed"),
            "output_bigbed": os.path.join(tmpdir, "bigbed"),
            "outfolder": tmpdir,
            "no_db_commit": True,
        }
    )
    assert os.path.isfile(os.path.join(tmpdir, "bed", "sample1.bed.gz"))
    assert os.path.isfile(os.path.join(tmpdir, "bigbed", "sample1.bigBed"))


@pytest.mark.skipif(
    not db_setup(),
    reason=pytest_db_skip_reason,
)
class TestStat:
    @pytest.fixture(scope="session")
    def output_temp_dir(self, tmp_path_factory):
        fn = tmp_path_factory.mktemp("data")
        return fn

    @pytest.mark.parametrize(
        "bedfile, bigbed_file, genome",
        [
            (
                FILE_PATH,
                BIGBED_PATH,
                "hg19",
            )
        ],
    )
    def test_stat(self, bedfile, bigbed_file, genome, output_temp_dir):
        main(
            {
                "command": "stat",
                "bedfile": bedfile,
                "outfolder": output_temp_dir,
                "bedbase_config": BEDBASE_CONFIG,
                "genome": genome,
                "bigbed": bigbed_file,
                "no_db_commit": True,
                "skip_qdrant": True,
            }
        )
        assert True

    case_name = "sample1"

    @pytest.mark.parametrize(
        "file",
        [
            f"{case_name}_cumulative_partitions.png",
            f"{case_name}_expected_partitions.pdf",
            f"{case_name}_partitions.png",
            f"{case_name}_partitions.pdf",
            f"{case_name}_cumulative_partitions.pdf",
            f"{case_name}_chrombins.pdf",
            f"{case_name}_widths_histogram.pdf",
            f"{case_name}_tssdist.pdf",
            f"{case_name}_tssdist.png",
            f"{case_name}_neighbor_distances.pdf",
            f"{case_name}_chrombins.png",
            f"{case_name}_expected_partitions.png",
            f"{case_name}_plots.json",
            f"{case_name}_widths_histogram.png",
            f"{case_name}_neighbor_distances.png",
        ],
    )
    def test_check_file_exists(self, file, output_temp_dir):
        assert os.path.isfile(
            os.path.join(
                output_temp_dir,
                "output",
                "bedstat_output",
                "c557c915a9901ce377ef724806ff7a2c",
                file,
            )
        )


@pytest.mark.skipif(
    not db_setup(),
    reason=pytest_db_skip_reason,
)
class TestAll:
    @pytest.fixture(scope="session")
    def output_temp_dir(self, tmp_path_factory):
        fn = tmp_path_factory.mktemp("data")
        return fn

    @pytest.mark.parametrize(
        "input_file, genome, input_type",
        [
            (
                FILE_PATH,
                "hg19",
                "bed",
            ),
        ],
    )
    def test_boss(self, input_file, genome, input_type, output_temp_dir):
        main(
            {
                "command": "all",
                "input_file": input_file,
                "genome": genome,
                "sample_name": "TestName",
                "input_type": input_type,
                "bedbase_config": BEDBASE_CONFIG,
                "no_db_commit": True,
                "outfolder": output_temp_dir,
                "skip_qdrant": True,
            }
        )
        assert True

    case_name = "sample1"

    @pytest.mark.parametrize(
        "file",
        [
            f"{case_name}_cumulative_partitions.png",
            f"{case_name}_expected_partitions.pdf",
            f"{case_name}_partitions.png",
            f"{case_name}_partitions.pdf",
            f"{case_name}_cumulative_partitions.pdf",
            f"{case_name}_chrombins.pdf",
            f"{case_name}_widths_histogram.pdf",
            f"{case_name}_tssdist.pdf",
            f"{case_name}_tssdist.png",
            f"{case_name}_neighbor_distances.pdf",
            f"{case_name}_chrombins.png",
            f"{case_name}_expected_partitions.png",
            f"{case_name}_plots.json",
            f"{case_name}_widths_histogram.png",
            f"{case_name}_neighbor_distances.png",
        ],
    )
    def test_check_file_exists(self, file, output_temp_dir):
        assert os.path.isfile(
            os.path.join(
                output_temp_dir,
                "output",
                "bedstat_output",
                "c557c915a9901ce377ef724806ff7a2c",
                file,
            )
        )
