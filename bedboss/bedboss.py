import bedstat
from bedmaker import BedMaker
import os
from .const import *
from .exceptions import OSMException, GenomeException
import urllib.request
import logmuse
from typing import NoReturn
from argparse import ArgumentParser
import sys


_LOGGER = logmuse.init_logger(name="bedboss")
BED_FOLDER_NAME = "bed_files"
BIGBED_FOLDER_NAME = "bigbed_files"


def download_file(url: str, path: str) -> NoReturn:
    """
    Download file from the url to specific location
    :param url: URL of the file
    :param path: Local path with filename
    :return: NoReturn
    """
    _LOGGER.info(f"Downloading file: {url}")
    _LOGGER.info(f"Downloading location: {path}")
    urllib.request.urlretrieve(url, path)
    _LOGGER.info(f"File has been downloaded successfully!")


def get_osm_path(genome: str) -> str:
    """
    By providing genome name download Open Signal Matrix
    :param genome: genome assembly
    :return: path to the Open Signal Matrix
    """
    _LOGGER.info(f"Getting Open Signal Matrix file path...")
    if genome == "hg19":
        osm_name = OS_HG19
    elif genome == "hg38":
        osm_name = OS_HG38
    elif genome == "mm10":
        osm_name = OS_MM10
    else:
        raise OSMException(
            "For this genome open Signal Matrix was not found. Exiting..."
        )
    osm_path = os.path.join(OPEN_SIGNAL_FOLDER, osm_name)
    if not os.path.exists(osm_path):
        if not os.path.exists(OPEN_SIGNAL_FOLDER):
            os.makedirs(OPEN_SIGNAL_FOLDER)
        download_file(url=f"{OPEN_SIGNAL_URL}{osm_name}", path=osm_path)
    return osm_path


def standard_genome_name(input_genome: str) -> str:
    """
    Standardizing user provided genome
    :param input_genome: standardize user provided genome, so bedboss know what genome
    we should use
    :return: genome name string
    """
    _LOGGER.info(f"processing genome name...")
    input_genome = input_genome.strip()
    # TODO: we have to add more genome options and preprocessing of the string
    if input_genome == "hg38" or input_genome == "GRCh38":
        return "hg38"
    elif input_genome == "hg19" or input_genome == "GRCh37":
        return "hg19"
    elif input_genome == "mm10":
        return "mm10"
    else:
        raise GenomeException("Incorrect genome assembly was provided")


def extract_file_name(file_path: str) -> str:
    """
    Extraction file name from file path
    :param file_path: full file path
    :return: file name without extension
    """
    file_name = os.path.basename(file_path)
    file_name = file_name.split(".")[0]
    return file_name


def run_bedboss(
    sample_name: str,
    input_file: str,
    input_type: str,
    output_folder: str,
    genome: str,
    bedbase_config: str,
    rfg_config: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    standard_chrom: bool = False,
    chrom_sizes: str = None,
    open_signal_matrix: str = None,
    ensdb: str = None,
    sample_yaml: str = None,
    just_db_commit: bool = False,
    no_db_commit: bool = False,
) -> NoReturn:
    """
    Running bedmaker, bedqc and bedstat in one package
    :param sample_name: Sample name [required]
    :param input_file: Input file [required]
    :param input_type: Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)
    :param output_folder: Folder, where output should be saved  [required]
    :param genome: genome_assembly of the sample. [required] options: (hg19, hg38) #TODO: add more
    :param bedbase_config: a path to the bedbase configuration file. [required] #TODO: add example
    :param open_signal_matrix: a full path to the openSignalMatrix required for the tissue [optional]
    :param rfg_config: file path to the genome config file [optional]
    :param narrowpeak: whether the regions are narrow
        (transcription factor implies narrow, histone mark implies broad peaks) [optional]
    :param check_qc: set True to run quality control during badmaking [optional] (default: True)
    :param standard_chrom: Standardize chromosome names. [optional] (Default: False)
    :param chrom_sizes: a full path to the chrom.sizes required for the bedtobigbed conversion [optional]
    :param sample_yaml: a yaml config file with sample attributes to pass on MORE METADATA into the database [optional]
    :param ensdb: a full path to the ensdb gtf file required for genomes not in GDdata [optional]
        (basically genomes that's not in GDdata)
    :param just_db_commit: whether just to commit the JSON to the database (default: False)
    :param no_db_commit: whether the JSON commit to the database should be skipped (default: False)
    :return: NoReturn
    """

    file_name = extract_file_name(input_file)
    genome = standard_genome_name(genome)
    cwd = os.getcwd()
    if not rfg_config:
        rfg_config = os.path.join(cwd, "genome_config.yaml")

    # find/download open signal matrix
    if not open_signal_matrix:
        open_signal_matrix = get_osm_path(genome)
    else:
        if not os.path.exists(open_signal_matrix):
            open_signal_matrix = get_osm_path(genome)

    if not sample_yaml:
        sample_yaml = f"{sample_name}.yaml"

    output_bed = os.path.join(output_folder, BED_FOLDER_NAME, f"{file_name}.bed.gz")
    output_bigbed = os.path.join(output_folder, BIGBED_FOLDER_NAME)
    _LOGGER.info(f"output_bed = {output_bed}")
    _LOGGER.info(f"output_bigbed = {output_bigbed}")

    # TODO: should we keep bed and bigfiles in output folder?
    # set env for bedstat:
    output_folder_bedstat = os.path.join(output_folder, "output")
    os.environ["BEDBOSS_OUTPUT_PATH"] = output_folder_bedstat

    BedMaker(
        input_file=input_file,
        input_type=input_type,
        output_bed=output_bed,
        output_bigbed=output_bigbed,
        sample_name=sample_name,
        genome=genome,
        rfg_config=rfg_config,
        narrowpeak=narrowpeak,
        check_qc=check_qc,
        standard_chrom=standard_chrom,
        chrom_sizes=chrom_sizes,
    ).make()

    bedstat.run_bedstat(
        bedfile=output_bed,
        bigbed=output_bigbed,
        genome_assembly=genome,
        ensdb=ensdb,
        open_signal_matrix=open_signal_matrix,
        bedbase_config=bedbase_config,
        sample_yaml=sample_yaml,
        just_db_commit=just_db_commit,
        no_db_commit=no_db_commit,
    )


# run_bedboss(
#     sample_name="new",
#     input_file="/home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/files/hg38/AML_db1.bed.gz",
#     input_type="bed",
#     output_folder="../test_f",
#     genome="hg38",
#     rfg_config="../test_f/cfg_test.yaml",
#     bedbase_config="/home/bnt4me/Virginia/repos/bedboss/bedboss/config_db_local.yaml",
#     chrom_sizes="/home/bnt4me/Virginia/repos/bedboss/test_f/data/2230c535660fb4774114bfa966a62f823fdb6d21acf138d4/fasta/default"
# )


def _parse_cmdl():
    parser = ArgumentParser(
        description="Running bedmaker, bedqc and bedstat in one package."
        "And uploading all data to the bedbase db",
        usage="""e.g.
sample_name="new",
input_file="/home/bnt4me/Virginia/bed_base_all/bedbase/bedbase_tutorial/files/hg38/AML_db1.bed.gz",
input_type="bed",
output_folder="../test_f",
genome="hg38",
rfg_config="../test_f/cfg_test.yaml",
bedbase_config="/home/bnt4me/Virginia/repos/bedboss/bedboss/config_db_local.yaml",
chrom_sizes="/home/bnt4me/Virginia/repos/bedboss/test_f/data/2230c535660fb4774114bfa966a62f823fdb6d21acf138d4/fasta/default"
""",
    )
    parser.add_argument(
        "-s",
        "--sample-name",
        required=True,
        help="name of the sample used to systematically build the output name",
        type=str,
    )
    parser.add_argument(
        "-f", "--input-file", required=True, help="Input file", type=str
    )
    parser.add_argument(
        "-t",
        "--input-type",
        required=True,
        help="Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)",
        type=str,
    )
    parser.add_argument(
        "-o", "--output_folder", required=True, help="Output folder", type=str
    )
    parser.add_argument(
        "-g", "--genome", required=True, help="reference genome", type=str
    )
    parser.add_argument(
        "-r",
        "--rfg-config",
        required=False,
        help="file path to the genome config file",
        type=str,
    )
    parser.add_argument(
        "--chrom-sizes",
        help="a full path to the chrom.sizes required for the bedtobigbed conversion",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-n",
        "--narrowpeak",
        help="whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks)",
        type=bool,
        required=False,
    )
    parser.add_argument(
        "--standard-chrom",
        help="Standardize chromosome names. Default: False",
        action="store_true",
    )
    parser.add_argument(
        "--check-qc",
        help="Standardize chromosome names. Default: True",
        action="store_false",
    )
    parser.add_argument(
        "--open-signal-matrix",
        type=str,
        required=False,
        default=None,
        help="a full path to the openSignalMatrix required for the tissue "
        "specificity plots",
    )
    parser.add_argument(
        "--ensdb",
        type=str,
        required=False,
        default=None,
        help="a full path to the ensdb gtf file required for genomes not in GDdata ",
    )
    parser.add_argument(
        "--bedbase-config",
        dest="bedbase_config",
        type=str,
        help="a path to the bedbase configuration file",
        required=True,
    )
    parser.add_argument(
        "-y",
        "--sample-yaml",
        dest="sample_yaml",
        type=str,
        required=False,
        help="a yaml config file with sample attributes to pass on more metadata "
        "into the database",
    )
    parser.add_argument(
        "--no-db-commit",
        action="store_true",
        help="whether the JSON commit to the database should be skipped",
    )
    parser.add_argument(
        "--just-db-commit",
        action="store_true",
        help="whether just to commit the JSON to the database",
    )
    args = parser.parse_args(sys.argv[1:])

    return args


def main():
    args = _parse_cmdl()
    args_dict = vars(args)
    run_bedboss(**args_dict)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
