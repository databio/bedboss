from ubiquerg import VersionInHelpParser
import sys
from typing import Tuple
import pypiper

from bedboss import __version__, __package_name__


def parse_opt() -> Tuple[str, dict]:
    """
    BEDboss parser
    :retrun: Tuple[pipeline, arguments]
    """
    parser = VersionInHelpParser(
        prog=__package_name__,
        description="Warehouse of pipelines for BED-like files: "
        "bedmaker, bedstat, and bedqc.",
        epilog="",
        version=__version__,
    )

    subparser = parser.add_subparsers()
    sub_all = subparser.add_parser(
        "all", help="Run all bedboss pipelines and insert data into bedbase"
    )
    sub_make = subparser.add_parser(
        "make",
        help="A pipeline to convert bed, bigbed, bigwig or bedgraph "
        "files into bed and bigbed formats",
    )
    sub_qc = subparser.add_parser("qc", help="Run quality control on bed file (bedqc)")
    sub_stat = subparser.add_parser(
        "stat",
        help="A pipeline to read a file in BED format and produce metadata "
        "in JSON format.",
    )

    sub_all.add_argument(
        "-s",
        "--sample-name",
        required=True,
        help="name of the sample used to systematically build the output name",
        type=str,
    )
    sub_all.add_argument(
        "-f", "--input-file", required=True, help="Input file", type=str
    )
    sub_all.add_argument(
        "-t",
        "--input-type",
        required=True,
        help="Input type [required] options: (bigwig|bedgraph|bed|bigbed|wig)",
        type=str,
    )
    sub_all.add_argument(
        "-o", "--output_folder", required=True, help="Output folder", type=str
    )
    sub_all.add_argument(
        "-g", "--genome", required=True, help="reference genome (assembly)", type=str
    )
    sub_all.add_argument(
        "-r",
        "--rfg-config",
        required=False,
        help="file path to the genome config file(refgenie)",
        type=str,
    )
    sub_all.add_argument(
        "--chrom-sizes",
        help="a full path to the chrom.sizes required for the bedtobigbed conversion",
        type=str,
        required=False,
    )
    sub_all.add_argument(
        "-n",
        "--narrowpeak",
        help="whether the regions are narrow (transcription factor implies narrow, "
        "histone mark implies broad peaks)",
        type=bool,
        required=False,
    )
    sub_all.add_argument(
        "--standard-chrom",
        help="Standardize chromosome names. Default: False",
        action="store_true",
    )
    sub_all.add_argument(
        "--check-qc",
        help="Check quality control before processing data. Default: True",
        action="store_false",
    )
    sub_all.add_argument(
        "--open-signal-matrix",
        type=str,
        required=False,
        default=None,
        help="a full path to the openSignalMatrix required for the tissue "
        "specificity plots",
    )
    sub_all.add_argument(
        "--ensdb",
        type=str,
        required=False,
        default=None,
        help="A full path to the ensdb gtf file required for genomes not in GDdata ",
    )
    sub_all.add_argument(
        "--bedbase-config",
        dest="bedbase_config",
        type=str,
        help="a path to the bedbase configuration file",
        required=True,
    )
    sub_all.add_argument(
        "-y",
        "--sample-yaml",
        dest="sample_yaml",
        type=str,
        required=False,
        help="a yaml config file with sample attributes to pass on more metadata "
        "into the database",
    )
    sub_all.add_argument(
        "--no-db-commit",
        action="store_true",
        help="skip the JSON commit to the database",
    )
    sub_all.add_argument(
        "--just-db-commit",
        action="store_true",
        help="just commit the JSON to the database",
    )

    # bed_qc
    sub_qc.add_argument(
        "--bedfile", help="a full path to bed file to process", required=True
    )
    sub_qc.add_argument(
        "--outfolder", help="a full path to output log folder.", required=True
    )

    # bed_maker

    sub_make.add_argument(
        "-f",
        "--input-file",
        required=True,
        help="path to the input file",
        type=str,
    )
    sub_make.add_argument(
        "-n",
        "--narrowpeak",
        help="whether the regions are narrow "
        "(transcription factor implies narrow, histone mark implies broad peaks)",
        type=bool,
    )
    sub_make.add_argument(
        "-t",
        "--input-type",
        required=True,
        help="a bigwig or a bedgraph file that will be converted into BED format",
        type=str,
    )
    sub_make.add_argument(
        "-g",
        "--genome",
        required=True,
        help="reference genome",
        type=str,
    )
    sub_make.add_argument(
        "-r",
        "--rfg-config",
        required=True,
        help="file path to the genome config file",
        type=str,
    )
    sub_make.add_argument(
        "-o",
        "--output-bed",
        required=True,
        help="path to the output BED files",
        type=str,
    )
    sub_make.add_argument(
        "--output-bigbed",
        required=True,
        help="path to the folder of output bigBed files",
        type=str,
    )
    sub_make.add_argument(
        "-s",
        "--sample-name",
        required=True,
        help="name of the sample used to systematically build the output name",
        type=str,
    )
    sub_make.add_argument(
        "--chrom-sizes",
        help="a full path to the chrom.sizes required for the bedtobigbed conversion",
        type=str,
        required=False,
    )
    sub_make.add_argument(
        "--standard-chrom",
        help="Standardize chromosome names. Default: False",
        action="store_true",
    )
    # bed_stat
    sub_stat.add_argument(
        "--bedfile", help="a full path to bed file to process", required=True
    )
    sub_stat.add_argument(
        "--open-signal-matrix",
        type=str,
        required=False,
        default=None,
        help="a full path to the openSignalMatrix required for the tissue "
        "specificity plots",
    )

    sub_stat.add_argument(
        "--ensdb",
        type=str,
        required=False,
        default=None,
        help="a full path to the ensdb gtf file required for genomes not in GDdata ",
    )

    sub_stat.add_argument(
        "--bigbed",
        type=str,
        required=False,
        default=None,
        help="a full path to the bigbed files",
    )

    sub_stat.add_argument(
        "--bedbase-config",
        dest="bedbase_config",
        type=str,
        default=None,
        help="a path to the bedbase configuration file",
    )
    sub_stat.add_argument(
        "-y",
        "--sample-yaml",
        dest="sample_yaml",
        type=str,
        required=False,
        help="a yaml config file with sample attributes to pass on more metadata "
        "into the database",
    )
    sub_stat.add_argument(
        "--genome",
        dest="genome_assembly",
        type=str,
        required=True,
        help="genome assembly of the sample",
    )
    sub_stat.add_argument(
        "--no-db-commit",
        action="store_true",
        help="whether the JSON commit to the database should be skipped",
    )
    sub_stat.add_argument(
        "--just-db-commit",
        action="store_true",
        help="whether just to commit the JSON to the database",
    )
    user_argv = sys.argv
    if len(user_argv) > 1:
        pipeline = user_argv[1]
    else:
        pipeline = None
    args = parser.parse_args(user_argv[1:])
    return pipeline, vars(args)
