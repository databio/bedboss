from ubiquerg import VersionInHelpParser
from argparse import ArgumentParser
import logmuse

from bedboss._version import __version__


def build_argparser() -> ArgumentParser:
    """
    BEDboss parser
    :retrun: Tuple[pipeline, arguments]
    """
    parser = VersionInHelpParser(
        prog="bedboss",
        description="Warehouse of pipelines for BED-like files: "
        "bedmaker, bedstat, and bedqc.",
        epilog="",
        version=__version__,
    )

    subparser = parser.add_subparsers(dest="command")
    sub_all = subparser.add_parser(
        "all", help="Run all bedboss pipelines and insert data into bedbase"
    )
    sub_all_pep = subparser.add_parser(
        "all-pep",
        help="Run all bedboss pipelines using one PEP and insert data into bedbase",
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
        "--outfolder",
        required=True,
        help="Pipeline output folder [Required]",
        type=str,
    )

    sub_all.add_argument(
        "-s",
        "--sample-name",
        required=True,
        help="name of the sample used to systematically build the output name [Required]",
        type=str,
    )
    sub_all.add_argument(
        "-f", "--input-file", required=True, help="Input file [Required]", type=str
    )
    sub_all.add_argument(
        "-t",
        "--input-type",
        required=True,
        help="Input type [Required] options: (bigwig|bedgraph|bed|bigbed|wig)",
        type=str,
    )
    sub_all.add_argument(
        "-g",
        "--genome",
        required=True,
        help="reference genome (assembly) [Required]",
        type=str,
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
        help="whether it's a narrowpeak file",
        action="store_true",
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
        help="a path to the bedbase configuration file [Required]",
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
    sub_all.add_argument(
        "--skip-qdrant",
        action="store_true",
        help="whether to skip qdrant indexing",
    )

    # all-pep
    sub_all_pep.add_argument(
        "--pep_config",
        dest="pep_config",
        required=True,
        help="Path to the pep configuration file [Required]\n "
        "Required fields in PEP are: "
        "sample_name, input_file, input_type,outfolder, genome, bedbase_config.\n "
        "Optional fields in PEP are: "
        "rfg_config, narrowpeak, check_qc, standard_chrom, chrom_sizes, "
        "open_signal_matrix, ensdb, sample_yaml, no_db_commit, just_db_commit, "
        "no_db_commit, force_overwrite, skip_qdrant",
        type=str,
    )

    # bed_qc
    sub_qc.add_argument(
        "--bedfile",
        help="a full path to bed file to process [Required]",
        required=True,
    )
    sub_qc.add_argument(
        "--outfolder",
        help="a full path to output log folder. [Required]",
        required=True,
    )

    # bed_maker

    sub_make.add_argument(
        "-f",
        "--input-file",
        required=True,
        help="path to the input file [Required]",
        type=str,
    )
    sub_make.add_argument(
        "--outfolder",
        required=True,
        help="Pipeline output folder [Required]",
        type=str,
    )
    sub_make.add_argument(
        "-n",
        "--narrowpeak",
        help="whether it's a narrowpeak file",
        action="store_true",
    )
    sub_make.add_argument(
        "-t",
        "--input-type",
        required=True,
        help="input file format (supported formats: bedGraph, bigBed, bigWig, wig) [Required]",
        type=str,
    )
    sub_make.add_argument(
        "-g",
        "--genome",
        required=True,
        help="reference genome [Required]",
        type=str,
    )
    sub_make.add_argument(
        "-r",
        "--rfg-config",
        required=False,
        default=None,
        help="file path to the genome config file",
        type=str,
    )
    sub_make.add_argument(
        "-o",
        "--output-bed",
        required=True,
        help="path to the output BED files [Required]",
        type=str,
    )
    sub_make.add_argument(
        "--output-bigbed",
        required=True,
        help="path to the folder of output bigBed files [Required]",
        type=str,
    )
    sub_make.add_argument(
        "-s",
        "--sample-name",
        required=True,
        help="name of the sample used to systematically build the output name [Required]",
        type=str,
    )
    sub_make.add_argument(
        "--chrom-sizes",
        help="whether standardize chromosome names. "
        "If ture, bedmaker will remove the regions on ChrUn chromosomes, "
        "such as chrN_random and chrUn_random. [Default: False]",
        default=None,
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
        "--bedfile", help="a full path to bed file to process [Required]", required=True
    )
    sub_stat.add_argument(
        "--outfolder",
        required=True,
        help="Pipeline output folder [Required]",
        type=str,
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
        required=True,
        help="a path to the bedbase configuration file [Required]",
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
        dest="genome",
        type=str,
        required=True,
        help="genome assembly of the sample [Required]",
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

    return logmuse.add_logging_options(parser)
