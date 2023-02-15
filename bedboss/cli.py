from argparse import ArgumentParser
import sys
import pypiper

from bedboss.bedboss import run_bedboss
from bedboss.bedstat.bedstat import run_bedstat
from bedboss.bedmaker.bedmaker import BedMaker
from bedboss.bedqc.bedqc import bedqc
from bedboss import __version__


class ParseOpt(object):
    def __init__(self):
        parser = ArgumentParser(
            description="Warehouse of pipelines for BED-like files: "
            "bedmaker, bedstat, and bedqc.",
            usage="""bedboss <command> [<args>]

The commands used in bedmaker are:
    boss        Run all bedboss pipelines and insert data into bedbase
    make        Make bed and bigBed file from other formats (bedmaker)
    qc          Run quality control on bed file (bedqc)
    stat        Run statistic calculation (bedstat)
""",
        )
        parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=f"%(prog)s {__version__}",
        )
        parser.add_argument("command", help="Command to run")
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command, running bedmaker")
            parser.print_help()
            return
        getattr(self, args.command)()

    @staticmethod
    def boss():
        parser = ArgumentParser(
            description="Run bedmaker, bedqc and bedstat in one pipeline, "
            "And upload all data to the bedbase",
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
            "-o",
            "--output-folder",
            required=True,
            help="Output folder",
            type=str,
        )
        parser.add_argument(
            "-g",
            "--genome",
            required=True,
            help="reference genome (assembly)",
            type=str,
        )
        parser.add_argument(
            "-r",
            "--rfg-config",
            required=False,
            help="file path to the genome config file(refgenie)",
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
            help="whether the regions are narrow (transcription factor implies narrow, "
            "histone mark implies broad peaks)",
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
            help="Check quality control before processing data. Default: True",
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
            help="A full path to the ensdb gtf file required for genomes not in GDdata ",
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
            help="skip the JSON commit to the database",
        )
        parser.add_argument(
            "--just-db-commit",
            action="store_true",
            help="just commit the JSON to the database",
        )
        args = parser.parse_args(sys.argv[2:])
        args_dict = vars(args)
        run_bedboss(**args_dict)

    @staticmethod
    def make():
        parser = ArgumentParser(
            description="A pipeline to convert bed, bigbed, bigwig, "
            "or bedgraph files into bed and bigbed formats"
        )

        parser.add_argument(
            "-i",
            "--input-file",
            required=True,
            help="path to the input file",
            type=str,
        )
        parser.add_argument(
            "--narrowpeak",
            action="store_true",
            help="whether it's a narrowpeak file",
        )
        parser.add_argument(
            "-t",
            "--input-type",
            required=True,
            help="input file format (supported formats: bedGraph, bigBed, bigWig, wig)",
            type=str,
        )
        parser.add_argument(
            "-g",
            "--genome",
            required=True,
            help="reference genome",
            type=str,
        )
        parser.add_argument(
            "-r",
            "--rfg-config",
            required=True,
            help="file path to the genome config file",
            type=str,
        )
        parser.add_argument(
            "-o",
            "--output-bed",
            required=True,
            help="path to the output BED files",
            type=str,
        )
        parser.add_argument(
            "--output-bigbed",
            required=True,
            help="path to the folder of output bigBed files",
            type=str,
        )
        parser.add_argument(
            "-s",
            "--sample-name",
            required=True,
            help="name of the sample used to systematically build the output name",
            type=str,
        )
        parser.add_argument(
            "--chrom-sizes",
            help="a full path to the chrom.sizes required for the bedtobigbed conversion",
            type=str,
            required=False,
        )
        parser.add_argument(
            "--standard-chrom",
            help="whether standardize chromosome names. "
            "If ture, bedmaker will remove the regions on ChrUn chromosomes, "
            "such as chrN_random and chrUn_random. Default: False",
            action="store_true",
        )
        # add pypiper args to make pipeline looper compatible
        parser = pypiper.add_pypiper_args(
            parser,
            groups=["pypiper", "looper"],
            required=["--input-file", "--input-type"],
        )

        args = parser.parse_args(sys.argv[2:])
        args_dict = vars(args)
        args_dict["args"] = args
        BedMaker(**args_dict).make()

    @staticmethod
    def qc():
        parser = ArgumentParser(description="A pipeline for bed file QC.")

        parser.add_argument(
            "--bedfile",
            help="a full path to bed file to process",
            required=True,
        )
        parser.add_argument(
            "--outfolder",
            help="a full path to output log folder.",
            required=True,
        )

        args = parser.parse_args(sys.argv[2:])
        bedqc(args.bedfile, args.outfolder)

    @staticmethod
    def stat():
        parser = ArgumentParser(
            description="A pipeline to read a file in BED format and produce metadata "
            "in JSON format."
        )
        parser.add_argument(
            "--bedfile",
            help="a full path to bed file to process",
            required=True,
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
            "--bigbed",
            type=str,
            required=False,
            default=None,
            help="a full path to the bigbed files",
        )
        parser.add_argument(
            "--bedbase-config",
            dest="bedbase_config",
            type=str,
            default=None,
            help="a path to the bedbase configuration file",
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
            "--genome",
            dest="genome_assembly",
            type=str,
            required=True,
            help="genome assembly of the sample",
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
        parser.add_argument(
            "--force-overwrite",
            action="store_true",
            help="whether to overwrite the existing record",
        )
        args = parser.parse_args(sys.argv[2:])
        args_dict = vars(args)
        run_bedstat(**args_dict)


def main():
    ParseOpt()
