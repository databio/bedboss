#!/usr/bin/env python3

from argparse import ArgumentParser
import pypiper
import os

# import re
import sys
import tempfile
import pandas as pd
import gzip
import shutil
from refgenconf import (
    RefGenConf as RGC,
    select_genome_config,
    RefgenconfError,
    CFG_ENV_VARS,
    CFG_FOLDER_KEY,
)
import logmuse
from yacman.exceptions import UndefinedAliasError
from ubiquerg import is_command_callable

from .bedqc import run_bedqc, QualityException
from .const import *


class BedMaker:
    """
    Python Package to convert various genome region files to bed file
    """

    def __init__(
        self,
        input_file: str,
        input_type: str,
        output_bed: str,
        output_bigbed: str,
        sample_name: str,
        genome: str,
        rfg_config: str,
        chrom_sizes: str = None,
        narrowpeak: bool = False,
        standard_chrom: bool = False,
        check_qc: bool = True,
        opts=None,
        **kwargs,
    ):
        """
        A pipeline to convert bigwig, bedGraph, bed, bigBed or wig files into bed and bigBed format
        :param check_qc: run quality control during badmaking
        :param input_file: path to the input file
        :param input_type: a [bigwig|bedgraph|bed|bigbed|wig] file that will be converted into BED format
        :param output_bed: path to the output BED files
        :param output_bigbed: path to the output bigBed files
        :param sample_name: name of the sample used to systematically build the output name
        :param genome: reference genome
        :param rfg_config: file path to the genome config file
        :param chrom_sizes: a full path to the chrom.sizes required for the bedtobigbed conversion
        :param narrowpeak: whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks)
        :param sntandard_chrom: Standardize chromosome names. Default: False
        :return: noReturn
        """

        if opts is not None:
            _LOGGER = logmuse.logger_via_cli(opts)
        else:
            _LOGGER = logmuse.init_logger(name="bedmaker")
        self._LOGGER = _LOGGER

        self.input_file = input_file
        self.input_type = input_type
        self.output_bed = output_bed
        self.output_bigbed = output_bigbed
        self.sample_name = sample_name
        self.genome = genome
        self.rfg_config = rfg_config
        self.chrom_sizes = chrom_sizes
        self.check_qc = check_qc

        self.narrowpeak = narrowpeak
        # Define whether Chip-seq data has broad or narrow peaks
        self.width = "bdgbroadcall" if not self.narrowpeak else "bdgpeakcall"

        self.standard_chrom = standard_chrom

        self.file_name = os.path.basename(input_file)
        self.file_id = os.path.splitext(self.file_name)[0]
        self.input_extension = os.path.splitext(self.file_name)[
            1
        ]  # is it gzipped or not?
        # check if output bed is file or folder:

        self.output_bed_extension = os.path.splitext(self.output_bed)[1]
        if self.output_bed_extension == "":
            self.output_bed = f"{self.output_bed}/{os.path.splitext(os.path.splitext(os.path.split(input_file)[1])[0])[0]}.bed.gz"
            self.output_bed_extension = os.path.splitext(self.output_bed)[1]

        # set output folders:
        if self.input_type != "bed":
            if self.input_extension == ".gz":
                self.output_bed = (
                    os.path.splitext(os.path.splitext(self.output_bed)[0])[0]
                    + ".bed.gz"
                )
            else:
                self.output_bed = os.path.splitext(self.output_bed)[0] + ".bed.gz"
        else:
            if self.input_extension != ".gz" and self.output_bed_extension != ".gz":
                self.output_bed = self.output_bed + ".gz"
            else:
                self.output_bed = self.output_bed

        self.bed_parent = os.path.dirname(self.output_bed)
        if not os.path.exists(self.bed_parent):
            self._LOGGER.info(
                f"Output directory does not exist. Creating: {self.bed_parent}"
            )
            os.makedirs(self.bed_parent)

        if not os.path.exists(self.output_bigbed):
            self._LOGGER.info(
                f"BigBed directory does not exist. Creating: {self.output_bigbed}"
            )
            os.makedirs(self.output_bigbed)

        self.logs_name = "bedmaker_logs"
        self.logs_dir = os.path.join(self.bed_parent, self.logs_name, self.sample_name)
        if not os.path.exists(self.logs_dir):
            self._LOGGER.info("bedmaker logs directory doesn't exist. Creating one...")
            os.makedirs(self.logs_dir)

        self.pm = pypiper.PipelineManager(
            name="bedmaker",
            outfolder=self.logs_dir,
        )

    def make(self):
        """
        :return: True if conversion was successful and bed and bigBed file was created
        """
        # pm = pypiper.PipelineManager(name="bedmaker", outfolder=logs_dir, args=args) # ArgParser and add_pypiper_args

        # Define target folder for converted files and implement the conversions; True=TF_Chipseq False=Histone_Chipseq

        self._LOGGER.info(f"Got input type: {self.input_type}")
        temp_bed_path = os.path.splitext(self.output_bed)[0]

        if not self.input_type == "bed":
            self._LOGGER.info(f"Converting {self.input_file} to BED format")

            # # Call pyBigWig to ensure bigWig and bigBed files have the correct format
            # if args.input_type in ["bigWig", "bigBed"]:
            #     obj = pyBigWig.open(args.input_file)
            #     validation_method = getattr(obj, "isBigBed" if args.input_type == "bigBed" else "isBigWig")
            #     if not validation_method():
            #         raise Exception("{} file did not pass the {} format validation".
            #                         format(args.input_file, args.input_type))

            # Use the gzip and shutil modules to produce temporary unzipped files
            if self.input_extension == ".gz":
                input_file = os.path.join(
                    os.path.dirname(self.output_bed),
                    os.path.splitext(self.file_name)[0],
                )
                with gzip.open(self.input_file, "rb") as f_in:
                    with open(input_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                self.pm.clean_add(input_file)

            if self.input_type == "bedGraph":
                cmd = BEDGRAPH_TEMPLATE.format(
                    input=self.input_file, output=temp_bed_path, width=self.width
                )
                if not is_command_callable("macs2"):
                    raise SystemExit(
                        "To convert bedGraph file You must first install the macs2 tool, "
                        "and add it to your PATH. Instruction: "
                        "https://pypi.org/project/MACS2/"
                    )
            elif self.input_type == "bigWig":
                cmd = BIGWIG_TEMPLATE.format(
                    input=self.input_file, output=temp_bed_path, width=self.width
                )
                if not is_command_callable("bigWigToBedGraph"):
                    raise SystemExit(
                        "To convert bigWig file You must first install the bigWigToBedGraph tool, "
                        "with bigWigToBedGraph in your PATH. Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                    )
            elif self.input_type == "wig":

                chrom_sizes = self._get_chrom_sizes()

                # define a target for temporary bw files
                temp_target = os.path.join(self.bed_parent, self.file_id + ".bw")
                self.pm.clean_add(temp_target)
                cmd1 = WIG_TEMPLATE.format(
                    input=self.input_file,
                    intermediate_bw=temp_target,
                    chrom_sizes=chrom_sizes,
                    width=self.width,
                )
                if not is_command_callable("wigToBigWig"):
                    raise SystemExit(
                        "To convert wig file You must first install the wigToBigWig tool, "
                        "with wigToBigWig in your PATH. Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                    )
                cmd2 = BIGWIG_TEMPLATE.format(
                    input=temp_target, output=temp_bed_path, width=self.width
                )
                cmd = [cmd1, cmd2]
                if not is_command_callable("bigWigToBedGraph"):
                    raise SystemExit(
                        "To convert bigWig file You must first install the bigWigToBedGraph tool, "
                        "with bigWigToBedGraph in your PATH. Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                    )
            elif self.input_type == "bigBed":
                cmd = BIGBED_TEMPLATE.format(
                    input=self.input_file, output=temp_bed_path
                )
                if not is_command_callable("bigBedToBed"):
                    raise SystemExit(
                        "To convert bigBed file You must first install the bigBedToBed tool, "
                        "with bigBedToBed in your PATH. Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigBed.html"
                    )
            else:
                raise NotImplementedError(
                    f"'{self.input_type}' format is not supported"
                )

        else:
            if self.input_extension == ".gz":
                cmd = BED_TEMPLATE.format(input=self.input_file, output=self.output_bed)
            else:
                cmd = [
                    BED_TEMPLATE.format(
                        input=self.input_file,
                        output=os.path.splitext(self.output_bed)[0],
                    ),
                    GZIP_TEMPLATE.format(
                        unzipped_converted_file=os.path.splitext(self.output_bed)[0]
                    ),
                ]

        if self.input_type != "bed" and self.input_extension != ".gz":
            gzip_cmd = GZIP_TEMPLATE.format(unzipped_converted_file=temp_bed_path)
            if not isinstance(cmd, list):
                cmd = [cmd]
            cmd.append(gzip_cmd)
        self.pm.run(cmd, target=self.output_bed)

        if self.check_qc:
            qc = run_bedqc(
                self.output_bed, outfolder=os.path.join(self.bed_parent, "bedqc_logs")
            )
            if len(qc) > 0:
                raise QualityException(str(qc))

        self._LOGGER.info(f"Generating bigBed files for: {self.input_file}")

        bedfile_name = os.path.split(self.output_bed)[1]
        fileid = os.path.splitext(os.path.splitext(bedfile_name)[0])[0]
        # Produce bigBed (big_narrow_peak) file from peak file
        big_narrow_peak = os.path.join(self.output_bigbed, fileid + ".bigBed")

        chrom_sizes = self._get_chrom_sizes()

        temp = os.path.join(self.output_bigbed, next(tempfile._get_candidate_names()))

        if not os.path.exists(big_narrow_peak):
            bedtype = self.get_bed_type(self.output_bed)
            self.pm.clean_add(temp)

            if not is_command_callable("bedToBigBed"):
                # raise SystemExit(
                #     "To convert bed to BigBed file You must first install the bedToBigBed tool, "
                #     "with bigBedToBed in your PATH. Instruction: "
                #     "https://genome.ucsc.edu/goldenpath/help/bigBed.html"
                # )
                self._LOGGER.warning(
                    "No bedToBigBed converter installed! "
                    "You must first install the bedToBigBed tool, "
                    "with bigBedToBed in your PATH. Instruction: "
                    "https://genome.ucsc.edu/goldenpath/help/bigBed.html"
                )
            if bedtype is not None:
                cmd = "zcat " + self.output_bed + "  | sort -k1,1 -k2,2n > " + temp
                self.pm.run(cmd, temp)

                cmd = f"bedToBigBed -type={bedtype} {temp} {chrom_sizes} {big_narrow_peak}"
                try:
                    self.pm.run(cmd, big_narrow_peak, nofail=True)
                except Exception as err:
                    self._LOGGER.info(
                        f"Fail to generating bigBed files for {self.input_file}: "
                        f"unable to validate genome assembly with Refgenie. Error: {err}"
                    )
            else:
                cmd = (
                    "zcat "
                    + self.output_bed
                    + " | awk '{ print $1, $2, $3 }'| sort -k1,1 -k2,2n > "
                    + temp
                )
                self.pm.run(cmd, temp)
                cmd = f"bedToBigBed -type=bed3 {temp} {chrom_sizes} {big_narrow_peak}"
                try:
                    self.pm.run(cmd, big_narrow_peak, nofail=True)
                except Exception as err:
                    self._LOGGER.info(
                        f"Fail to generating bigBed files for {self.input_file}: "
                        f"unable to validate genome assembly with Refgenie. Error: {err}"
                    )

        self.pm.stop_pipeline()

    def _get_chrom_sizes(self):
        """
        Get chrom.sizes file path by input arg, or Refegenie.

        :return str: chrom.sizes file path
        """

        if self.chrom_sizes:
            return self.chrom_sizes

        self._LOGGER.info("Determining path to chrom.sizes asset via Refgenie.")
        # get path to the genome config; from arg or env var if arg not provided
        refgenie_cfg_path = select_genome_config(
            filename=self.rfg_config, check_exist=False
        )
        if not refgenie_cfg_path:
            raise OSError(
                "Could not determine path to a refgenie genome configuration file. "
                "Use --rfg-config argument or set '{}' environment variable to provide it".format(
                    CFG_ENV_VARS
                )
            )
        if isinstance(refgenie_cfg_path, str) and not os.path.exists(refgenie_cfg_path):
            # file path not found, initialize a new config file
            self._LOGGER.info(
                f"File '{refgenie_cfg_path}' does not exist. Initializing refgenie genome configuration file."
            )
            rgc = RGC(entries={CFG_FOLDER_KEY: os.path.dirname(refgenie_cfg_path)})
            rgc.initialize_config_file(filepath=refgenie_cfg_path)
        else:
            self._LOGGER.info(
                f"Reading refgenie genome configuration file from file: {refgenie_cfg_path}"
            )
            rgc = RGC(filepath=refgenie_cfg_path)
        try:
            # get path to the chrom.sizes asset
            chrom_sizes = rgc.seek(
                genome_name=self.genome,
                asset_name="fasta",
                tag_name="default",
                seek_key="chrom_sizes",
            )
            self._LOGGER.info(chrom_sizes)
        except (UndefinedAliasError, RefgenconfError):
            # if chrom.sizes not found, pull it first
            self._LOGGER.info("Could not determine path to chrom.sizes asset, pulling")
            rgc.pull(genome=self.genome, asset="fasta", tag="default")
            chrom_sizes = rgc.seek(
                genome_name=self.genome,
                asset_name="fasta",
                tag_name="default",
                seek_key="chrom_sizes",
            )

        self._LOGGER.info(
            "Determined path to chrom.sizes asset: {}".format(chrom_sizes)
        )

        return chrom_sizes

    def get_bed_type(self, bed: str):
        """
        get bed type + standardize chromosomes if necessary
        :param bed: path to the bed file
        :return bed type
        """
        #    column format for bed12
        #    string chrom;       "Reference sequence chromosome or scaffold"
        #    uint   chromStart;  "Start position in chromosome"
        #    uint   chromEnd;    "End position in chromosome"
        #    string name;        "Name of item."
        #    uint score;          "Score (0-1000)"
        #    char[1] strand;     "+ or - for strand"
        #    uint thickStart;   "Start of where display should be thick (start codon)"
        #    uint thickEnd;     "End of where display should be thick (stop codon)"
        #    uint reserved;     "Used as itemRgb as of 2004-11-22"
        #    int blockCount;    "Number of blocks"
        #    int[blockCount] blockSizes; "Comma separated list of block sizes"
        #    int[blockCount] chromStarts; "Start positions relative to chromStart"
        df = pd.read_csv(bed, sep="\t", header=None)
        df = df.dropna(axis=1)

        # standardizing chromosome names
        if self.standard_chrom:
            self._LOGGER.info("Standardizing chromosomes...")
            df = df[df.loc[:, 0].isin(STANDARD_CHROM_LIST)]
            df.to_csv(bed, compression="gzip", sep="\t", header=False, index=False)

        num_cols = len(df.columns)
        bedtype = 0
        for col in df:
            if col <= 2:
                if col == 0:
                    if df[col].dtype == "O":
                        bedtype += 1
                    else:
                        return None
                else:
                    if df[col].dtype == "int" and (df[col] >= 0).all():
                        bedtype += 1
                    else:
                        return None
            else:
                if col == 3:
                    if df[col].dtype == "O":
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}"
                elif col == 4:
                    if df[col].dtype == "int" and df[col].between(0, 1000).all():
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}"
                elif col == 5:
                    if df[col].isin(["+", "-", "."]).all():
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}"
                elif 6 <= col <= 8:
                    if df[col].dtype == "int" and (df[col] >= 0).all():
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}"
                elif col == 9:
                    if df[col].dtype == "int":
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}"
                elif col == 10 or col == 11:
                    if df[col].str.match(r"^(\d+(,\d+)*)?$").all():
                        bedtype += 1
                    else:
                        n = num_cols - bedtype
                        return f"bed{bedtype}+{n}"
                else:
                    n = num_cols - bedtype
                    return f"bed{bedtype}+{n}"


class ParseOpt(object):
    def __init__(self):
        parser = ArgumentParser(
            description="A pipeline to convert bigwig or bedgraph files into bed format",
            usage="""bedmaker <command> [<args>]
        
The commands used in bedmaker are:
    make        Making bed and bigBed file from other formats
    qc          Run quality control on bed file
""",
        )
        parser.add_argument("command", help="Subcommand to run")
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command, running bedmaker")
            self.make()
        getattr(self, args.command)()

    @staticmethod
    def make():
        parser = ArgumentParser(
            description="A pipeline to convert bigwig or bedgraph files into bed format"
        )

        parser.add_argument(
            "-f", "--input-file", help="path to the input file", type=str
        )
        parser.add_argument(
            "-n",
            "--narrowpeak",
            help="whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks)",
            type=bool,
        )
        parser.add_argument(
            "-t",
            "--input-type",
            help="a bigwig or a bedgraph file that will be converted into BED format",
            type=str,
        )
        parser.add_argument("-g", "--genome", help="reference genome", type=str)
        parser.add_argument(
            "-r", "--rfg-config", help="file path to the genome config file", type=str
        )
        parser.add_argument(
            "-o", "--output-bed", help="path to the output BED files", type=str
        )
        parser.add_argument(
            "--output-bigbed", help="path to the output bigBed files", type=str
        )
        parser.add_argument(
            "-s",
            "--sample-name",
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
            help="Standardize chromosome names. Default: False",
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
        """
        parser for bedqc
        :return: NoReturn
        """
        parser = ArgumentParser(description="A pipeline for bed file QC.")

        parser.add_argument(
            "--bedfile", help="a full path to bed file to process", required=True
        )
        parser.add_argument(
            "--outfolder", help="a full path to output folder", required=True
        )

        parser = pypiper.add_pypiper_args(
            parser, groups=["pypiper", "common", "looper", "ngs"]
        )
        args = parser.parse_args(sys.argv[2:])
        bedfile = args.bedfile
        outfolder = args.outfolder
        run_bedqc(bedfile, outfolder)


def main():
    # args = _parse_cmdl(sys.argv[1:])
    # args_dict = vars(args)
    # args_dict["args"] = args
    # BedMaker(**args_dict).make()
    ParseOpt()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
