#!/usr/bin/env python3

from typing import Union
import pypiper
import os
from pathlib import Path

import logging
import tempfile
import pandas as pd
import gzip
import shutil

from refgenconf.exceptions import MissingGenomeError

from ubiquerg import is_command_callable
from geniml.io import RegionSet

from geniml.bbclient import BBClient

from bedboss.bedclassifier.bedclassifier import get_bed_type
from bedboss.bedqc.bedqc import bedqc
from bedboss.exceptions import RequirementsException, BedBossException

from bedboss.bedmaker.const import (
    BEDGRAPH_TEMPLATE,
    BIGWIG_TEMPLATE,
    BIGBED_TEMPLATE,
    WIG_TEMPLATE,
    GZIP_TEMPLATE,
    STANDARD_CHROM_LIST,
    BED_TO_BIGBED_PROGRAM,
    BIGBED_TO_BED_PROGRAM,
    QC_FOLDER_NAME,
)
from bedboss.bedmaker.utils import get_chrom_sizes

_LOGGER = logging.getLogger("bedboss")


def make_bigbed(
    bed_path: Union[str, Path],
    output_path: Union[str, Path],
    genome: str,
    bed_type: str = None,
    rfg_config: Union[str, Path] = None,
    chrom_sizes: Union[str, Path] = None,
    pm: pypiper.PipelineManager = None,
) -> str:
    """
    Generate bigBed file for the BED file.

    :param bed_path: path to the BED file
    :param output_path: folder to save the bigBed file.
    :param genome: reference genome (e.g. hg38, mm10, etc.)
    :param chrom_sizes: a full path to the chrom.sizes required for the
                        bedtobigbed conversion
    :param rfg_config: file path to the genome config file. [Default: None]
    :param bed_type: bed type to be used for bigBed file generation "bed{bedtype}+{n}" [Default: None] (e.g bed3+1)

    :return: path to the bigBed file
    """
    if not pm:
        pm = pypiper.PipelineManager(
            name="bedmaker",
            outfolder=os.path.join(output_path, "bedmaker_logs"),
            recover=True,
            multi=True,
        )
        pm_clean = True
    else:
        pm_clean = False
    _LOGGER.info(f"Generating bigBed files for: {bed_path}")

    bedfile_name = os.path.split(bed_path)[1]
    fileid = os.path.splitext(os.path.splitext(bedfile_name)[0])[0]
    # Produce bigBed (big_narrow_peak) file from peak file
    big_bed_path = os.path.join(output_path, fileid + ".bigBed")
    if not chrom_sizes:
        try:
            chrom_sizes = get_chrom_sizes(genome=genome, rfg_config=rfg_config)
        except MissingGenomeError:
            _LOGGER.error(f"Could not find Genome in refgenie. Skipping...")
            chrom_sizes = ""

    temp = os.path.join(output_path, next(tempfile._get_candidate_names()))

    if not os.path.exists(big_bed_path):
        pm.clean_add(temp)

        if not is_command_callable(f"{BED_TO_BIGBED_PROGRAM}"):
            raise RequirementsException(
                "To convert bed to BigBed file You must first install "
                "bedToBigBed add in your PATH. "
                "Instruction: "
                "https://genome.ucsc.edu/goldenpath/help/bigBed.html"
            )
        if bed_type is not None:
            cmd = f"zcat {bed_path} | sort -k1,1 -k2,2n > {temp}"
            pm.run(cmd, temp)

            cmd = f"{BED_TO_BIGBED_PROGRAM} -type={bed_type} {temp} {chrom_sizes} {big_bed_path}"
            try:
                _LOGGER.info(f"Running: {cmd}")
                pm.run(cmd, big_bed_path, nofail=False)
            except Exception as err:
                _LOGGER.error(
                    f"Fail to generating bigBed files for {bed_path}: "
                    f"unable to validate genome assembly with Refgenie. "
                    f"Error: {err}"
                )
        else:
            cmd = (
                "zcat "
                + bed_path
                + " | awk '{ print $1, $2, $3 }'| sort -k1,1 -k2,2n > "
                + temp
            )
            pm.run(cmd, temp)
            cmd = f"{BED_TO_BIGBED_PROGRAM} -type=bed3 {temp} {chrom_sizes} {big_bed_path}"

            try:
                pm.run(cmd, big_bed_path, nofail=True)
            except Exception as err:
                _LOGGER.info(
                    f"Fail to generating bigBed files for {bed_path}: "
                    f"unable to validate genome assembly with Refgenie. "
                    f"Error: {err}"
                )
        pm._cleanup()
    if pm_clean:
        pm.stop_pipeline()
    return big_bed_path


def make_bed(
    input_file: str,
    input_type: str,
    output_bed: str,
    genome: str,
    narrowpeak: bool = False,
):
    """
    Convert the input file to BED format by construct the command based
    on input file type and execute the command.
    """
    pass


class BedMaker:
    """
    Python Package to convert various genomic region files to bed file
    and generate bigbed file for visulization.
    """

    def __init__(
        self,
        input_file: str,
        input_type: str,
        output_bed: str,
        output_bigbed: str,
        sample_name: str,
        genome: str,
        rfg_config: str = None,
        chrom_sizes: str = None,
        narrowpeak: bool = False,
        standardize: bool = False,
        check_qc: bool = True,
        pm: pypiper.PipelineManager = None,
    ):
        """
        Pypiper pipeline to convert supported file formats into
        BED format and bigBed format. Currently supported formats*:
            - bedGraph
            - bigBed
            - bigWig
            - wig
        :param input_file: path to the input file
        :param input_type: a [bigwig|bedgraph|bed|bigbed|wig] file that will be
                           converted into BED format
        :param output_bed: path to the output BED files
        :param output_bigbed: path to the output bigBed files
        :param sample_name: name of the sample used to systematically build the
                            output name
        :param genome: reference genome
        :param rfg_config: file path to the genome config file
        :param chrom_sizes: a full path to the chrom.sizes required for the
                            bedtobigbed conversion
        :param narrowpeak: whether the regions are narrow (transcription factor
                           implies narrow, histone mark implies broad peaks)
        :param standardize: whether standardize bed file. (includes standardizing chromosome names and
            sanitize file first rows if they exist) Default: False
            Additionally, standardize chromosome names.
            If true, filter the input file to contain only
            the standard chromosomes, remove regions on
            ChrUn chromosomes
        :param check_qc: run quality control during bedmaking
        :param pm: pypiper object
        :return: noReturn
        """

        # Define file paths
        self.input_file = input_file
        self.input_type = input_type.lower()
        self.output_bed = output_bed
        self.output_bigbed = output_bigbed
        self.file_name = os.path.basename(input_file)
        self.file_id = os.path.splitext(self.file_name)[0]
        self.input_extension = os.path.splitext(self.file_name)[1]

        self.sample_name = sample_name
        self.genome = genome
        self.chrom_sizes = chrom_sizes
        self.check_qc = check_qc
        self.rfg_config = rfg_config
        self.standardize = standardize

        # Define whether input file data is broad or narrow peaks
        self.narrowpeak = narrowpeak
        self.width = "bdgbroadcall" if not self.narrowpeak else "bdgpeakcall"

        # check if output bed is file or folder:
        self.output_bed_extension = os.path.splitext(self.output_bed)[1]
        if self.output_bed_extension == "":
            self.output_bed = os.path.join(
                self.output_bed,
                f"{os.path.splitext(os.path.splitext(os.path.split(input_file)[1])[0])[0]}"
                ".bed.gz",
            )
            self.output_bed_extension = os.path.splitext(self.output_bed)[1]

        # Set output folders:
        # create one if doesn't exist
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
            _LOGGER.info(
                f"Output directory does not exist. Creating: {self.bed_parent}"
            )
            os.makedirs(self.bed_parent)

        if not os.path.exists(self.output_bigbed):
            _LOGGER.info(
                f"BigBed directory does not exist. Creating: {self.output_bigbed}"
            )
            os.makedirs(self.output_bigbed)

        if not pm:
            self.logs_name = "bedmaker_logs"
            self.logs_dir = os.path.join(
                self.bed_parent, self.logs_name, self.sample_name
            )
            if not os.path.exists(self.logs_dir):
                _LOGGER.info("bedmaker logs directory doesn't exist. Creating one...")
                os.makedirs(self.logs_dir)
            self.pm = pypiper.PipelineManager(
                name="bedmaker",
                outfolder=self.logs_dir,
                recover=True,
                multi=True,
            )
        else:
            self.pm = pm

    def make(self) -> dict:
        """
        Create bed and BigBed files.
        This is main function that executes every step of the bedmaker pipeline.
        """
        _LOGGER.info(f"Got input type: {self.input_type}")
        # converting to bed.gz if needed
        self.make_bed()
        try:
            bed_type, bed_format = get_bed_type(self.input_file)
        except Exception:
            # we need this exception to catch the case when the input file is not a bed file
            bed_type, bed_format = get_bed_type(self.output_bed)
        if self.check_qc:
            try:
                bedqc(
                    self.output_bed,
                    outfolder=os.path.join(self.bed_parent, QC_FOLDER_NAME),
                    pm=self.pm,
                )
            except Exception as e:
                raise BedBossException(
                    f"Quality control failed for {self.output_bed}. Error: {e}"
                )
        return_value = make_bigbed(
            bed_type=bed_type,
            bed_path=self.output_bed,
            genome=self.genome,
            output_path=self.output_bigbed,
            rfg_config=self.rfg_config,
            chrom_sizes=self.chrom_sizes,
            pm=self.pm,
        )

        return {
            "bed_type": bed_type,
            "bed_format": bed_format,
            "bed_path": self.output_bed,
            "genome": self.genome,
            "digest": RegionSet(self.output_bed).identifier,
        }

    def make_bed(self) -> None:
        """
        Convert the input file to BED format by construct the command based
        on input file type and execute the command.
        """

        _LOGGER.info(f"Converting {os.path.abspath(self.input_file)} to BED format.")
        temp_bed_path = os.path.splitext(self.output_bed)[0]

        # creat cmd to run that convert non bed file to bed file
        if not self.input_type == "bed":
            _LOGGER.info(f"Converting {self.input_file} to BED format")

            # Use the gzip and shutil modules to produce temporary unzipped files
            if self.input_extension == ".gz":
                temp_input_file = os.path.join(
                    os.path.dirname(self.output_bed),
                    os.path.splitext(self.file_name)[0],
                )
                with gzip.open(self.input_file, "rb") as f_in:
                    with open(temp_input_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                self.pm.clean_add(temp_input_file)

            # creating cmd for bedGraph files
            if self.input_type == "bedGraph":
                if not is_command_callable("macs2"):
                    raise RequirementsException(
                        "To convert bedGraph file You must first install "
                        "macs2 and add it to your PATH. "
                        "Instruction: "
                        "https://pypi.org/project/MACS2/"
                    )
                else:
                    cmd = BEDGRAPH_TEMPLATE.format(
                        input=self.input_file,
                        output=temp_bed_path,
                        width=self.width,
                    )
            # creating cmd for bigWig files
            elif self.input_type == "bigWig":
                if not is_command_callable("bigWigToBedGraph"):
                    raise RequirementsException(
                        "To convert bigWig file You must first install "
                        "bigWigToBedGraph and add it to your PATH. "
                        "Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                    )
                else:
                    cmd = BIGWIG_TEMPLATE.format(
                        input=self.input_file,
                        output=temp_bed_path,
                        width=self.width,
                    )
            # creating cmd for wig files
            elif self.input_type == "wig":
                if not self.chrom_sizes:
                    self.chrom_sizes = get_chrom_sizes(
                        genome=self.genome, rfg_config=self.rfg_config
                    )

                # define a target for temporary bw files
                temp_target = os.path.join(self.bed_parent, self.file_id + ".bw")
                if not is_command_callable("wigToBigWig"):
                    raise RequirementsException(
                        "To convert wig file You must first install "
                        "wigToBigWig and add it in your PATH. "
                        "Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                    )
                else:
                    self.pm.clean_add(temp_target)
                    cmd1 = WIG_TEMPLATE.format(
                        input=self.input_file,
                        intermediate_bw=temp_target,
                        chrom_sizes=self.chrom_sizes,
                        width=self.width,
                    )

                if not is_command_callable("bigWigToBedGraph"):
                    raise RequirementsException(
                        "To convert bigWig file You must first install "
                        "bigWigToBedGraph and add it in your PATH. "
                        "Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                    )
                else:
                    cmd2 = BIGWIG_TEMPLATE.format(
                        input=temp_target,
                        output=temp_bed_path,
                        width=self.width,
                    )
                    cmd = [cmd1, cmd2]
            # creating cmd for bigBed files
            elif self.input_type == "bigBed":
                if not is_command_callable(BIGBED_TO_BED_PROGRAM):
                    raise RequirementsException(
                        "To convert bigBed file You must first install "
                        "bigBedToBed and add it in your PATH. "
                        "Instruction: "
                        "https://genome.ucsc.edu/goldenpath/help/bigBed.html"
                    )
                else:
                    cmd = BIGBED_TEMPLATE.format(
                        input=self.input_file, output=temp_bed_path
                    )
            else:
                raise NotImplementedError(
                    f"'{self.input_type}' format is not supported"
                )
            # add cmd to create the gz file
            if self.input_extension != ".gz":
                gzip_cmd = GZIP_TEMPLATE.format(unzipped_converted_file=temp_bed_path)
                if not isinstance(cmd, list):
                    cmd = [cmd]
                cmd.append(gzip_cmd)
        else:
            # If bed file was provided:
            bbclient = BBClient()
            bed_id = bbclient.add_bed_to_cache(self.input_file)

            self.output_bed = bbclient.seek(bed_id)

        self.pm._cleanup()


def make_all(
    input_file: str,
    input_type: str,
    output_bed: str,
    output_bigbed: str,
    sample_name: str,
    genome: str,
    rfg_config: str = None,
    chrom_sizes: str = None,
    narrowpeak: bool = False,
    standardize: bool = False,
    check_qc: bool = True,
    pm: pypiper.PipelineManager = None,
):
    """
    Maker of bed and bigbed files.

    Pipeline to convert supported file formats into
    BED format and bigBed format. Currently supported formats*:
        - bedGraph
        - bigBed
        - bigWig
        - wig
    :param input_file: path to the input file
    :param input_type: a [bigwig|bedgraph|bed|bigbed|wig] file that will be
                       converted into BED format
    :param output_bed: path to the output BED files
    :param output_bigbed: path to the output bigBed files
    :param sample_name: name of the sample used to systematically build the
                        output name
    :param genome: reference genome
    :param rfg_config: file path to the genome config file
    :param chrom_sizes: a full path to the chrom.sizes required for the
                        bedtobigbed conversion
    :param narrowpeak: whether the regions are narrow (transcription factor
                       implies narrow, histone mark implies broad peaks)
    :param standardize: whether standardize bed file. (includes standardizing chromosome names and
        sanitize file first rows if they exist) Default: False
        Additionally, standardize chromosome names.
        If true, filter the input file to contain only
        the standard chromosomes, remove regions on
        ChrUn chromosomes
    :param check_qc: run quality control during bedmaking
    :param pm: pypiper object
    :return: dict with generated bed metadata:
        {
            "bed_type": bed_type. e.g. bed, bigbed
            "bed_format": bed_format. e.g. narrowpeak, broadpeak
            "genome": genome of the sample,
            "digest": bedfile identifier,
        }
    """
    return BedMaker(
        input_file=input_file,
        input_type=input_type,
        output_bed=output_bed,
        output_bigbed=output_bigbed,
        sample_name=sample_name,
        genome=genome,
        rfg_config=rfg_config,
        chrom_sizes=chrom_sizes,
        narrowpeak=narrowpeak,
        standardize=standardize,
        check_qc=check_qc,
        pm=pm,
    ).make()
