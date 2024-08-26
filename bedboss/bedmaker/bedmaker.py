import gzip
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Union

import pypiper
from geniml.bbclient import BBClient
from geniml.io import RegionSet
from refgenconf.exceptions import MissingGenomeError
from ubiquerg import is_command_callable

from bedboss.bedclassifier import get_bed_type
from bedboss.bedmaker.const import (
    BED_TO_BIGBED_PROGRAM,
    BEDGRAPH_TEMPLATE,
    BIGBED_FILE_NAME,
    BIGBED_TEMPLATE,
    BIGBED_TO_BED_PROGRAM,
    BIGWIG_TEMPLATE,
    QC_FOLDER_NAME,
    WIG_TEMPLATE,
)
from bedboss.bedmaker.models import BedMakerOutput, InputTypes
from bedboss.bedmaker.utils import get_chrom_sizes
from bedboss.bedqc.bedqc import bedqc
from bedboss.exceptions import BedBossException, RequirementsException

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
    :param output_path: parent folder to save the bigBed file. (function will create a subfolder for bigBed files)
    :param genome: reference genome (e.g. hg38, mm10, etc.)
    :param chrom_sizes: a full path to the chrom.sizes required for the
                        bedtobigbed conversion
    :param rfg_config: file path to the genome config file. [Default: None]
    :param bed_type: bed type to be used for bigBed file generation "bed{bedtype}+{n}" [Default: None] (e.g bed3+1)
    :param pm: pypiper object

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

    bigbed_output_folder = os.path.join(output_path, BIGBED_FILE_NAME)
    if not os.path.exists(bigbed_output_folder):
        os.makedirs(bigbed_output_folder)

    bedfile_name = os.path.split(bed_path)[1]
    fileid = os.path.splitext(os.path.splitext(bedfile_name)[0])[0]
    # Produce bigBed (big_narrow_peak) file from peak file
    big_bed_path = os.path.join(bigbed_output_folder, fileid + ".bigBed")
    if not chrom_sizes:
        try:
            chrom_sizes = get_chrom_sizes(genome=genome, rfg_config=rfg_config)
        except MissingGenomeError:
            _LOGGER.error("Could not find Genome in refgenie. Skipping...")
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
            pm.run(cmd, temp, nofail=False)

            cmd = f"{BED_TO_BIGBED_PROGRAM} -type={bed_type} {temp} {chrom_sizes} {big_bed_path}"
            try:
                _LOGGER.info(f"Running: {cmd}")
                pm.run(cmd, big_bed_path, nofail=False)
            except Exception as err:
                raise BedBossException(
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
    output_path: str,
    genome: str,
    narrowpeak: bool = False,
    rfg_config: str = None,
    chrom_sizes: str = None,
    pm: pypiper.PipelineManager = None,
) -> str:
    """
    Convert the input file to BED format by construct the command based
    on input file type and execute the command.

    :param input_file: path to the input file
    :param input_type: a [bigwig|bedgraph|bed|bigbed|wig] file that will be
                        converted into BED format
    :param output_path: path to the output folder, logs will be saved (BED files will be cached)
    :param genome: reference genome
    :param narrowpeak: whether the regions are narrow (transcription factor
                        implies narrow, histone mark implies broad peaks)
    :param rfg_config: file path to the genome config file
    :param chrom_sizes: a full path to the chrom.sizes required for the
                        bedtobigbed conversion
    :param pm: pypiper object

    :return: path to the BED file
    """
    _LOGGER.info(f"Converting {os.path.abspath(input_file)} to BED format.")

    input_type = input_type.lower()
    if input_type not in [member.value for member in InputTypes]:
        raise BedBossException(
            f"Invalid input type: {input_type}. "
            f"Supported types: {', '.join([k.value for k in InputTypes])}"
        )

    if not pm:
        pm = pypiper.PipelineManager(
            name="bedmaker",
            outfolder=os.path.join(os.path.dirname(output_path), "bedmaker_logs"),
            recover=True,
            multi=True,
        )
        pm_clean = True
    else:
        pm_clean = False

    temp_bed_path = os.path.splitext(output_path)[0]

    file_base_name = os.path.basename(input_file)
    input_extension = os.path.splitext(file_base_name)[1]

    width = "bdgbroadcall" if not narrowpeak else "bdgpeakcall"

    # creat cmd to run that convert non bed file to bed file
    if input_type == InputTypes.BED.value:
        try:
            # If bed file was provided:
            bbclient = BBClient()
            bed_id = bbclient.add_bed_to_cache(input_file)
            output_path = bbclient.seek(bed_id)
        except FileNotFoundError as e:
            raise BedBossException(f"File not found: {input_file} Error: {e}")

    else:
        _LOGGER.info(f"Converting {input_file} to BED format")

        # Use the gzip and shutil modules to produce temporary unzipped files
        if input_extension == ".gz":
            temp_input_file = os.path.join(
                os.path.dirname(output_path),
                os.path.splitext(file_base_name)[0],
            )
            with gzip.open(input_file, "rb") as f_in:
                with open(temp_input_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            pm.clean_add(temp_input_file)

        # creating cmd for bedGraph files
        if input_type == InputTypes.BED_GRAPH.value:
            if not is_command_callable("macs2"):
                raise RequirementsException(
                    "To convert bedGraph file You must first install "
                    "macs2 and add it to your PATH. "
                    "Instruction: "
                    "https://pypi.org/project/MACS2/"
                )
            else:
                cmd = BEDGRAPH_TEMPLATE.format(
                    input=input_file,
                    output=temp_bed_path,
                    width=width,
                )

        # creating cmd for bigWig files
        elif input_type == InputTypes.BIG_WIG.value:
            if not is_command_callable("bigWigToBedGraph"):
                raise RequirementsException(
                    "To convert bigWig file You must first install "
                    "bigWigToBedGraph and add it to your PATH. "
                    "Instruction: "
                    "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                )
            else:
                cmd = BIGWIG_TEMPLATE.format(
                    input=input_file,
                    output=temp_bed_path,
                    width=width,
                )
        # creating cmd for wig files
        elif input_type == InputTypes.WIG.value:
            if not chrom_sizes:
                chrom_sizes = get_chrom_sizes(genome=genome, rfg_config=rfg_config)

            # define a target for temporary bw files
            temp_target = os.path.join(output_path, file_base_name + ".bw")
            if not is_command_callable("wigToBigWig"):
                raise RequirementsException(
                    "To convert wig file You must first install "
                    "wigToBigWig and add it in your PATH. "
                    "Instruction: "
                    "https://genome.ucsc.edu/goldenpath/help/bigWig.html"
                )
            else:
                pm.clean_add(temp_target)
                cmd1 = WIG_TEMPLATE.format(
                    input=input_file,
                    intermediate_bw=temp_target,
                    chrom_sizes=chrom_sizes,
                    width=width,
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
                    width=width,
                )
                cmd = [cmd1, cmd2]
        # creating cmd for bigBed files
        elif input_type == InputTypes.BIG_BED.value:
            if not is_command_callable(BIGBED_TO_BED_PROGRAM):
                raise RequirementsException(
                    "To convert bigBed file You must first install "
                    "bigBedToBed and add it in your PATH. "
                    "Instruction: "
                    "https://genome.ucsc.edu/goldenpath/help/bigBed.html"
                )
            else:
                cmd = BIGBED_TEMPLATE.format(input=input_file, output=temp_bed_path)

        else:
            raise NotImplementedError(f"'{input_type}' format is not supported")

        pm.run(cmd, temp_bed_path, nofail=False)

        bbclient = BBClient()
        bed_id = bbclient.add_bed_to_cache(input_file)
        output_path = bbclient.seek(bed_id)

    pm._cleanup()
    if pm_clean:
        pm.stop_pipeline()

    return output_path


def make_all(
    input_file: str,
    input_type: str,
    output_path: str,
    genome: str,
    rfg_config: str = None,
    chrom_sizes: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    pm: pypiper.PipelineManager = None,
) -> BedMakerOutput:
    """
    Maker of bed and bigbed files.

    Pipeline to convert supported file formats into
    BED format and bigBed format. Currently supported formats*:
        - bedGraph
        - bigBed
        - bigWig
        - wig
        - bed
    :param input_file: path to the input file
    :param input_type: a [bigwig|bedgraph|bed|bigbed|wig] file that will be
                       converted into BED format
    :param output_path: path to the output folder, where bigbed and logs will be saved
    :param genome: reference genome
    :param rfg_config: file path to the genome config file
    :param chrom_sizes: a full path to the chrom.sizes required for the
                        bedtobigbed conversion
    :param narrowpeak: whether the regions are narrow (transcription factor
                       implies narrow, histone mark implies broad peaks)
    :param check_qc: run quality control during bedmaking
    :param pm: pypiper object

    :return: dict with generated bed metadata - BedMakerOutput object:
        {
            "bed_type": bed_type. e.g. bed, bigbed
            "bed_format": bed_format. e.g. narrowpeak, broadpeak
            "bed_file": path to the bed file
            "bigbed_file": path to the bigbed file
            "bed_digest": bed_digest
        }
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
    output_bed = make_bed(
        input_file=input_file,
        input_type=input_type,
        output_path=output_path,
        genome=genome,
        narrowpeak=narrowpeak,
        rfg_config=rfg_config,
        chrom_sizes=chrom_sizes,
        pm=None,
    )
    bed_type, bed_format = get_bed_type(output_bed)
    if check_qc:
        try:
            bedqc(
                output_bed,
                outfolder=os.path.join(output_path, QC_FOLDER_NAME),
                pm=pm,
            )
        except Exception as e:
            raise BedBossException(
                f"Quality control failed for {output_path}. Error: {e}"
            )
    try:
        output_bigbed = make_bigbed(
            bed_path=output_bed,
            output_path=output_path,
            genome=genome,
            bed_type=bed_type,
            rfg_config=rfg_config,
            chrom_sizes=chrom_sizes,
            pm=pm,
        )
    except BedBossException:
        output_bigbed = None
    if pm_clean:
        pm.stop_pipeline()

    _LOGGER.info(f"Bed output file: {output_bed}")
    _LOGGER.info(f"BigBed output file: {output_bigbed}")

    return BedMakerOutput(
        bed_file=output_bed,
        bigbed_file=os.path.abspath(output_bigbed) if output_bigbed else None,
        bed_digest=RegionSet(output_bed).identifier,
        bed_type=bed_type,
        bed_format=bed_format,
    )
