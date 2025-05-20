import gzip
import logging
import os
import shutil
from pathlib import Path
from typing import Union, Tuple

import pypiper
from geniml.bbclient import BBClient
from gtars.models import RegionSet
from refgenconf.exceptions import MissingGenomeError
from ubiquerg import is_command_callable

from bedboss.bedclassifier.bedclassifier import get_bed_classification
from bedboss.bedmaker.const import (
    BEDGRAPH_TEMPLATE,
    BIGBED_FOLDER_NAME,
    BIGBED_TEMPLATE,
    BIGBED_TO_BED_PROGRAM,
    BIGWIG_TEMPLATE,
    WIG_TEMPLATE,
)
from bedboss.const import MAX_FILE_SIZE, MAX_REGION_NUMBER, MIN_REGION_WIDTH
from bedboss.bedmaker.models import BedMakerOutput, InputTypes
from bedboss.bedmaker.utils import get_chrom_sizes
from bedboss.exceptions import BedBossException, RequirementsException, QualityException

_LOGGER = logging.getLogger("bedboss")


def make_bigbed(
    bed: Union[str, RegionSet],
    output_path: str,
    genome: str,
    rfg_config: Union[str, Path] = None,
) -> None:
    """
    Generate bigBed file for the BED file.

    :param bed: path to the BED file, or RegionSet object from Rust
    :param genome: reference genome (e.g. hg38, mm10, etc.)
    :param output_path: full path to the output bigBed file

    :return: None
    """
    try:
        chrom_sizes = get_chrom_sizes(genome=genome, rfg_config=rfg_config)
    except MissingGenomeError:
        raise BedBossException("Could not find Genome in refgenie. Skipping...")

    if isinstance(bed, str):
        bed = RegionSet(bed)
    elif not isinstance(bed, RegionSet):
        raise BedBossException("Invalid bed object. Must be a path or RegionSet.")

    try:
        if not chrom_sizes:
            raise BedBossException("Chrom sizes not found. Skipping...")
        bed.to_bigbed(output_path, chrom_sizes)
    except BaseException as err:
        raise BedBossException(
            f"Failed to generate bigBed file for {output_path}: Error: {err}"
        )

    _LOGGER.info(f"BigBed file generated: {output_path}")


def make_bed(
    input_file: str,
    input_type: str,
    output_path: str,
    genome: str,
    narrowpeak: bool = False,
    rfg_config: str = None,
    chrom_sizes: str = None,
    pm: pypiper.PipelineManager = None,
) -> Tuple[str, str, RegionSet]:
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
                        wig files conversion
    :param pm: pypiper object

    :return: path to the BED file
    """
    _LOGGER.info(f"Processing {input_file} file in bedmaker...")

    bbclient = BBClient()

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
            bed_obj = bbclient.add_bed_to_cache(input_file)
            bed_id = bed_obj.identifier
            output_path = bbclient.seek(bed_id)
        except BaseException as e:
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

        bed_obj = bbclient.add_bed_to_cache(input_file)
        bed_id = bed_obj.identifier
        output_path = bbclient.seek(bed_id)

    pm._cleanup()
    if pm_clean:
        pm.stop_pipeline()

    _LOGGER.info(
        f"Bed output file: {output_path}. BEDmaker: File processed successfully."
    )

    return output_path, bed_id, bed_obj


def make_all(
    input_file: str,
    input_type: str,
    output_path: str,
    genome: str,
    rfg_config: str = None,
    chrom_sizes: str = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    lite: bool = False,
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
    :param lite: run the pipeline in lite mode (without producing bigBed files)
    :param pm: pypiper object

    :return: dict with generated bed metadata - BedMakerOutput object:
        {
            "bed_compliance": bed_compliance. e.g. bed3+0
            "data_format": data_format. e.g. narrowpeak, broadpeak
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
    output_bed, bed_id, bed_obj = make_bed(
        input_file=input_file,
        input_type=input_type,
        output_path=output_path,
        genome=genome,
        narrowpeak=narrowpeak,
        rfg_config=rfg_config,
        chrom_sizes=chrom_sizes,
        pm=pm,
    )
    bed_classification = get_bed_classification(output_bed)
    if check_qc:
        try:
            file_size = os.path.getsize(output_bed)
            if file_size >= MAX_FILE_SIZE:
                raise QualityException(
                    f"File size is larger than {MAX_FILE_SIZE} bytes. File size= {file_size} bytes."
                )

            mean_region_width = bed_obj.mean_region_width()
            if mean_region_width < MIN_REGION_WIDTH:
                raise QualityException(
                    f"Mean region width is less than {MIN_REGION_WIDTH} bp. File mean region width= {mean_region_width} bp."
                )
            number_of_regions = len(bed_obj)
            if number_of_regions > MAX_REGION_NUMBER:
                raise QualityException(
                    f"Number of regions is greater than {MAX_REGION_NUMBER}. File number of regions= {number_of_regions}."
                )
        except QualityException as e:
            raise QualityException(
                f"Quality control failed for {output_path}. Error: {e}"
            )

        _LOGGER.info(f"File ({output_bed}) has passed Quality Control!")

    if lite:
        _LOGGER.info("Skipping bigBed generation due to lite mode.")
        output_bigbed = None
    else:
        try:
            bigbed_folder_path = os.path.join(
                output_path, BIGBED_FOLDER_NAME, bed_id[0], bed_id[1]
            )
            os.makedirs(bigbed_folder_path, exist_ok=True)
            output_bigbed = os.path.join(bigbed_folder_path, f"{bed_id}.bigBed")

            make_bigbed(
                bed=bed_obj,
                output_path=output_bigbed,
                genome=genome,
                rfg_config=rfg_config,
            )
        except BedBossException:
            output_bigbed = None
    if pm_clean:
        pm.stop_pipeline()

    _LOGGER.info(f"Bed output file: {output_bed}")
    _LOGGER.info(f"BigBed output file: {output_bigbed}")

    return BedMakerOutput(
        bed_object=bed_obj,
        bed_file=output_bed,
        bigbed_file=os.path.abspath(output_bigbed) if output_bigbed else None,
        bed_digest=bed_id,
        bed_compliance=bed_classification.bed_compliance,
        compliant_columns=bed_classification.compliant_columns,
        non_compliant_columns=bed_classification.non_compliant_columns,
        data_format=bed_classification.data_format,
    )
