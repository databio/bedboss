import gzip
import logging
import os
import shutil

import pypiper
from geniml.bbclient import BBClient
from gtars.models import RegionSet
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
from bedboss.bedmaker.models import BedMakerOutput, InputTypes
from bedboss.bedmaker.utils import write_chrom_sizes_file
from bedboss.const import MAX_FILE_SIZE, MAX_REGION_NUMBER, MIN_REGION_WIDTH
from bedboss.exceptions import BedBossException, QualityException, RequirementsException
from bedboss.refgenome_validator.genome_model import GenomeModel
from bedboss.refgenome_validator.refgenie_chrom_sizes import chrom_sizes_for_digest

_LOGGER = logging.getLogger("bedboss")


def make_bigbed(
    bed: str | RegionSet,
    output_path: str,
    digest: str,
    genome_models: list[GenomeModel] | None = None,
) -> None:
    """
    Generate bigBed file for the BED file.

    Chrom sizes come from the same seqcol data the reference-genome validator
    loads, looked up by the reference genome's ``digest`` directly, and passed to
    ``RegionSet.to_bigbed`` as a dict — no temporary chrom.sizes file. The digest
    is typically the one predicted by the reference-genome validator (a member of
    ``genome_models``), so the bigBed and the stored classification agree.

    Args:
        bed: Path to the BED file, or RegionSet object from Rust.
        output_path: Full path to the output bigBed file.
        digest: Seqcol digest of the reference genome.
        genome_models: Optional list of already-loaded GenomeModels (e.g. a live
            ReferenceValidator's) to reuse for the chrom-sizes lookup.
    """

    chrom_sizes = chrom_sizes_for_digest(digest, genome_models=genome_models)
    if not chrom_sizes:
        raise BedBossException(
            f"Could not find chrom sizes for genome digest '{digest}'. Skipping..."
        )

    if isinstance(bed, str):
        bed = RegionSet(bed)
    elif not isinstance(bed, RegionSet):
        raise BedBossException("Invalid bed object. Must be a path or RegionSet.")

    try:
        bed.to_bigbed(output_path, chrom_sizes)
    except BaseException as err:
        raise BedBossException(
            f"Failed to generate bigBed file for {output_path}: Error: {err}"
        )

    _LOGGER.info(f"BigBed file generated: {output_path}")


def make_bigbed_for_bed(
    bed: str | RegionSet,
    bed_id: str,
    output_path: str,
    digest: str,
    genome_models: list[GenomeModel] | None = None,
) -> str | None:
    """
    Build the bigBed at the canonical output location for a bed id.

    Writes to ``<output_path>/<BIGBED_FOLDER_NAME>/<bed_id[0]>/<bed_id[1]>/<bed_id>.bigBed``.
    Shared by ``make_all`` (BED inputs) and the pipeline's deferred build for
    non-BED inputs so the path logic lives in one place.

    Args:
        bed: Path to the BED file, or RegionSet object.
        bed_id: Bed digest/identifier (used for the nested folder layout).
        output_path: Pipeline output folder.
        digest: Seqcol digest of the reference genome.
        genome_models: Optional list of already-loaded GenomeModels to reuse.

    Returns:
        Absolute path to the bigBed file, or ``None`` if it could not be built
        (the failure is logged).
    """
    try:
        bigbed_folder_path = os.path.join(
            output_path, BIGBED_FOLDER_NAME, bed_id[0], bed_id[1]
        )
        os.makedirs(bigbed_folder_path, exist_ok=True)
        output_bigbed = os.path.join(bigbed_folder_path, f"{bed_id}.bigBed")

        make_bigbed(
            bed=bed,
            output_path=output_bigbed,
            digest=digest,
            genome_models=genome_models,
        )
        return os.path.abspath(output_bigbed)
    except BedBossException as err:
        _LOGGER.warning(f"Could not build bigBed for {bed_id}: {err}")
        return None


def make_bed(
    input_file: str,
    input_type: str,
    output_path: str,
    genome: str,
    narrowpeak: bool = False,
    chrom_sizes: str | None = None,
    genome_models: list[GenomeModel] | None = None,
    pm: pypiper.PipelineManager | None = None,
) -> tuple[str, str, RegionSet]:
    """
    Convert the input file to BED format by constructing the command based on input file type and executing it.

    Args:
        input_file: Path to the input file.
        input_type: A [bigwig|bedgraph|bed|bigbed|wig] file that will be converted into BED format.
        output_path: Path to the output folder, logs will be saved (BED files will be cached).
        genome: Reference genome.
        narrowpeak: Whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks).
        chrom_sizes: A full path to the chrom.sizes required for the wig files conversion.
        genome_models: Optional list of already-loaded GenomeModels to reuse for
            the chrom-sizes lookup (WIG conversion only).
        pm: Pypiper object.

    Returns:
        Tuple of (bed_file_path, bed_id, bed_object).
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
                chrom_sizes = write_chrom_sizes_file(
                    genome_alias=genome,
                    out_dir=os.path.dirname(output_path),
                    genome_models=genome_models,
                )

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
    chrom_sizes: str | None = None,
    narrowpeak: bool = False,
    check_qc: bool = True,
    genome_models: list[GenomeModel] | None = None,
    pm: pypiper.PipelineManager = None,
) -> BedMakerOutput:
    """
    Maker of BED files.

    Pipeline to convert supported file formats (bedGraph, bigBed, bigWig, wig, bed)
    into BED format. The bigBed is **not** built here: the reference genome is
    predicted only after conversion (prediction needs the converted BED regions),
    so the pipeline builds the bigBed once, after validation, in ``run_all``
    (``bedboss.py``). ``bigbed_file`` in the returned object is therefore always
    ``None``.

    Args:
        input_file: Path to the input file.
        input_type: A [bigwig|bedgraph|bed|bigbed|wig] file that will be converted into BED format.
        output_path: Path to the output folder, where logs will be saved.
        genome: Reference genome.
        chrom_sizes: A full path to the chrom.sizes required for the wig conversion.
        narrowpeak: Whether the regions are narrow (transcription factor implies narrow, histone mark implies broad peaks).
        check_qc: Run quality control during bedmaking.
        genome_models: Optional list of already-loaded GenomeModels (e.g. a live
            ReferenceValidator's) reused for the WIG chrom-sizes lookup.
        pm: Pypiper object.

    Returns:
        BedMakerOutput object with bed_compliance, data_format, bed_file, bed_digest
        (and bigbed_file=None; the bigBed is built by the pipeline).
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
        chrom_sizes=chrom_sizes,
        genome_models=genome_models,
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

    if pm_clean:
        pm.stop_pipeline()

    _LOGGER.info(f"Bed output file: {output_bed}")

    return BedMakerOutput(
        bed_object=bed_obj,
        bed_file=output_bed,
        bigbed_file=None,
        bed_digest=bed_id,
        bed_compliance=bed_classification.bed_compliance,
        compliant_columns=bed_classification.compliant_columns,
        non_compliant_columns=bed_classification.non_compliant_columns,
        data_format=bed_classification.data_format,
    )
