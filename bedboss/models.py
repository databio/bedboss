from pydantic import BaseModel, ConfigDict, Field

from enum import Enum
import pypiper
import pathlib
from typing import Union

from bedboss.const import MAX_FILE_SIZE, MAX_REGION_NUMBER, MIN_REGION_WIDTH


class FILE_TYPE(str, Enum):
    BED = "bed"
    NARROWPEAK = "narrowpeak"
    BROADPEAK = "broadpeak"


class BedMetadata(BaseModel):
    sample_name: str
    genome: str
    organism: str = ""
    species_id: str = ""
    cell_type: str = ""
    cell_line: str = ""
    exp_protocol: str = Field("", description="Experimental protocol (e.g. ChIP-seq)")
    library_source: str = Field(
        "", description="Library source (e.g. genomic, transcriptomic)"
    )
    genotype: str = Field("", description="Genotype of the sample")
    target: str = Field("", description="Target of the assay (e.g. H3K4me3)")
    antibody: str = Field("", description="Antibody used in the assay")
    treatment: str = Field(
        "", description="Treatment of the sample (e.g. drug treatment)"
    )
    tissue: str = Field("", description="Tissue type")
    global_sample_id: str = Field("", description="Global sample identifier")
    global_experiment_id: str = Field("", description="Global experiment identifier")
    description: str = Field("", description="Description of the sample")

    # THIS IS NOW PART OF THE BedBase model in bbconf
    # bed_format: FILE_TYPE = FILE_TYPE.BED
    # bed_type: str = Field(
    #     default="bed3", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    # )

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )


class BedStatCLIModel(BaseModel):
    """
    CLI model for bedstat
    """

    bedfile: Union[str, pathlib.Path]
    genome: str
    outfolder: Union[str, pathlib.Path]
    bed_digest: str = None
    bigbed: Union[str, pathlib.Path] = None
    ensdb: str = None
    open_signal_matrix: str = None
    just_db_commit: bool = False
    pm: pypiper.PipelineManager = None

    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


class BedQCCLIModel(BaseModel):
    """
    CLI model for bedqc
    """

    bedfile: Union[str, pathlib.Path]
    outfolder: Union[str, pathlib.Path]
    max_file_size: int = MAX_FILE_SIZE
    max_region_number: int = MAX_REGION_NUMBER
    min_region_width: int = MIN_REGION_WIDTH
    pm: pypiper.PipelineManager = None

    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


class BedMakerCLIModel(BaseModel):
    """
    CLI model for bedmaker
    """

    input_file: Union[str, pathlib.Path]
    input_type: str
    output_bed: Union[str, pathlib.Path]
    output_bigbed: Union[str, pathlib.Path]
    sample_name: str
    genome: str
    rfg_config: Union[str, pathlib.Path] = None
    chrom_sizes: str = None
    narrowpeak: bool = False
    standardize: bool = False
    check_qc: bool = True
    pm: pypiper.PipelineManager = None

    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)
