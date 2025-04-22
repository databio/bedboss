import pathlib
from enum import Enum
from typing import Union

import pypiper
from bbconf.models.bed_models import (
    BedClassification,
    BedFiles,
    BedPlots,
    BedStatsModel,
)
from pydantic import BaseModel, ConfigDict, Field

from bedboss.const import MAX_FILE_SIZE, MAX_REGION_NUMBER, MIN_REGION_WIDTH


class DATA_FORMAT(str, Enum):
    UNKNOWN = "unknown_data_format"
    UCSC_BED = "ucsc_bed"
    UCSC_BED_RS = "ucsc_bed_rs"
    BED_LIKE = "bed_like"
    BED_LIKE_RS = "bed_like_rs"
    ENCODE_NARROWPEAK = "encode_narrowpeak"
    ENCODE_NARROWPEAK_RS = "encode_narrowpeak_rs"
    ENCODE_BROADPEAK = "encode_broadpeak"
    ENCODE_BROADPEAK_RS = "encode_broadpeak_rs"
    ENCODE_GAPPEDPEAK = "encode_gappedpeak"
    ENCODE_GAPPEDPEAK_RS = "encode_gappedpeak_rs"
    ENCODE_RNA_ELEMENTS = "encode_rna_elements"
    ENCODE_RNA_ELEMENTS_RS = "encode_rna_elements_rs"


class BedMetadata(BaseModel):
    sample_name: str
    genome: str

    species_name: str = Field(
        default="", description="Name of species. e.g. Homo sapiens.", alias="organism"
    )
    species_id: str = ""
    genotype: str = Field("", description="Genotype of the sample")
    phenotype: str = Field("", description="Phenotype of the sample")

    cell_type: str = Field(
        "",
        description="specific kind of cell with distinct characteristics found in an organism. e.g. Neurons, Hepatocytes, Adipocytes",
    )
    cell_line: str = Field(
        "",
        description="population of cells derived from a single cell and cultured in the lab for extended use, e.g. HeLa, HepG2, k562",
    )
    tissue: str = Field("", description="Tissue type")

    library_source: str = Field(
        "", description="Library source (e.g. genomic, transcriptomic)"
    )
    assay: str = Field(
        "", description="Experimental protocol (e.g. ChIP-seq)", alias="exp_protocol"
    )
    antibody: str = Field("", description="Antibody used in the assay")
    target: str = Field("", description="Target of the assay (e.g. H3K4me3)")
    treatment: str = Field(
        "", description="Treatment of the sample (e.g. drug treatment)"
    )

    global_sample_id: str = Field(
        "", description="Global sample identifier. e.g. GSM000"
    )  # excluded in training
    global_experiment_id: str = Field(
        "", description="Global experiment identifier. e.g. GSE000"
    )  # excluded in training
    description: str = Field("", description="Description of the sample")

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
    output_path: Union[str, pathlib.Path]
    # output_bigbed: Union[str, pathlib.Path]
    # sample_name: str
    genome: str
    rfg_config: Union[str, pathlib.Path] = None
    chrom_sizes: str = None
    narrowpeak: bool = False
    # standardize: bool = False
    check_qc: bool = True
    pm: pypiper.PipelineManager = None

    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)


class StatsUpload(BedStatsModel):
    model_config = ConfigDict(extra="ignore")


class PlotsUpload(BedPlots):
    model_config = ConfigDict(extra="ignore")


class FilesUpload(BedFiles):
    model_config = ConfigDict(extra="ignore")


class BedClassificationUpload(BedClassification):
    model_config = ConfigDict(extra="ignore")


class BedSetAnnotations(BaseModel):
    """
    Annotations for a bedset
    """

    author: Union[str, None] = None
    source: Union[str, None] = None

    model_config = ConfigDict(extra="ignore")


class BedClassificationOutput(BaseModel):
    bed_compliance: str
    data_format: DATA_FORMAT
    compliant_columns: int
    non_compliant_columns: int
