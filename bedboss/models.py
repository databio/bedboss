from pydantic import BaseModel, ConfigDict, Field

from enum import Enum


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
