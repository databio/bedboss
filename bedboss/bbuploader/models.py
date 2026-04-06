
from pydantic import BaseModel, ConfigDict, Field, field_validator

from bedboss.bbuploader.metadata_extractor import (
    standardize_assay,
    standardize_cell_line,
)


class BedBossMetadata(BaseModel):
    genome: str = Field(None, alias="ref_genome")
    species_name: str | None = Field("", alias="sample_organism_ch1")
    species_id: str | None = Field("", alias="sample_taxid_ch1")
    cell_type: str | None = ""
    cell_line: str | None = Field("", alias="line")
    genotype: str | None = ""
    assay: str | None = Field("", alias="sample_library_strategy")
    library_source: str | None = Field("", alias="sample_library_source")
    target: str | None = Field("")
    antibody: str | None = Field("", alias="chip_antibody")
    treatment: str | None = Field("", alias="sample_treatment_protocol_ch1")
    tissue: str | None = ""

    global_sample_id: str | None = Field("", alias="sample_geo_accession")
    global_experiment_id: str | None = Field("", alias="gse")
    description: str | None = Field("", alias="sample_description")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    @field_validator("global_sample_id", "global_experiment_id")
    def value_must_not_be_empty(cls, v):
        value = v.lower()
        if value.startswith("gsm") or value.startswith("gse"):
            return f"geo:{value}"
        return value

    @field_validator("cell_type", mode="before")
    @classmethod
    def standardize_cell_type(cls, v):
        if v:
            return standardize_cell_line(v)
        return v

    @field_validator("assay", mode="before")
    @classmethod
    def standardize_assay_value(cls, v):
        if v:
            return standardize_assay(v)
        return v


class BedBossMetadataSeries(BedBossMetadata):
    # TODO: check if all this values are correct:
    description: str | None = Field("", alias="series_title")
    genome: str = Field(None, alias="ref_genome")
    species_name: str | None = Field("", alias="series_sample_organism")
    species_id: str | None = Field("", alias="series_sample_taxid")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )


class BedBossRequired(BaseModel):
    sample_name: str
    file_path: str
    ref_genome: str
    type: str | None = "bed"
    narrowpeak: bool | None = False
    description: str | None = ""
    organism: str | None = None
    pep: BedBossMetadata | BedBossMetadataSeries | None = None
    title: str | None = None


class ProjectProcessingStatus(BaseModel):
    number_of_samples: int = 0
    number_of_processed: int = 0
    number_of_skipped: int = 0
    number_of_failed: int = 0
