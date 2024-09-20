from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BedBossMetadata(BaseModel):
    genome: str = Field(None, alias="ref_genome")
    species_name: Optional[str] = Field("", alias="sample_organism_ch1")
    species_id: Optional[str] = Field("", alias="sample_taxid_ch1")
    cell_type: Optional[str] = ""
    cell_line: Optional[str] = ""
    genotype: Optional[str] = ""
    assay: Optional[str] = Field("", alias="sample_library_strategy")
    library_source: Optional[str] = Field("", alias="sample_library_source")
    target: Optional[str] = Field("")
    antibody: Optional[str] = Field("", alias="chip_antibody")
    treatment: Optional[str] = Field("", alias="sample_treatment_protocol_ch1")
    tissue: Optional[str] = ""

    global_sample_id: Optional[str] = Field("", alias="sample_geo_accession")
    global_experiment_id: Optional[str] = Field("", alias="gse")
    description: Optional[str] = Field("", alias="sample_description")

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


class BedBossRequired(BaseModel):
    sample_name: str
    file_path: str
    ref_genome: str
    type: Optional[str] = "bed"
    narrowpeak: Optional[bool] = False
    description: Optional[str] = ""
    organism: Optional[str] = None
    pep: Optional[BedBossMetadata] = None
    title: Optional[str] = None


class ProjectProcessingStatus(BaseModel):
    number_of_samples: int = 0
    number_of_processed: int = 0
    number_of_skipped: int = 0
    number_of_failed: int = 0
