from pydantic import BaseModel, ConfigDict, Field

from enum import Enum


class FILE_TYPE(str, Enum):
    BED = "bed"
    NARROWPEAK = "narrowpeak"
    BROADPEAK = "broadpeak"


class BedMetadata(BaseModel):
    sample_name: str
    genome: str
    format_type: FILE_TYPE = FILE_TYPE.BED
    bed_type: str = Field(
        default="bed3", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    description: str = None
    organism: str = None
    cell_type: str = None
    tissue: str = None
    antibody: str = None
    sample_library_strategy: str = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )
