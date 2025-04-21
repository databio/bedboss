from enum import Enum
from pathlib import Path
from typing import Union

from pydantic import BaseModel, Field, ConfigDict
from gtars.models import RegionSet

from bedboss.models import DATA_FORMAT


class InputTypes(Enum):
    BED_GRAPH = "bedgraph"
    BIG_BED = "bigbed"
    BIG_WIG = "bigwig"
    WIG = "wig"
    BED = "bed"


class BedMakerOutput(BaseModel):
    bed_object: Union[str, RegionSet]
    bed_file: Union[str, Path]
    bigbed_file: Union[str, Path, None] = None
    bed_digest: str = None
    bed_compliance: str = Field(
        default="bed3+0", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    compliant_columns: int
    non_compliant_columns: int
    data_format: DATA_FORMAT

    model_config = ConfigDict(arbitrary_types_allowed=True)
