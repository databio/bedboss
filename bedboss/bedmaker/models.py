from enum import Enum
from pathlib import Path
from typing import Union

from pydantic import BaseModel, Field


class InputTypes(Enum):
    BED_GRAPH = "bedgraph"
    BIG_BED = "bigbed"
    BIG_WIG = "bigwig"
    WIG = "wig"
    BED = "bed"


class BedType(str, Enum):
    BED = "bed"
    NARROWPEAK = "narrowpeak"
    BROADPEAK = "broadpeak"


class BedMakerOutput(BaseModel):
    bed_file: Union[str, Path]
    bigbed_file: Union[str, Path, None] = None
    bed_digest: str = None
    bed_type: str = Field(
        default="bed3", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    bed_format: BedType = BedType.BED
