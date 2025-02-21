from typing import Union

from pydantic import BaseModel, ConfigDict


class ChromNameStats(BaseModel):
    xs: float = 0.0
    q_and_m: float = 0.0
    q_and_not_m: float = 0.0
    not_q_and_m: float = 0.0
    jaccard_index: float = 0.0
    jaccard_index_binary: float = 0.0
    passed_chrom_names: bool = False


class ChromLengthStats(BaseModel):
    oobr: Union[float, None] = None
    beyond_range: bool = False
    num_of_chrom_beyond: int = 0
    percentage_bed_chrom_beyond: float = 0.0
    percentage_genome_chrom_beyond: float = 0.0


class SequenceFitStats(BaseModel):
    sequence_fit: Union[float, None] = None


class RatingModel(BaseModel):
    assigned_points: int
    tier_ranking: int
    # model_config = ConfigDict(extra="forbid")


class CompatibilityStats(BaseModel):
    chrom_name_stats: ChromNameStats
    chrom_length_stats: ChromLengthStats
    chrom_sequence_fit_stats: SequenceFitStats
    igd_stats: Union[dict, None] = None
    compatibility: Union[RatingModel, None] = None

    model_config = ConfigDict(extra="forbid")


class CompatibilityConcise(BaseModel):
    xs: float = 0.0
    oobr: Union[float, None] = None
    sequence_fit: Union[float, None] = None
    assigned_points: int
    tier_ranking: int
