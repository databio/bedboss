import logging
import subprocess
from typing import List, Union, Dict, Any

from bedboss.refgenome_validator.models import CompatibilityConcise
from bedboss.refgenome_validator.const import GENOME_FILES
from gtars.models import RegionSet as GRegionSet

_LOGGER = logging.getLogger("bedboss")


def get_bed_chrom_info(bedfile: Union[str, GRegionSet]) -> Dict[str, int]:
    """
    Determine chrom lengths for bed file

    :param bedfile: RegionSet object or path to bed file
    returns dict: returns dictionary where keys are chrom names and values are the max end position of that chromosome.
    """
    # if isinstance(bedfile, RegionSet):
    #     df = bedfile.to_pandas()
    #     max_end_for_each_chrom = df.groupby(0)[2].max().to_dict()
    # elif isinstance(bedfile, GRegionSet):
    #     max_end_for_each_chrom = {}
    #     for region in bedfile:
    #         if region.chrom not in max_end_for_each_chrom:
    #             max_end_for_each_chrom[region.chrom] = region.end
    #         if region.end > max_end_for_each_chrom[region.chrom]:
    #             max_end_for_each_chrom[region.chrom] = region.end
    # else:
    #     df = RegionSet(bedfile).to_pandas()
    #     max_end_for_each_chrom = df.groupby(0)[2].max().to_dict()
    #
    # return max_end_for_each_chrom

    if isinstance(bedfile, GRegionSet):
        return_dict = bedfile.get_max_end_per_chr()
    else:
        rs = GRegionSet(bedfile)
        return_dict = rs.get_max_end_per_chr()
    return return_dict


def run_igd_command(command):
    """
    Run IGD via a subprocess, this is a temp implementation until Rust IGD python bindings are finished.
    """

    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout
    else:
        _LOGGER.error(f"Error running command: {result.stderr}")
        return None


def parse_IGD_output(output_str) -> Union[None, List[dict]]:
    """
    Parses IGD terminal output into a list of dicts
    Args:
      output_str: The output string from IGD

    Returns:
      A list of dictionaries, where each dictionary represents a record.
    """

    try:
        lines = output_str.splitlines()
        data = []
        for line in lines:
            if line.startswith("index"):
                continue  # Skip the header line
            elif line.startswith("Total"):
                break  # Stop parsing after the "Total" line
            else:
                fields = line.split()
                record = {
                    "index": int(fields[0]),
                    "number_of_regions": int(fields[1]),
                    "number_of_hits": int(fields[2]),
                    "file_name": fields[3],
                }
                data.append(record)
        return data
    except Exception:
        return None


def predict_from_compatibility_resutlts(
    compatibility_stats: dict[str, CompatibilityConcise],
) -> tuple[str | Any, str]:
    tier1_genomes: List[tuple[str, CompatibilityConcise]] = [
        (genome, prediction)
        for genome, prediction in compatibility_stats.items()
        if prediction.tier_ranking == 1
    ]

    if not tier1_genomes:
        # Fall back to tier 2 if there's exactly one tier 2 genome and no tier 1 genomes
        tier2_genomes: List[tuple[str, CompatibilityConcise]] = [
            (genome, prediction)
            for genome, prediction in compatibility_stats.items()
            if prediction.tier_ranking == 2
        ]
        if len(tier2_genomes) == 1:
            _LOGGER.info(
                "No tier 1 genomes found, using single tier 2 genome as fallback"
            )
            best_digest = tier2_genomes[0][0]
            genome_id = GENOME_FILES.get(best_digest)
            if genome_id is None:
                return None, None
            return genome_id, best_digest
        return None, None

    tier1_genomes.sort(
        key=lambda x: (
            x[1].xs,
            x[1].oobr if x[1].oobr is not None else 0.0,
            x[1].sequence_fit if x[1].sequence_fit is not None else 0.0,
        ),
        reverse=True,
    )

    best_digest = tier1_genomes[0][0]
    genome_id = GENOME_FILES.get(best_digest)
    if genome_id is None:
        return None, None
    return genome_id, best_digest
