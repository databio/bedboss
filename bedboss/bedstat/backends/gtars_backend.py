import json
import logging
import os
import statistics
from pathlib import Path
from typing import Union

import pypiper
from gtars.models import RegionSet

from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.compress_distributions import (
    compress_distributions,
    compress_to_kde,
)
from bedboss.bedstat.gc_content import calculate_gc_content
from bedboss.bedstat.ref_utils import (
    get_chrom_sizes_path,
    get_gda_path,
    get_osm_path_with_precompile,
)
from bedboss.const import BEDSTAT_OUTPUT, DEFAULT_PRECISION, OUTPUT_FOLDER_NAME
from bedboss.exceptions import BedBossException

_LOGGER = logging.getLogger("bedboss")

# Map gtars partition names to legacy DB column name prefixes
PARTITION_NAME_MAP = {
    "promoterCore": "promotercore",
    "promoterProx": "promoterprox",
    "threeUTR": "threeutr",
    "fiveUTR": "fiveutr",
    "exon": "exon",
    "intron": "intron",
    "intergenic": "intergenic",
}


def round_floats(obj, precision: int = 4):
    """Recursively round all floats in a nested dict/list structure."""
    if isinstance(obj, float):
        return round(obj, precision)
    elif isinstance(obj, dict):
        return {k: round_floats(v, precision) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [round_floats(v, precision) for v in obj]
    return obj


class GtarsStatBackend(StatBackend):
    """gtars genomicdist-based statistics backend."""

    def __init__(
        self,
        region_dist_bins: int = 250,
        promoter_upstream: int = 200,
        promoter_downstream: int = 2000,
        precision: int = DEFAULT_PRECISION,
        **kwargs,
    ):
        self._region_dist_bins = region_dist_bins
        self._promoter_upstream = promoter_upstream
        self._promoter_downstream = promoter_downstream
        self._precision = precision

    def compute(
        self,
        bedfile: str,
        genome: str,
        outfolder: str,
        bed_digest: str = None,
        ensdb: str = None,
        open_signal_matrix: str = None,
        just_db_commit: bool = False,
        rfg_config: Union[str, Path] = None,
        pm: pypiper.PipelineManager = None,
    ) -> dict:
        # Auto-fetch GDA/GTF annotation via refgenie if not provided
        if not ensdb:
            try:
                ensdb = get_gda_path(genome, rfg_config=rfg_config)
            except Exception:
                _LOGGER.warning(
                    f"Could not fetch annotation for {genome}. "
                    "Partition and TSS analysis will be skipped."
                )

        # Pre-compile open signal matrix to .bin if available
        if open_signal_matrix and os.path.exists(open_signal_matrix):
            open_signal_matrix = get_osm_path_with_precompile(open_signal_matrix)

        # Auto-fetch chrom.sizes via refgenie if not provided
        chrom_sizes = None
        try:
            chrom_sizes = get_chrom_sizes_path(genome, rfg_config=rfg_config)
        except Exception:
            _LOGGER.warning(
                f"Could not fetch chrom.sizes for {genome}. "
                "Region distribution will not be normalized."
            )

        outfolder_stats = os.path.join(outfolder, OUTPUT_FOLDER_NAME, BEDSTAT_OUTPUT)
        os.makedirs(outfolder_stats, exist_ok=True)

        # Used to stop pipeline if bedstat is used independently
        stop_pipeline = not pm

        bed_object = RegionSet(bedfile)
        if not bed_digest:
            bed_digest = bed_object.identifier

        outfolder_stats_results = os.path.abspath(
            os.path.join(outfolder_stats, bed_digest)
        )
        os.makedirs(outfolder_stats_results, exist_ok=True)

        json_file_path = os.path.abspath(
            os.path.join(outfolder_stats_results, bed_digest + ".json")
        )

        if not just_db_commit:
            if not pm:
                pm_out_path = os.path.abspath(
                    os.path.join(outfolder_stats, "pypiper", bed_digest)
                )
                os.makedirs(pm_out_path, exist_ok=True)
                pm = pypiper.PipelineManager(
                    name="bedstat-pipeline",
                    outfolder=pm_out_path,
                    pipestat_sample_name=bed_digest,
                )

            # Build gtars genomicdist command
            cmd_parts = [
                "gtars",
                "genomicdist",
                "--bed",
                bedfile,
                "--output",
                json_file_path,
            ]
            if ensdb:
                cmd_parts.extend(["--gtf", ensdb])
            if chrom_sizes:
                cmd_parts.extend(["--chrom-sizes", chrom_sizes])
            if open_signal_matrix:
                cmd_parts.extend(["--signal-matrix", open_signal_matrix])
            cmd_parts.extend(
                [
                    "--bins",
                    str(self._region_dist_bins),
                    "--promoter-upstream",
                    str(self._promoter_upstream),
                    "--promoter-downstream",
                    str(self._promoter_downstream),
                    "--compact",
                ]
            )

            command = " ".join(cmd_parts)
            try:
                _LOGGER.info(f"Running gtars genomicdist: {command}")
                pm.run(cmd=command, target=json_file_path)
            except Exception as e:
                _LOGGER.error(f"gtars genomicdist failed: {e}")
                raise BedBossException(f"gtars genomicdist failed: {e}")

        # Read gtars JSON output
        gtars_output = {}
        if os.path.exists(json_file_path):
            with open(json_file_path, "r", encoding="utf-8") as f:
                gtars_output = json.load(f)

        # Extract scalars to flat dict keys
        data = {}
        scalars = gtars_output.get("scalars", {})
        data["number_of_regions"] = scalars.get("number_of_regions")
        data["mean_region_width"] = scalars.get("mean_region_width")
        data["median_tss_dist"] = scalars.get("median_tss_dist")

        # Derive median_neighbor_distance from the raw neighbor_distances list.
        # (The gtars CLI output includes the full list; we reduce it to one
        # scalar here rather than aggregating the full distribution at bedset
        # level — see bbconf aggregation decisions.)
        neighbor_distances = gtars_output.get("distributions", {}).get(
            "neighbor_distances"
        )
        if neighbor_distances:
            abs_vals = [abs(d) for d in neighbor_distances if d is not None]
            if abs_vals:
                data["median_neighbor_distance"] = round(statistics.median(abs_vals), 4)
            else:
                data["median_neighbor_distance"] = None
        else:
            data["median_neighbor_distance"] = None

        # Populate legacy partition flat columns
        partitions = gtars_output.get("partitions")
        if partitions:
            total = partitions.get("total", 0)
            for name, count in partitions.get("counts", []):
                db_name = PARTITION_NAME_MAP.get(name)
                if db_name and total > 0:
                    data[f"{db_name}_frequency"] = count
                    data[f"{db_name}_percentage"] = round(count / total, 4)

        # GC content: compute via Python bindings (requires refgenie FASTA)
        try:
            gc_contents = calculate_gc_content(
                bedfile=bed_object, genome=genome, rfg_config=rfg_config
            )
        except BaseException as e:
            _LOGGER.warning(
                f"GC content calculation skipped for {genome}: {e}. "
                "Ensure refgenie is configured with a FASTA asset."
            )
            gc_contents = None

        if gc_contents:
            gc_mean = round(statistics.mean(gc_contents), 4)
            data["gc_content"] = gc_mean

            # Compress per-region GC values to 512-pt KDE, inject into distributions
            gc_kde = compress_to_kde(gc_contents, n_points=512, log_transform=False)
            if gc_kde:
                gc_kde["mean"] = gc_mean
                if "distributions" not in gtars_output:
                    gtars_output["distributions"] = {}
                gtars_output["distributions"]["gc_content"] = gc_kde
        else:
            data["gc_content"] = None

        # Compress distributions for DB storage
        compress_distributions(gtars_output)

        # Store entire augmented gtars JSON as distributions blob
        data["distributions"] = gtars_output

        if self._precision is not None:
            data = round_floats(data, self._precision)

        if stop_pipeline and pm:
            pm.stop_pipeline()

        return data
