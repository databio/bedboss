"""gtars Python-bindings-direct statistics backend.

Parallel implementation of GtarsStatBackend that skips the gtars CLI
subprocess and calls gtars Python bindings directly. Caches reference
data (FASTA, GeneModel, PartitionList, SignalMatrix, chrom_sizes) per
backend instance so batch processing amortizes load cost across files.

Produces the same output dict shape as GtarsStatBackend for downstream
compatibility (compress_distributions + DB insertion unchanged).

This coexists with GtarsStatBackend during performance evaluation.
After benchmarking, only one backend will remain.
"""

import logging
import os
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import pypiper
from gtars.models import (
    GeneModel,
    GenomeAssembly,
    GenomicDistAnnotation,
    PartitionList,
    RegionSet,
    SignalMatrix,
    TssIndex,
)
from gtars.genomic_distributions import (
    calc_expected_partitions,
    calc_gc_content,
    calc_partitions,
    calc_summary_signal,
    median_abs_distance,
)

from bedboss.bedstat.backends.base import StatBackend
from bedboss.bedstat.compress_distributions import (
    compress_distributions,
    compress_to_kde,
)
from bedboss.bedstat.gc_content import get_genome_assembly_obj
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


def _normalize_partitions(raw: Optional[dict]) -> Optional[dict]:
    """Convert Python binding's partitions dict to CLI JSON schema.

    Python binding: ``{"partition": [names], "count": [counts], "total": int}``
    CLI schema:     ``{"counts": [[name, count], ...], "total": int}``
    """
    if not raw:
        return None
    names = raw.get("partition") or []
    counts = raw.get("count") or []
    return {
        "counts": list(zip(names, counts)),
        "total": raw.get("total", 0),
    }


def _normalize_expected_partitions(raw: Optional[dict]) -> Optional[dict]:
    """Pass-through for now — bedboss doesn't consume expected_partitions
    scalars beyond storing the blob, so we leave the Python binding's
    native shape intact.
    """
    return raw or None


def _parse_chrom_sizes(path: str) -> dict:
    """Parse a chrom.sizes TSV file into a dict {chrom: length}."""
    sizes = {}
    with open(path, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                try:
                    sizes[parts[0]] = int(parts[1])
                except ValueError:
                    continue
    return sizes


@dataclass
class GenomeRefs:
    """Cached reference data for a genome.

    Fields are independently optional — any failure to load a given ref
    is logged and the field stays None, matching the per-file graceful
    degradation of the CLI-based backend.
    """

    assembly: Optional[GenomeAssembly] = None
    chrom_sizes: Optional[dict] = None
    gene_model: Optional[GeneModel] = None
    partition_list: Optional[PartitionList] = None
    tss_index: Optional[TssIndex] = None
    signal_matrix: Optional[SignalMatrix] = None

    @classmethod
    def load(
        cls,
        genome: str,
        rfg_config: Optional[str],
        ensdb: Optional[str],
        signal_matrix_path: Optional[str],
        promoter_upstream: int,
        promoter_downstream: int,
    ) -> "GenomeRefs":
        refs = cls()

        # chrom_sizes (lightweight, try first)
        try:
            cs_path = get_chrom_sizes_path(genome, rfg_config=rfg_config)
            if cs_path:
                refs.chrom_sizes = _parse_chrom_sizes(cs_path)
        except Exception as e:
            _LOGGER.warning(f"chrom_sizes unavailable for {genome}: {e}")

        # GenomeAssembly (FASTA-backed, for GC content) — leverages module-level
        # cache in bedboss.bedstat.gc_content
        try:
            refs.assembly = get_genome_assembly_obj(genome, rfg_config=rfg_config)
        except Exception as e:
            _LOGGER.warning(f"GenomeAssembly unavailable for {genome}: {e}")

        # Gene model (GTF or GDA .bin) + derived PartitionList + TssIndex
        if not ensdb:
            try:
                ensdb = get_gda_path(genome, rfg_config=rfg_config)
            except Exception:
                pass

        if ensdb:
            try:
                if str(ensdb).endswith(".bin"):
                    # GDA binary: has gene_model + derived tss_index + helper
                    # partition_list() method
                    gda = GenomicDistAnnotation.load_bin(str(ensdb))
                    refs.gene_model = gda.gene_model()
                    refs.tss_index = gda.tss_index()
                    refs.partition_list = gda.partition_list(
                        promoter_upstream,
                        promoter_downstream,
                        refs.chrom_sizes,
                    )
                else:
                    # Raw GTF: load GeneModel, derive PartitionList manually,
                    # TssIndex deferred (would need genes+strands — not in
                    # Python binding's GeneModel API yet, skip for now)
                    refs.gene_model = GeneModel.from_gtf(str(ensdb), True, True)
                    try:
                        refs.partition_list = PartitionList.from_gene_model(
                            refs.gene_model,
                            promoter_upstream,
                            promoter_downstream,
                            refs.chrom_sizes,
                        )
                    except Exception as e:
                        _LOGGER.warning(f"PartitionList from GTF failed: {e}")
            except Exception as e:
                _LOGGER.warning(f"Gene model load failed: {e}")

        # Signal matrix (optional)
        if signal_matrix_path and os.path.exists(signal_matrix_path):
            try:
                resolved = get_osm_path_with_precompile(signal_matrix_path)
                if resolved.endswith(".bin"):
                    refs.signal_matrix = SignalMatrix.load_bin(resolved)
                else:
                    refs.signal_matrix = SignalMatrix.from_tsv(resolved)
            except Exception as e:
                _LOGGER.warning(f"SignalMatrix load failed: {e}")

        return refs


class GtarsPyStatBackend(StatBackend):
    """gtars Python-bindings-direct statistics backend (no CLI subprocess)."""

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
        # Instance-level cache keyed by (genome, ensdb, signal_matrix_path)
        self._ref_cache: dict = {}

    def _get_refs(
        self,
        genome: str,
        rfg_config: Optional[str],
        ensdb: Optional[str],
        signal_matrix_path: Optional[str],
    ) -> GenomeRefs:
        cache_key = (genome, ensdb, signal_matrix_path)
        if cache_key not in self._ref_cache:
            self._ref_cache[cache_key] = GenomeRefs.load(
                genome=genome,
                rfg_config=rfg_config,
                ensdb=ensdb,
                signal_matrix_path=signal_matrix_path,
                promoter_upstream=self._promoter_upstream,
                promoter_downstream=self._promoter_downstream,
            )
        return self._ref_cache[cache_key]

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
        refs = self._get_refs(genome, rfg_config, ensdb, open_signal_matrix)

        outfolder_stats = os.path.join(outfolder, OUTPUT_FOLDER_NAME, BEDSTAT_OUTPUT)
        os.makedirs(outfolder_stats, exist_ok=True)

        stop_pipeline = not pm

        bed_object = RegionSet(bedfile)
        if not bed_digest:
            bed_digest = bed_object.identifier

        outfolder_stats_results = os.path.abspath(
            os.path.join(outfolder_stats, bed_digest)
        )
        os.makedirs(outfolder_stats_results, exist_ok=True)

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

        if not just_db_commit:
            try:
                gtars_output = self._compute_all(bed_object, refs, pm)
            except Exception as e:
                _LOGGER.error(f"gtars-py compute failed: {e}")
                raise BedBossException(f"gtars-py compute failed: {e}")
        else:
            gtars_output = {}

        # Extract scalars to flat dict keys
        data = {}
        scalars = gtars_output.get("scalars", {})
        data["number_of_regions"] = scalars.get("number_of_regions")
        data["mean_region_width"] = scalars.get("mean_region_width")
        data["median_tss_dist"] = scalars.get("median_tss_dist")

        # Derive median_neighbor_distance from the raw neighbor_distances list
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

        # GC content: computed inside _compute_all, add mean as a scalar here
        gc_contents = gtars_output.pop("_gc_contents", None)
        if gc_contents:
            gc_mean = round(statistics.mean(gc_contents), 4)
            data["gc_content"] = gc_mean
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

        # Store entire augmented gtars output as distributions blob
        data["distributions"] = gtars_output

        if self._precision is not None:
            data = round_floats(data, self._precision)

        if stop_pipeline and pm:
            pm.stop_pipeline()

        return data

    def _compute_all(
        self,
        rs: RegionSet,
        refs: GenomeRefs,
        pm: pypiper.PipelineManager,
    ) -> dict:
        """Run all gtars statistics via Python bindings, return raw output dict
        matching the CLI JSON schema (pre-compression)."""
        pm.timestamp("### Computing core statistics")
        widths = rs.widths()
        neighbor_distances = rs.neighbor_distances()
        nearest_neighbors = rs.nearest_neighbors()
        chrom_stats_obj = rs.chromosome_statistics()

        # Serialize ChromosomeStatistics objects to dicts
        chromosome_stats = {}
        for chrom, stats_obj in chrom_stats_obj.items():
            chromosome_stats[chrom] = {
                "chromosome": stats_obj.chromosome,
                "number_of_regions": stats_obj.number_of_regions,
                "start_nucleotide_position": stats_obj.start_nucleotide_position,
                "end_nucleotide_position": stats_obj.end_nucleotide_position,
                "minimum_region_length": stats_obj.minimum_region_length,
                "maximum_region_length": stats_obj.maximum_region_length,
                "mean_region_length": stats_obj.mean_region_length,
                "median_region_length": stats_obj.median_region_length,
            }

        pm.timestamp("### Computing region distribution")
        region_distribution = rs.distribution(
            n_bins=self._region_dist_bins,
            chrom_sizes=refs.chrom_sizes,
        )

        number_of_regions = len(widths)
        mean_region_width = (
            sum(widths) / number_of_regions if number_of_regions > 0 else 0.0
        )

        # TSS distances via cached TssIndex
        tss_distances = None
        median_tss_dist = None
        if refs.tss_index is not None:
            pm.timestamp("### Computing TSS distances")
            try:
                tss_distances = refs.tss_index.feature_distances(rs)
                median_tss_dist = median_abs_distance(
                    [float(d) for d in tss_distances if d is not None]
                )
            except Exception as e:
                _LOGGER.warning(f"TSS distance computation failed: {e}")

        # Partitions + expected partitions.
        # The Python binding returns {partition: [names], count: [counts], total: n}
        # but the CLI JSON schema (that downstream code expects) has
        # {counts: [[name, count], ...], total: n}. Normalize here.
        partitions = None
        expected_partitions = None
        if refs.partition_list is not None:
            pm.timestamp("### Computing partitions")
            try:
                raw = calc_partitions(rs, refs.partition_list, False)
                partitions = _normalize_partitions(raw)
            except Exception as e:
                _LOGGER.warning(f"Partition classification failed: {e}")
        if refs.partition_list is not None and refs.chrom_sizes is not None:
            try:
                raw_ep = calc_expected_partitions(
                    rs, refs.partition_list, refs.chrom_sizes, False
                )
                expected_partitions = _normalize_expected_partitions(raw_ep)
            except Exception as e:
                _LOGGER.warning(f"Expected partitions failed: {e}")

        # Signal matrix overlap
        open_signal = None
        if refs.signal_matrix is not None:
            pm.timestamp("### Computing open chromatin signal")
            try:
                open_signal = calc_summary_signal(rs, refs.signal_matrix)
            except Exception as e:
                _LOGGER.warning(f"Signal summary failed: {e}")

        # GC content (passed through as _gc_contents for outer fn to handle)
        gc_contents = None
        if refs.assembly is not None:
            pm.timestamp("### Computing GC content")
            try:
                gc_contents = calc_gc_content(rs, refs.assembly, ignore_unk_chroms=True)
            except Exception as e:
                _LOGGER.warning(f"GC content failed: {e}")

        # Assemble output matching CLI JSON schema
        return {
            "scalars": {
                "number_of_regions": number_of_regions,
                "mean_region_width": mean_region_width,
                "median_tss_dist": median_tss_dist,
            },
            "partitions": partitions,
            "distributions": {
                "widths": widths,
                "tss_distances": tss_distances,
                "neighbor_distances": neighbor_distances,
                "nearest_neighbors": nearest_neighbors,
                "region_distribution": region_distribution,
                "chromosome_stats": chromosome_stats,
            },
            "expected_partitions": expected_partitions,
            "open_signal": open_signal,
            "_gc_contents": gc_contents,  # consumed by compute()
        }
