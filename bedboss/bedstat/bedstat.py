import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Union

import pypiper
from gtars.models import RegionSet

from bedboss.bedstat.compress_distributions import compress_distributions, compress_to_kde
from bedboss.bedstat.gc_content import calculate_gc_content
from bedboss.const import (
    BEDSTAT_OUTPUT,
    HOME_PATH,
    OPEN_SIGNAL_FOLDER_NAME,
    OPEN_SIGNAL_URL,
    OS_HG19,
    OS_HG38,
    OS_MM10,
    OUTPUT_FOLDER_NAME,
)
from bedboss.exceptions import BedBossException, OpenSignalMatrixException
from bedboss.utils import download_file

_LOGGER = logging.getLogger("bedboss")

SCHEMA_PATH_BEDSTAT = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "pep_schema.yaml"
)

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


def get_osm_path(genome: str, out_path: str = None) -> Union[str, None]:
    """
    By providing genome name download Open Signal Matrix.

    :param genome: genome assembly
    :param out_path: working directory, where osm should be saved.
        If None, current working directory will be used.
    :return: path to the Open Signal Matrix
    """
    _LOGGER.info("Getting Open Signal Matrix file path...")
    if genome in ("hg19", "GRCh37"):
        osm_name = OS_HG19
    elif genome in ("hg38", "GRCh38"):
        osm_name = OS_HG38
    elif genome in ("mm10", "GRCm38"):
        osm_name = OS_MM10
    else:
        raise OpenSignalMatrixException(
            "For this genome open Signal Matrix was not found."
        )
    if not out_path:
        osm_folder = os.path.join(HOME_PATH, OPEN_SIGNAL_FOLDER_NAME)
    else:
        osm_folder = os.path.join(out_path, OPEN_SIGNAL_FOLDER_NAME)

    osm_path = os.path.join(osm_folder, osm_name)
    if not os.path.exists(osm_path):
        os.makedirs(osm_folder, exist_ok=True)
        download_file(
            url=f"{OPEN_SIGNAL_URL}{osm_name}",
            path=osm_path,
            no_fail=True,
        )
    _LOGGER.info(f"Open Signal Matrix file path: {osm_path}")
    return osm_path


def bedstat(
    bedfile: str,
    genome: str,
    outfolder: str,
    bed_digest: str = None,
    ensdb: str = None,
    chrom_sizes: str = None,
    open_signal_matrix: str = None,
    just_db_commit: bool = False,
    rfg_config: Union[str, Path] = None,
    pm: pypiper.PipelineManager = None,
) -> dict:
    """
    Run bedstat pipeline - compute statistics for a BED file using gtars genomicdist.

    For faster loading when processing many BED files, pre-compile the GTF and
    signal matrix with ``gtars prep`` and pass the .bin paths::

        gtars prep --gtf gencode.v47.annotation.gtf.gz
        gtars prep --signal-matrix openSignalMatrix_hg38.txt.gz

    Then pass the resulting .bin files here (e.g. ``ensdb="gencode.v47.annotation.gtf.bin"``).
    gtars auto-detects .bin vs raw files by extension. Without pre-compilation,
    each BED file re-parses the full GTF/signal matrix from scratch.

    :param str bedfile: the full path to the bed file to process
    :param str genome: genome assembly of the sample
    :param str outfolder: The folder for storing the pipeline results.
    :param str bed_digest: the digest of the bed file. Defaults to None.
    :param str ensdb: path to GTF file (.gtf/.gtf.gz) or pre-compiled .bin for TSS/partition analysis
    :param str chrom_sizes: path to chrom.sizes file for region distribution
    :param str open_signal_matrix: path to signal matrix TSV (.txt/.txt.gz) or pre-compiled .bin
    :param bool just_db_commit: if True, skip running gtars and read existing JSON
    :param str rfg_config: path to the refgenie config file
    :param pm: pypiper object

    :return: dict with statistics and distributions
    """
    # Auto-download open signal matrix if not provided
    if not open_signal_matrix or not os.path.exists(open_signal_matrix):
        try:
            open_signal_matrix = get_osm_path(genome)
        except OpenSignalMatrixException:
            _LOGGER.warning(
                f"Open Signal Matrix was not found for {genome}. Skipping..."
            )

    outfolder_stats = os.path.join(outfolder, OUTPUT_FOLDER_NAME, BEDSTAT_OUTPUT)
    os.makedirs(outfolder_stats, exist_ok=True)

    # Used to stop pipeline if bedstat is used independently
    stop_pipeline = not pm

    bed_object = RegionSet(bedfile)

    if not bed_digest:
        bed_digest = bed_object.identifier

    outfolder_stats_results = os.path.abspath(os.path.join(outfolder_stats, bed_digest))
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
            "gtars", "genomicdist",
            "--bed", bedfile,
            "--output", json_file_path,
        ]
        if ensdb:
            cmd_parts.extend(["--gtf", ensdb])
        if chrom_sizes:
            cmd_parts.extend(["--chrom-sizes", chrom_sizes])
        if open_signal_matrix:
            cmd_parts.extend(["--signal-matrix", open_signal_matrix])
        cmd_parts.extend(["--bins", "250", "--compact"])

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

    # Populate legacy partition flat columns (deprecated — use distributions JSONB)
    partitions = gtars_output.get("partitions")
    if partitions:
        _LOGGER.info("Populating legacy partition columns (deprecated -- use distributions JSONB)")
        total = partitions.get("total", 0)
        for name, count in partitions.get("counts", []):
            db_name = PARTITION_NAME_MAP.get(name)
            if db_name and total > 0:
                data[f"{db_name}_frequency"] = count
                data[f"{db_name}_percentage"] = round(count / total * 100, 4)

    # GC content: compute via Python bindings
    try:
        gc_contents = calculate_gc_content(
            bedfile=bed_object, genome=genome, rfg_config=rfg_config
        )
    except BaseException:
        gc_contents = None

    if gc_contents:
        gc_mean = round(sum(gc_contents) / len(gc_contents), 4)
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

    if stop_pipeline and pm:
        pm.stop_pipeline()

    return data
