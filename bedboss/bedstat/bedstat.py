import json
import logging
import os
import subprocess

from pathlib import Path
from typing import Union

import pypiper
from gtars.models import RegionSet
from refgenconf import RefgenconfError
from yacman.exceptions import UndefinedAliasError

from bedboss.bedstat.compress_distributions import compress_distributions, compress_to_kde
from bedboss.bedstat.gc_content import calculate_gc_content
from bedboss.bedmaker.utils import get_rgc
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
from bbconf.modules.aggregation import round_floats, DEFAULT_PRECISION
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
    osm_bin_path = osm_path + ".bin"

    # Return pre-compiled binary if it already exists
    if os.path.exists(osm_bin_path):
        _LOGGER.info(f"Open Signal Matrix (pre-compiled): {osm_bin_path}")
        return osm_bin_path

    if not os.path.exists(osm_path):
        os.makedirs(osm_folder, exist_ok=True)
        download_file(
            url=f"{OPEN_SIGNAL_URL}{osm_name}",
            path=osm_path,
            no_fail=True,
        )

    # Pre-compile to .bin for faster loading on subsequent runs
    if os.path.exists(osm_path):
        _LOGGER.info(f"Pre-compiling signal matrix: {osm_path}")
        result = subprocess.run(
            ["gtars", "prep", "--signal-matrix", osm_path, "-o", osm_bin_path],
            capture_output=True, text=True,
        )
        if result.returncode == 0 and os.path.exists(osm_bin_path):
            _LOGGER.info(f"Open Signal Matrix (pre-compiled): {osm_bin_path}")
            return osm_bin_path
        else:
            _LOGGER.warning(f"gtars prep failed, using raw file: {result.stderr}")

    _LOGGER.info(f"Open Signal Matrix file path: {osm_path}")
    return osm_path


def _get_chrom_sizes_seqcol(genome: str) -> Union[str, None]:
    """
    Fallback: fetch chrom.sizes via seqcol API when refgenie doesn't have it.

    Resolves genome name to a seqcol digest via the refgenie /v4/genomes
    endpoint, then fetches chromosome names + lengths directly.

    :param genome: genome assembly name
    :return: path to cached chrom.sizes file, or None
    """
    import requests
    from refget.clients import SequenceCollectionClient

    refgenie_api = "https://api.refgenie.org"
    cache_dir = os.path.join(HOME_PATH, "chrom_sizes")

    chrom_sizes_path = os.path.join(cache_dir, f"{genome}.chrom.sizes")
    if os.path.exists(chrom_sizes_path):
        _LOGGER.info(f"Chrom sizes (seqcol cache): {chrom_sizes_path}")
        return chrom_sizes_path

    # Resolve genome name -> seqcol digest
    try:
        resp = requests.get(
            f"{refgenie_api}/v4/genomes", params={"limit": 1000}, timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        _LOGGER.warning(f"seqcol fallback: failed to query genome list: {e}")
        return None

    genome_lower = genome.lower()
    digest = None
    fallback_digest = None
    for entry in resp.json().get("items", []):
        for alias in entry.get("aliases", []):
            alias_lower = alias.lower()
            if alias_lower == f"{genome_lower}-refgenie":
                digest = entry["digest"]
                break
            if not fallback_digest and alias_lower.startswith(genome_lower):
                fallback_digest = entry["digest"]
        if digest:
            break
    digest = digest or fallback_digest

    if not digest:
        _LOGGER.warning(f"seqcol fallback: no digest found for '{genome}'")
        return None

    try:
        client = SequenceCollectionClient(urls=[f"{refgenie_api}/seqcol"])
        os.makedirs(cache_dir, exist_ok=True)
        client.write_chrom_sizes(digest, chrom_sizes_path)
        _LOGGER.info(f"Chrom sizes (seqcol): {chrom_sizes_path}")
        return chrom_sizes_path
    except Exception as e:
        _LOGGER.warning(f"seqcol fallback: failed to fetch chrom.sizes: {e}")
        return None


def get_chrom_sizes_path(genome: str, rfg_config=None) -> Union[str, None]:
    """
    Get a chrom.sizes file for a genome.

    Tries refgenie first (rgc.seek/pull), falls back to the seqcol API.

    :param genome: genome assembly name (e.g. hg38, hg19, mm10)
    :param rfg_config: path to the refgenie config file
    :return: path to chrom.sizes file, or None if unavailable
    """
    rgc = get_rgc(rfg_config=rfg_config)
    try:
        return rgc.seek(
            genome_name=genome, asset_name="fasta",
            tag_name="default", seek_key="chrom_sizes",
        )
    except (UndefinedAliasError, RefgenconfError):
        _LOGGER.info(f"chrom.sizes not local for {genome}, pulling from refgenie")
        try:
            rgc.pull(genome=genome, asset="fasta", tag="default")
            return rgc.seek(
                genome_name=genome, asset_name="fasta",
                tag_name="default", seek_key="chrom_sizes",
            )
        except Exception:
            _LOGGER.info(f"refgenie pull failed for {genome}, trying seqcol API")

    return _get_chrom_sizes_seqcol(genome)


def get_gda_path(genome: str, rfg_config=None) -> Union[str, None]:
    """
    Get a GDA (GenomicDist Annotation) binary for a genome.

    Pulls the Ensembl GTF from refgenie and pre-compiles it to a GDA .bin
    using ``gtars prep``. Falls back to the raw .gtf.gz if compilation fails.

    :param genome: genome assembly name (e.g. hg38, hg19, mm10)
    :param rfg_config: path to the refgenie config file
    :return: path to .gda.bin file (or raw .gtf.gz as fallback), None if unavailable
    """
    _LOGGER.info(f"Getting GDA annotation for genome: {genome}")
    rgc = get_rgc(rfg_config=rfg_config)

    try:
        gtf_path = rgc.seek(
            genome_name=genome, asset_name="ensembl_gtf",
            tag_name="default", seek_key="ensembl_gtf",
        )
    except (UndefinedAliasError, RefgenconfError):
        _LOGGER.info(f"ensembl_gtf not local for {genome}, pulling from refgenie")
        try:
            rgc.pull(genome=genome, asset="ensembl_gtf", tag="default")
            gtf_path = rgc.seek(
                genome_name=genome, asset_name="ensembl_gtf",
                tag_name="default", seek_key="ensembl_gtf",
            )
        except Exception as e:
            _LOGGER.warning(f"Could not fetch GTF for {genome}: {e}")
            return None

    gda_bin_path = gtf_path + ".gda.bin"

    # Return pre-compiled GDA binary if it already exists
    if os.path.exists(gda_bin_path):
        _LOGGER.info(f"GDA annotation (pre-compiled): {gda_bin_path}")
        return gda_bin_path

    # Pre-compile to GDA .bin
    _LOGGER.info(f"Pre-compiling GDA: {gtf_path}")
    result = subprocess.run(
        ["gtars", "prep", "--gtf", gtf_path, "-o", gda_bin_path],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and os.path.exists(gda_bin_path):
        _LOGGER.info(f"GDA annotation (pre-compiled): {gda_bin_path}")
        return gda_bin_path
    else:
        _LOGGER.warning(f"gtars prep (GDA) failed, using raw GTF: {result.stderr}")

    return gtf_path


# Keep old name as alias for backward compatibility
get_gtf_path = get_gda_path


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
    region_dist_bins: int = 250,
    promoter_upstream: int = 200,
    promoter_downstream: int = 2000,
    precision: int = DEFAULT_PRECISION,
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
    # Auto-fetch GDA/GTF annotation via refgenie if not provided
    if not ensdb:
        try:
            ensdb = get_gda_path(genome, rfg_config=rfg_config)
        except Exception:
            _LOGGER.warning(
                f"Could not fetch annotation for {genome}. "
                "Partition and TSS analysis will be skipped."
            )

    # Auto-download open signal matrix if not provided
    if not open_signal_matrix or not os.path.exists(open_signal_matrix):
        try:
            open_signal_matrix = get_osm_path(genome)
        except OpenSignalMatrixException:
            _LOGGER.warning(
                f"Open Signal Matrix was not found for {genome}. Skipping..."
            )

    # Auto-fetch chrom.sizes via refgenie if not provided
    if not chrom_sizes:
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
        cmd_parts.extend([
            "--bins", str(region_dist_bins),
            "--promoter-upstream", str(promoter_upstream),
            "--promoter-downstream", str(promoter_downstream),
            "--compact",
        ])

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

    # GC content: compute via Python bindings (requires refgenie FASTA)
    try:
        gc_contents = calculate_gc_content(
            bedfile=bed_object, genome=genome, rfg_config=rfg_config
        )
    except BaseException as e:
        _LOGGER.warning(
            f"GC content calculation skipped for {genome}: {e}. "
            "Ensure refgenie is configured with a FASTA asset for this genome."
        )
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

    if precision is not None:
        data = round_floats(data, precision)

    if stop_pipeline and pm:
        pm.stop_pipeline()

    return data
