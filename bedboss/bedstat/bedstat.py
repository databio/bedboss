import json
import logging
import os
import subprocess
import urllib.request

from pathlib import Path
from typing import Union

import pypiper
from gtars.models import RegionSet

from bedboss.bedstat.compress_distributions import compress_distributions, compress_to_kde
from bedboss.bedstat.gc_content import calculate_gc_content
from bedboss.const import (
    BEDSTAT_OUTPUT,
    ENSEMBL_GENOMES,
    GTF_FOLDER_NAME,
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


_ENSEMBL_FTP_BASES = {
    "ensembl": "https://ftp.ensembl.org/pub/release-{release}/gtf",
    "grch37": "https://ftp.ensembl.org/pub/grch37/release-{release}/gtf",
    "plants": "https://ftp.ebi.ac.uk/ensemblgenomes/pub/plants/release-{release}/gtf",
    "fungi": "https://ftp.ebi.ac.uk/ensemblgenomes/pub/fungi/release-{release}/gtf",
    "protists": "https://ftp.ebi.ac.uk/ensemblgenomes/pub/protists/release-{release}/gtf",
    "metazoa": "https://ftp.ebi.ac.uk/ensemblgenomes/pub/metazoa/release-{release}/gtf",
}

# Map Ensembl REST API division names to FTP division keys
_DIVISION_MAP = {
    "EnsemblVertebrates": "ensembl",
    "EnsemblPlants": "plants",
    "EnsemblFungi": "fungi",
    "EnsemblProtists": "protists",
    "EnsemblMetazoa": "metazoa",
}


def _ensembl_gtf_url(species: str, assembly: str, release: int, division: str = "ensembl") -> str:
    """Build Ensembl FTP URL for a GTF annotation file."""
    species_cap = species[0].upper() + species[1:]
    filename = f"{species_cap}.{assembly}.{release}.gtf.gz"
    base = _ENSEMBL_FTP_BASES.get(division, _ENSEMBL_FTP_BASES["ensembl"])
    base = base.format(release=release)
    return f"{base}/{species}/{filename}"


def _resolve_ensembl_genome(genome: str) -> Union[tuple, None]:
    """
    Resolve a genome name to (species, assembly, release, division).

    Checks the static ENSEMBL_GENOMES map first (covers UCSC aliases and legacy
    assemblies), then falls back to the Ensembl REST API for ~350 vertebrate
    species and ~2000 plants/fungi/protists/metazoa.

    :param genome: genome name (e.g. hg38, GRCh38, danio_rerio, GRCz11, tair10)
    :return: (species, assembly, release, division) tuple or None
    """
    if genome in ENSEMBL_GENOMES:
        return ENSEMBL_GENOMES[genome]

    genome_lower = genome.lower()

    # Dynamic lookup via main Ensembl REST API (vertebrates + model organisms)
    try:
        req = urllib.request.Request(
            "https://rest.ensembl.org/info/software?content-type=application/json"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            release = json.loads(resp.read().decode())["release"]

        req = urllib.request.Request(
            "https://rest.ensembl.org/info/species?content-type=application/json"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            species_list = json.loads(resp.read().decode())["species"]

        for sp in species_list:
            if (
                sp.get("assembly", "").lower() == genome_lower
                or sp.get("name", "") == genome_lower
                or sp.get("common_name", "").lower() == genome_lower
                or genome_lower in [a.lower() for a in sp.get("aliases", [])]
            ):
                division = _DIVISION_MAP.get(sp.get("division", ""), "ensembl")
                return (sp["name"], sp["assembly"], release, division)
    except Exception as e:
        _LOGGER.warning(f"Ensembl REST API lookup failed: {e}")

    return None


def get_gtf_path(genome: str, out_path: str = None) -> Union[str, None]:
    """
    Get GTF gene annotation file for a genome, downloading from Ensembl if needed.

    Supports UCSC aliases (hg38, hg19, mm10, mm39), Ensembl assembly names
    (GRCh38, GRCm39, GRCz11), and Ensembl species names (danio_rerio, etc.).
    For genomes not in the static map, queries the Ensembl REST API (~380 species).

    Downloaded GTFs are cached and pre-compiled to .bin with ``gtars prep``.

    :param genome: genome assembly name
    :param out_path: cache directory override (default: ~/ensembl_gtf/)
    :return: path to .bin file (or raw .gtf.gz if prep fails), None if unavailable
    """
    _LOGGER.info(f"Getting GTF annotation for genome: {genome}")

    resolved = _resolve_ensembl_genome(genome)
    if not resolved:
        _LOGGER.warning(f"Could not resolve genome '{genome}' to an Ensembl species")
        return None

    species, assembly, release, division = resolved
    species_cap = species[0].upper() + species[1:]
    gtf_filename = f"{species_cap}.{assembly}.{release}.gtf.gz"

    if not out_path:
        gtf_folder = os.path.join(HOME_PATH, GTF_FOLDER_NAME)
    else:
        gtf_folder = os.path.join(out_path, GTF_FOLDER_NAME)

    gtf_path = os.path.join(gtf_folder, gtf_filename)
    gtf_bin_path = gtf_path + ".bin"

    # Return pre-compiled binary if it already exists
    if os.path.exists(gtf_bin_path):
        _LOGGER.info(f"GTF annotation (pre-compiled): {gtf_bin_path}")
        return gtf_bin_path

    if not os.path.exists(gtf_path):
        os.makedirs(gtf_folder, exist_ok=True)
        url = _ensembl_gtf_url(species, assembly, release, division)
        download_file(url=url, path=gtf_path, no_fail=True)

    # Pre-compile to .bin for faster loading on subsequent runs
    if os.path.exists(gtf_path):
        _LOGGER.info(f"Pre-compiling GTF: {gtf_path}")
        result = subprocess.run(
            ["gtars", "prep", "--gtf", gtf_path, "-o", gtf_bin_path],
            capture_output=True, text=True,
        )
        if result.returncode == 0 and os.path.exists(gtf_bin_path):
            _LOGGER.info(f"GTF annotation (pre-compiled): {gtf_bin_path}")
            return gtf_bin_path
        else:
            _LOGGER.warning(f"gtars prep failed, using raw GTF: {result.stderr}")

    if os.path.exists(gtf_path):
        return gtf_path

    _LOGGER.warning(f"GTF annotation not available for {genome}")
    return None


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
    # Auto-download GTF annotation if not provided
    if not ensdb:
        try:
            ensdb = get_gtf_path(genome)
        except Exception:
            _LOGGER.warning(
                f"Could not auto-download GTF for {genome}. "
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

    if precision is not None:
        data = round_floats(data, precision)

    if stop_pipeline and pm:
        pm.stop_pipeline()

    return data
