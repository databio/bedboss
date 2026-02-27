"""Smoketests for reference file auto-download and prep (GTF + open signal matrix).

These tests mock network calls and subprocess (gtars prep) so they run
offline and without gtars installed.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from bedboss.bedstat.bedstat import (
    _ensembl_gtf_url,
    _resolve_ensembl_genome,
    get_gtf_path,
    get_osm_path,
)
from bedboss.const import ENSEMBL_GENOMES


# ---------------------------------------------------------------------------
# _resolve_ensembl_genome: static map
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "genome,expected_species,expected_assembly",
    [
        ("hg38", "homo_sapiens", "GRCh38"),
        ("GRCh38", "homo_sapiens", "GRCh38"),
        ("hg19", "homo_sapiens", "GRCh37"),
        ("hg18", "homo_sapiens", "NCBI36"),
        ("mm39", "mus_musculus", "GRCm39"),
        ("mm10", "mus_musculus", "GRCm38"),
        ("mm9", "mus_musculus", "NCBIM37"),
        ("dm6", "drosophila_melanogaster", "BDGP6.54"),
        ("dm3", "drosophila_melanogaster", "BDGP5"),
        ("ce11", "caenorhabditis_elegans", "WBcel235"),
        ("ce10", "caenorhabditis_elegans", "WS220"),
        ("sacCer3", "saccharomyces_cerevisiae", "R64-1-1"),
        ("saccer3", "saccharomyces_cerevisiae", "R64-1-1"),
        ("tair10", "arabidopsis_thaliana", "TAIR10"),
        ("ax4", "dictyostelium_discoideum", "dicty_2.7"),
    ],
)
def test_resolve_static(genome, expected_species, expected_assembly):
    result = _resolve_ensembl_genome(genome)
    assert result is not None
    species, assembly, release, division = result
    assert species == expected_species
    assert assembly == expected_assembly
    assert isinstance(release, int)
    assert division in ("ensembl", "grch37", "plants", "fungi", "protists", "metazoa")


def test_resolve_unknown_no_network():
    """Unknown genome with network mocked out returns None."""
    with patch("bedboss.bedstat.bedstat.urllib.request.urlopen", side_effect=Exception("no network")):
        result = _resolve_ensembl_genome("totally_fake_genome_xyz")
    assert result is None


# ---------------------------------------------------------------------------
# _ensembl_gtf_url: URL construction
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "args,expected_url",
    [
        (
            ("homo_sapiens", "GRCh38", 114, "ensembl"),
            "https://ftp.ensembl.org/pub/release-114/gtf/homo_sapiens/Homo_sapiens.GRCh38.114.gtf.gz",
        ),
        (
            ("homo_sapiens", "GRCh37", 87, "grch37"),
            "https://ftp.ensembl.org/pub/grch37/release-87/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.gtf.gz",
        ),
        (
            ("arabidopsis_thaliana", "TAIR10", 62, "plants"),
            "https://ftp.ebi.ac.uk/ensemblgenomes/pub/plants/release-62/gtf/arabidopsis_thaliana/Arabidopsis_thaliana.TAIR10.62.gtf.gz",
        ),
        (
            ("dictyostelium_discoideum", "dicty_2.7", 62, "protists"),
            "https://ftp.ebi.ac.uk/ensemblgenomes/pub/protists/release-62/gtf/dictyostelium_discoideum/Dictyostelium_discoideum.dicty_2.7.62.gtf.gz",
        ),
    ],
)
def test_ensembl_gtf_url(args, expected_url):
    assert _ensembl_gtf_url(*args) == expected_url


# ---------------------------------------------------------------------------
# get_gtf_path: download + prep flow
# ---------------------------------------------------------------------------

def test_get_gtf_path_returns_cached_bin(tmp_path):
    """If .bin already exists, return it immediately without downloading."""
    gtf_dir = tmp_path / "ensembl_gtf"
    gtf_dir.mkdir()
    bin_file = gtf_dir / "Homo_sapiens.GRCh38.114.gtf.gz.bin"
    bin_file.write_bytes(b"fake bin")

    result = get_gtf_path("hg38", out_path=str(tmp_path))
    assert result == str(bin_file)


def test_get_gtf_path_downloads_and_preps(tmp_path):
    """Mocked download + successful gtars prep returns .bin path."""
    def fake_download(url, path, no_fail=False):
        with open(path, "w") as f:
            f.write("fake gtf")

    def fake_subprocess_run(cmd, **kwargs):
        # Simulate gtars prep creating the .bin file
        out_idx = cmd.index("-o") + 1
        with open(cmd[out_idx], "w") as f:
            f.write("fake bin")
        return MagicMock(returncode=0)

    with patch("bedboss.bedstat.bedstat.download_file", side_effect=fake_download), \
         patch("bedboss.bedstat.bedstat.subprocess.run", side_effect=fake_subprocess_run):
        result = get_gtf_path("hg38", out_path=str(tmp_path))

    assert result is not None
    assert result.endswith(".bin")
    assert os.path.exists(result)


def test_get_gtf_path_falls_back_to_raw(tmp_path):
    """If gtars prep fails, return the raw .gtf.gz."""
    def fake_download(url, path, no_fail=False):
        with open(path, "w") as f:
            f.write("fake gtf")

    with patch("bedboss.bedstat.bedstat.download_file", side_effect=fake_download), \
         patch("bedboss.bedstat.bedstat.subprocess.run", return_value=MagicMock(returncode=1, stderr="prep failed")):
        result = get_gtf_path("hg38", out_path=str(tmp_path))

    assert result is not None
    assert result.endswith(".gtf.gz")
    assert os.path.exists(result)


def test_get_gtf_path_unresolvable_genome(tmp_path):
    """Unresolvable genome returns None."""
    with patch("bedboss.bedstat.bedstat.urllib.request.urlopen", side_effect=Exception("no network")):
        result = get_gtf_path("totally_fake_xyz", out_path=str(tmp_path))
    assert result is None


# ---------------------------------------------------------------------------
# get_osm_path: download + prep flow
# ---------------------------------------------------------------------------

def test_get_osm_path_returns_cached_bin(tmp_path):
    """If .bin already exists, return it immediately."""
    from bedboss.const import OS_HG38, OPEN_SIGNAL_FOLDER_NAME

    osm_dir = tmp_path / OPEN_SIGNAL_FOLDER_NAME
    osm_dir.mkdir()
    bin_file = osm_dir / (OS_HG38 + ".bin")
    bin_file.write_bytes(b"fake bin")

    result = get_osm_path("hg38", out_path=str(tmp_path))
    assert result == str(bin_file)


def test_get_osm_path_downloads_and_preps(tmp_path):
    """Mocked download + successful gtars prep returns .bin path."""
    def fake_download(url, path, no_fail=False):
        with open(path, "w") as f:
            f.write("fake osm")

    def fake_subprocess_run(cmd, **kwargs):
        out_idx = cmd.index("-o") + 1
        with open(cmd[out_idx], "w") as f:
            f.write("fake bin")
        return MagicMock(returncode=0)

    with patch("bedboss.bedstat.bedstat.download_file", side_effect=fake_download), \
         patch("bedboss.bedstat.bedstat.subprocess.run", side_effect=fake_subprocess_run):
        result = get_osm_path("hg38", out_path=str(tmp_path))

    assert result is not None
    assert result.endswith(".bin")
    assert os.path.exists(result)


def test_get_osm_path_falls_back_to_raw(tmp_path):
    """If gtars prep fails, return the raw file."""
    def fake_download(url, path, no_fail=False):
        with open(path, "w") as f:
            f.write("fake osm")

    with patch("bedboss.bedstat.bedstat.download_file", side_effect=fake_download), \
         patch("bedboss.bedstat.bedstat.subprocess.run", return_value=MagicMock(returncode=1, stderr="prep failed")):
        result = get_osm_path("hg38", out_path=str(tmp_path))

    assert result is not None
    assert not result.endswith(".bin")
    assert os.path.exists(result)


def test_get_osm_path_unsupported_genome():
    """Unsupported genome raises OpenSignalMatrixException."""
    from bedboss.exceptions import OpenSignalMatrixException

    with pytest.raises(OpenSignalMatrixException):
        get_osm_path("totally_fake_xyz")


# ---------------------------------------------------------------------------
# Static map completeness
# ---------------------------------------------------------------------------

def test_all_static_entries_produce_valid_urls():
    """Every entry in ENSEMBL_GENOMES produces a well-formed URL."""
    seen = set()
    for genome, (species, assembly, release, division) in ENSEMBL_GENOMES.items():
        url = _ensembl_gtf_url(species, assembly, release, division)
        assert url.startswith("https://")
        assert url.endswith(".gtf.gz")
        assert species in url
        assert str(release) in url
        seen.add((species, assembly, release, division))

    # Sanity: we have at least the core genomes
    assert len(seen) >= 10
