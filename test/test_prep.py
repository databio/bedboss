"""Smoketests for reference file auto-download and prep (GTF + open signal matrix).

These tests mock refgenie calls and subprocess (gtars prep) so they run
offline and without gtars installed.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from bedboss.bedstat.bedstat import (
    get_gda_path,
    get_gtf_path,
    get_osm_path,
    get_chrom_sizes_path,
)


# ---------------------------------------------------------------------------
# get_gda_path: refgenie seek/pull + gtars prep flow
# ---------------------------------------------------------------------------

def test_get_gda_path_seek_local_and_cached_bin(tmp_path):
    """If rgc.seek finds GTF and .gda.bin exists, return it immediately."""
    gtf_path = str(tmp_path / "hg38.gtf.gz")
    bin_path = gtf_path + ".gda.bin"
    with open(bin_path, "w") as f:
        f.write("fake gda bin")

    mock_rgc = MagicMock()
    mock_rgc.seek.return_value = gtf_path

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc):
        result = get_gda_path("hg38")

    assert result == bin_path


def test_get_gda_path_seek_local_compiles_gda(tmp_path):
    """If rgc.seek finds GTF but no .gda.bin, compile it."""
    gtf_path = str(tmp_path / "hg38.gtf.gz")
    with open(gtf_path, "w") as f:
        f.write("fake gtf")

    def fake_subprocess_run(cmd, **kwargs):
        out_idx = cmd.index("-o") + 1
        with open(cmd[out_idx], "w") as f:
            f.write("fake gda bin")
        return MagicMock(returncode=0)

    mock_rgc = MagicMock()
    mock_rgc.seek.return_value = gtf_path

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc), \
         patch("bedboss.bedstat.bedstat.subprocess.run", side_effect=fake_subprocess_run):
        result = get_gda_path("hg38")

    assert result.endswith(".gda.bin")
    assert os.path.exists(result)


def test_get_gda_path_pulls_when_not_local(tmp_path):
    """If seek fails, pulls from refgenie, then compiles."""
    from refgenconf import RefgenconfError

    gtf_path = str(tmp_path / "hg38.gtf.gz")
    with open(gtf_path, "w") as f:
        f.write("fake gtf")

    def fake_subprocess_run(cmd, **kwargs):
        out_idx = cmd.index("-o") + 1
        with open(cmd[out_idx], "w") as f:
            f.write("fake gda bin")
        return MagicMock(returncode=0)

    mock_rgc = MagicMock()
    mock_rgc.seek.side_effect = [RefgenconfError("not local"), gtf_path]

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc), \
         patch("bedboss.bedstat.bedstat.subprocess.run", side_effect=fake_subprocess_run):
        result = get_gda_path("hg38")

    mock_rgc.pull.assert_called_once_with(genome="hg38", asset="ensembl_gtf", tag="default")
    assert result.endswith(".gda.bin")


def test_get_gda_path_falls_back_to_raw_on_prep_failure(tmp_path):
    """If gtars prep fails, return the raw .gtf.gz."""
    gtf_path = str(tmp_path / "hg38.gtf.gz")
    with open(gtf_path, "w") as f:
        f.write("fake gtf")

    mock_rgc = MagicMock()
    mock_rgc.seek.return_value = gtf_path

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc), \
         patch("bedboss.bedstat.bedstat.subprocess.run",
               return_value=MagicMock(returncode=1, stderr="prep failed")):
        result = get_gda_path("hg38")

    assert result == gtf_path


def test_get_gda_path_returns_none_when_pull_fails():
    """If both seek and pull fail, return None."""
    from refgenconf import RefgenconfError

    mock_rgc = MagicMock()
    mock_rgc.seek.side_effect = RefgenconfError("not local")
    mock_rgc.pull.side_effect = Exception("pull failed")

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc):
        result = get_gda_path("totally_fake_xyz")

    assert result is None


def test_get_gtf_path_alias():
    """get_gtf_path is an alias for get_gda_path."""
    assert get_gtf_path is get_gda_path


# ---------------------------------------------------------------------------
# get_chrom_sizes_path: refgenie primary, seqcol fallback
# ---------------------------------------------------------------------------

def test_get_chrom_sizes_path_seek_local():
    """If rgc.seek finds chrom_sizes locally, return it."""
    mock_rgc = MagicMock()
    mock_rgc.seek.return_value = "/path/to/hg38.chrom.sizes"

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc):
        result = get_chrom_sizes_path("hg38")

    assert result == "/path/to/hg38.chrom.sizes"


def test_get_chrom_sizes_path_pulls_when_not_local():
    """If seek fails, pulls from refgenie."""
    from refgenconf import RefgenconfError

    mock_rgc = MagicMock()
    mock_rgc.seek.side_effect = [
        RefgenconfError("not local"),
        "/path/to/hg38.chrom.sizes",
    ]

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc):
        result = get_chrom_sizes_path("hg38")

    mock_rgc.pull.assert_called_once()
    assert result == "/path/to/hg38.chrom.sizes"


def test_get_chrom_sizes_path_falls_back_to_seqcol():
    """If refgenie fails entirely, falls back to seqcol."""
    from refgenconf import RefgenconfError

    mock_rgc = MagicMock()
    mock_rgc.seek.side_effect = RefgenconfError("not local")
    mock_rgc.pull.side_effect = Exception("pull failed")

    with patch("bedboss.bedstat.bedstat.get_rgc", return_value=mock_rgc), \
         patch("bedboss.bedstat.bedstat._get_chrom_sizes_seqcol",
               return_value="/tmp/chrom_sizes/hg38.chrom.sizes") as mock_seqcol:
        result = get_chrom_sizes_path("hg38")

    mock_seqcol.assert_called_once_with("hg38")
    assert result == "/tmp/chrom_sizes/hg38.chrom.sizes"


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
