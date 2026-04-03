"""Compress raw gtars genomicdist distributions for DB storage.

Produces fixed-size representations (~18KB total) regardless of region count.
Output formats match what bedbase-ui expects for client-side rendering.
"""

from typing import List, Optional

import numpy as np


def compress_to_histogram(
    values: List[float], n_bins: int = 50, trim_percentile: float = 99.0
) -> Optional[dict]:
    """Compress a list of values to a fixed-bin histogram.

    Uses percentile trimming to remove outliers, matching the UI's
    quantileTrimmedHistogram() behavior.

    Returns: {"x_min", "x_max", "bins", "counts", "total"}
    """
    if not values:
        return None

    arr = np.asarray(values, dtype=np.float64)
    total = len(arr)
    cutoff = np.percentile(arr, trim_percentile)
    trimmed = arr[arr <= cutoff]
    if len(trimmed) == 0:
        return None

    x_min = float(trimmed.min())
    x_max = float(trimmed.max())
    if x_min == x_max:
        return {
            "x_min": x_min,
            "x_max": x_max,
            "bins": 1,
            "counts": [int(len(trimmed))],
            "total": total,
        }

    counts, _ = np.histogram(trimmed, bins=n_bins, range=(x_min, x_max))
    overflow = total - len(trimmed)
    return {
        "x_min": x_min,
        "x_max": x_max,
        "bins": n_bins,
        "counts": counts.tolist(),
        "total": total,
        "overflow": overflow,
    }


def compress_to_kde(
    values: List[float],
    n_points: int = 512,
    log_transform: bool = False,
    trim_percentile: float = 99.0,
    max_samples: int = 5000,
) -> Optional[dict]:
    """Compress a list of values to a Gaussian KDE curve.

    Replicates the UI's exact math (genomicdist-plots.ts):
    1. Optional log10 transform (for neighbor_distances)
    2. Percentile trim + downsample
    3. Silverman bandwidth
    4. Evaluate Gaussian kernel over evenly-spaced points

    Returns: {"x_min", "x_max", "n", "densities"}
    """
    if not values or len(values) < 2:
        return None

    arr = np.asarray(values, dtype=np.float64)

    # Optional log10 transform (filter non-positive first)
    if log_transform:
        arr = arr[arr > 0]
        if len(arr) < 2:
            return None
        arr = np.log10(arr)

    if len(arr) < 2:
        return None

    # Percentile trim
    cutoff = np.percentile(arr, trim_percentile)
    trimmed = np.sort(arr[arr <= cutoff])
    if len(trimmed) < 2:
        return None

    # Compute bandwidth stats from FULL trimmed data (matching UI behavior)
    full_n = len(trimmed)
    sd = float(np.std(trimmed, ddof=0))
    if sd == 0:
        sd = 1e-10

    # IQR
    q1 = float(np.percentile(trimmed, 25))
    q3 = float(np.percentile(trimmed, 75))
    iqr = q3 - q1

    # Silverman bandwidth (using full trimmed count, not downsampled)
    h = 0.9 * min(sd, iqr / 1.34 if iqr > 0 else sd) * (full_n**-0.2)

    # Downsample AFTER bandwidth computation (only affects KDE evaluation speed)
    if len(trimmed) > max_samples:
        indices = np.linspace(0, len(trimmed) - 1, max_samples, dtype=int)
        trimmed = trimmed[indices]

    n = len(trimmed)
    if h <= 0:
        h = sd * (n**-0.2)
    if h <= 0:
        return None

    # Evaluation range: min - 3h to max + 3h
    x_min = float(trimmed[0]) - 3 * h
    x_max = float(trimmed[-1]) + 3 * h

    # Vectorized Gaussian kernel evaluation
    xs = np.linspace(x_min, x_max, n_points)
    # Shape: (n_points, n_samples) — broadcast subtract
    u = (xs[:, np.newaxis] - trimmed[np.newaxis, :]) / h
    densities = np.sum(np.exp(-0.5 * u * u), axis=1) / (h * np.sqrt(2 * np.pi) * n)

    return {
        "x_min": round(x_min, 6),
        "x_max": round(x_max, 6),
        "n": n_points,
        "densities": np.round(densities, 8).tolist(),
    }


def compress_tss_histogram(
    values: List[float], n_bins: int = 100, max_distance: float = 100_000.0
) -> Optional[dict]:
    """Compress signed TSS distances to a fixed-range symmetric histogram.

    Expects signed distances (negative = upstream, positive = downstream)
    from gtars calc_feature_distances. Bins into [-max_distance, +max_distance]
    to match the UI's local TSS distance plot.

    Returns: {"x_min", "x_max", "bins", "counts", "total"}
    """
    if not values:
        return None

    arr = np.asarray(values, dtype=np.float64)
    total = len(arr)

    # Clamp to symmetric range
    clamped = arr[(arr >= -max_distance) & (arr <= max_distance)]
    if len(clamped) == 0:
        return None

    counts, _ = np.histogram(clamped, bins=n_bins, range=(-max_distance, max_distance))
    return {
        "x_min": -max_distance,
        "x_max": max_distance,
        "bins": n_bins,
        "counts": counts.tolist(),
        "total": total,
    }


def compress_region_distribution(raw: dict) -> Optional[dict]:
    """Compress per-chromosome region distribution to dense count arrays.

    Input: gtars format {"chr1": [{"start": ..., "end": ..., "rid": ...}, ...], ...}
    Output: {"chr1": [count_at_rid_0, count_at_rid_1, ...], ...}

    The array index is the rid (bin index) used by the UI's faceted chart.
    """
    if not raw:
        return None

    result = {}
    for chrom, regions in raw.items():
        if not regions:
            result[chrom] = []
            continue
        rids = np.array(
            [r.get("rid", 0) if isinstance(r, dict) else 0 for r in regions],
            dtype=np.int32,
        )
        counts_arr = np.array(
            [r.get("n", 1) if isinstance(r, dict) else 1 for r in regions],
            dtype=np.int32,
        )
        bins = np.zeros(rids.max() + 1, dtype=np.int64)
        np.add.at(bins, rids, counts_arr)
        result[chrom] = bins.tolist()

    return result


def compress_distributions(gtars_output: dict) -> dict:
    """Compress all distributions in a gtars genomicdist output.

    Modifies gtars_output["distributions"] in place, replacing raw arrays
    with compressed formats.

    Returns the modified gtars_output.
    """
    dists = gtars_output.get("distributions", {})

    # Widths: histogram (50 bins)
    if "widths" in dists and isinstance(dists["widths"], list):
        dists["widths"] = compress_to_histogram(dists["widths"], n_bins=50)

    # TSS distances: fixed-range histogram (100 bins, 0–100kb)
    if "tss_distances" in dists and isinstance(dists["tss_distances"], list):
        dists["tss_distances"] = compress_tss_histogram(
            dists["tss_distances"], n_bins=100
        )

    # Neighbor distances: KDE with log10 transform
    if "neighbor_distances" in dists and isinstance(dists["neighbor_distances"], list):
        dists["neighbor_distances"] = compress_to_kde(
            dists["neighbor_distances"], n_points=512, log_transform=True
        )

    # Drop nearest_neighbors (redundant with neighbor_distances)
    dists.pop("nearest_neighbors", None)

    # Region distribution: dense count arrays
    # gtars outputs a flat list of {chr, start, end, n, rid}; group by chr first
    if "region_distribution" in dists:
        rd = dists["region_distribution"]
        if isinstance(rd, list):
            grouped = {}
            for entry in rd:
                chrom = entry.get("chr", "unknown")
                grouped.setdefault(chrom, []).append(entry)
            rd = grouped
        if isinstance(rd, dict):
            dists["region_distribution"] = compress_region_distribution(rd)

    # chromosome_stats: unchanged (already compact)

    gtars_output["distributions"] = dists
    return gtars_output
