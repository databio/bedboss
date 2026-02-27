"""Benchmark bedstat pipeline: gtars genomicdist + GC content + compression.

Usage:
    # Prep reference files first (one-time):
    gtars prep --gtf test/sam/gencode.v47.annotation.gtf.gz
    gtars prep --signal-matrix test/sam/openSignalMatrix_hg38.txt.gz

    # Run full benchmark (all encode BED files):
    python test/test_genomicdist.py

    # Run a single file:
    python test/test_genomicdist.py encode_4

    # Component breakdown mode:
    python test/test_genomicdist.py --breakdown

    # Both:
    python test/test_genomicdist.py --breakdown encode_4
"""

import glob
import json
import os
import subprocess
import sys
import tempfile
import time

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Paths — adjust these to your local setup
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_SAM = os.path.join(REPO_ROOT, "test", "sam")
BED_DIR = os.path.join(os.path.dirname(REPO_ROOT), "test")

GTF_BIN = os.path.join(TEST_SAM, "gencode.v47.annotation.gtf.bin")
SIGNAL_BIN = os.path.join(TEST_SAM, "openSignalMatrix_hg38.txt.bin")
CHROM_SIZES = os.path.join(
    os.path.dirname(REPO_ROOT), "gtars", "tests", "hg38.chrom.sizes"
)


from bbconf.modules.aggregation import round_floats


def check_refs():
    missing = []
    if not os.path.exists(GTF_BIN):
        missing.append(
            f"GTF bin not found: {GTF_BIN}\n"
            f"  Run: gtars prep --gtf {TEST_SAM}/gencode.v47.annotation.gtf.gz"
        )
    if not os.path.exists(SIGNAL_BIN):
        missing.append(
            f"Signal matrix bin not found: {SIGNAL_BIN}\n"
            f"  Run: gtars prep --signal-matrix {TEST_SAM}/openSignalMatrix_hg38.txt.gz"
        )
    if not os.path.exists(CHROM_SIZES):
        missing.append(f"Chrom sizes not found: {CHROM_SIZES}")
    if missing:
        print("Missing reference files:\n")
        for m in missing:
            print(f"  {m}\n")
        sys.exit(1)


def find_bed_files(pattern=None):
    bed_files = sorted(glob.glob(os.path.join(BED_DIR, "encode_*.bed.gz")))
    if not bed_files:
        print(f"No encode BED files found in {BED_DIR}")
        sys.exit(1)
    if pattern:
        bed_files = [f for f in bed_files if pattern in os.path.basename(f)]
        if not bed_files:
            print(f"No BED files matching '{pattern}'")
            sys.exit(1)
    return bed_files


def run_e2e(bed_files, save_dir=None):
    """End-to-end bedstat() timing with optional output saving."""
    from bedboss.bedstat.bedstat import bedstat

    if save_dir:
        os.makedirs(save_dir, exist_ok=True)

    print(
        f"{'File':<16} {'Regions':>8} {'Total':>8} "
        f"{'GC content':>12} {'Dist KB':>8}"
    )
    print("-" * 57)

    for bed_path in bed_files:
        label = os.path.basename(bed_path).replace(".bed.gz", "")
        outfolder = tempfile.mkdtemp(prefix=f"bedstat_{label}_")

        t0 = time.time()
        result = bedstat(
            bedfile=bed_path,
            genome="hg38",
            outfolder=outfolder,
            ensdb=GTF_BIN,
            chrom_sizes=CHROM_SIZES,
            open_signal_matrix=SIGNAL_BIN,
        )
        elapsed = time.time() - t0

        n_regions = result.get("number_of_regions", "?")
        gc = result.get("gc_content")
        gc_str = f"{gc:.4f}" if gc is not None else "N/A"
        dist_json = json.dumps(result.get("distributions", {}))
        dist_size = len(dist_json) / 1024

        print(
            f"{label:<16} {n_regions:>8} {elapsed:>7.2f}s "
            f"{gc_str:>12} {dist_size:>7.1f}"
        )

        if save_dir:
            out_path = os.path.join(save_dir, f"{label}_bedstat.json")
            with open(out_path, "w") as f:
                json.dump(round_floats(result), f, separators=(",", ":"))
            print(f"  -> saved {out_path}")


def run_breakdown(bed_files):
    """Component-level timing breakdown."""
    from bedboss.bedstat.compress_distributions import compress_distributions, compress_to_kde
    from bedboss.bedstat.gc_content import calculate_gc_content
    from gtars.models import RegionSet

    print(
        f"{'File':<16} {'Regions':>8} {'gtars':>8} {'JSON rd':>8} "
        f"{'GC calc':>8} {'GC KDE':>8} {'compress':>8} {'Total':>8}"
    )
    print("-" * 82)

    for bed_path in bed_files:
        label = os.path.basename(bed_path).replace(".bed.gz", "")
        outdir = tempfile.mkdtemp(prefix=f"bench_{label}_")
        json_path = os.path.join(outdir, "out.json")

        # 1. gtars genomicdist
        t0 = time.time()
        subprocess.run(
            [
                "gtars", "genomicdist",
                "--bed", bed_path,
                "--output", json_path,
                "--gtf", GTF_BIN,
                "--chrom-sizes", CHROM_SIZES,
                "--signal-matrix", SIGNAL_BIN,
                "--bins", "250",
            ],
            check=True,
            capture_output=True,
        )
        t_gtars = time.time() - t0

        # 2. Read JSON
        t0 = time.time()
        with open(json_path) as f:
            gtars_output = json.load(f)
        t_json = time.time() - t0

        n_regions = gtars_output.get("scalars", {}).get("number_of_regions", "?")

        # 3. GC content calculation (refgenie fasta lookup)
        bed_obj = RegionSet(bed_path)
        t0 = time.time()
        try:
            gc_contents = calculate_gc_content(bedfile=bed_obj, genome="hg38")
        except BaseException:
            gc_contents = None
        t_gc = time.time() - t0

        # 4. GC KDE compression
        t_gc_kde = 0.0
        if gc_contents:
            t0 = time.time()
            compress_to_kde(gc_contents, n_points=512, log_transform=False)
            t_gc_kde = time.time() - t0

        # 5. Compress all distributions (histograms, KDEs, region dist)
        t0 = time.time()
        compress_distributions(gtars_output)
        t_compress = time.time() - t0

        t_total = t_gtars + t_json + t_gc + t_gc_kde + t_compress

        print(
            f"{label:<16} {n_regions:>8} {t_gtars:>7.2f}s {t_json:>7.3f}s "
            f"{t_gc:>7.2f}s {t_gc_kde:>7.3f}s {t_compress:>7.3f}s {t_total:>7.2f}s"
        )


def main():
    check_refs()

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]
    breakdown = "--breakdown" in flags
    save = "--save" in flags
    pattern = args[0] if args else None

    bed_files = find_bed_files(pattern)

    print(f"Reference files:")
    print(f"  GTF bin:        {GTF_BIN}")
    print(f"  Signal bin:     {SIGNAL_BIN}")
    print(f"  Chrom sizes:    {CHROM_SIZES}")
    print(f"  BED files:      {len(bed_files)} in {BED_DIR}")
    print()

    save_dir = TEST_SAM if save else None
    if breakdown:
        run_breakdown(bed_files)
    else:
        run_e2e(bed_files, save_dir=save_dir)


if __name__ == "__main__":
    main()
