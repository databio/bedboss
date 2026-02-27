"""End-to-end test for bedset creation and aggregation.

Self-contained: seeds the database from saved bedstat JSON files, creates
a bedset, reads it back through every accessor, and cleans up.

Usage:
    # Full self-contained run (seed → create → verify → cleanup):
    python test/test_bedset.py

    # Seed only (insert test BED records into DB):
    python test/test_bedset.py --seed

    # Remove seeded data only:
    python test/test_bedset.py --unseed

    # Run against pre-existing data (skip auto-seed):
    python test/test_bedset.py --no-seed

    # Specify how many BED IDs to group (default: all in DB):
    python test/test_bedset.py --limit 5

    # Use a specific config:
    python test/test_bedset.py --config path/to/config.yaml

    # Inspect the resulting BedSetStats JSON:
    python test/test_bedset.py --dump

    # Clean up (delete the test bedset from the DB):
    python test/test_bedset.py --cleanup
"""

import argparse
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_SAM = os.path.join(REPO_ROOT, "test", "sam")
DEFAULT_CONFIG = os.path.join(TEST_SAM, "bedbase_config.yaml")
BEDSET_ID = "test_bedset_smoketest"
SEED_PREFIX = "bedset_test_"  # prefix for seeded BED IDs


from bbconf.modules.aggregation import round_floats


def find_bedstat_jsons():
    """Find all encode_*_bedstat.json files in test/sam/."""
    pattern = os.path.join(TEST_SAM, "encode_*_bedstat.json")
    return sorted(glob.glob(pattern))


def run_seed(bbagent):
    """Insert minimal Bed + BedStats records from saved bedstat JSON files.

    Uses direct SQLAlchemy inserts (not the full bedboss pipeline) so this
    test is self-contained and doesn't need reference genomes or BED files.
    """
    from sqlalchemy.orm import Session
    from bbconf.db_utils import Bed, BedStats

    json_files = find_bedstat_jsons()
    if not json_files:
        print(f"No bedstat JSON files found in {TEST_SAM}")
        sys.exit(1)

    engine = bbagent.config.db_engine.engine
    inserted = 0

    with Session(engine) as session:
        for json_path in json_files:
            # e.g. "encode_4_bedstat.json" → "bedset_test_encode_4"
            stem = os.path.basename(json_path).replace("_bedstat.json", "")
            bed_id = f"{SEED_PREFIX}{stem}"

            # Skip if already exists
            existing = session.get(Bed, bed_id)
            if existing:
                continue

            with open(json_path) as f:
                data = json.load(f)

            # Insert Bed record (minimal)
            bed = Bed(id=bed_id, genome_alias="hg38", name=f"Test {stem}")
            session.add(bed)
            session.flush()

            # Extract scalar stats (everything except 'distributions')
            stat_fields = {
                k: v for k, v in data.items()
                if k != "distributions" and isinstance(v, (int, float))
            }
            stat_fields["distributions"] = data.get("distributions")

            bed_stats = BedStats(id=bed_id, **stat_fields)
            session.add(bed_stats)
            inserted += 1

        session.commit()

    print(f"Seeded {inserted} BED records (skipped {len(json_files) - inserted} existing)")
    return inserted


def run_unseed(bbagent):
    """Remove seeded BED records (by prefix)."""
    from sqlalchemy.orm import Session
    from sqlalchemy import delete
    from bbconf.db_utils import Bed

    engine = bbagent.config.db_engine.engine

    with Session(engine) as session:
        result = session.execute(
            delete(Bed).where(Bed.id.like(f"{SEED_PREFIX}%"))
        )
        session.commit()

    print(f"Removed {result.rowcount} seeded BED records")
    return result.rowcount


def get_bed_ids(bbagent, limit=None):
    """Fetch BED file IDs that have distributions in the database."""
    from sqlalchemy.orm import Session
    from sqlalchemy import select
    from bbconf.db_utils import BedStats

    with Session(bbagent.config.db_engine.engine) as session:
        stmt = (
            select(BedStats.id)
            .where(BedStats.distributions.isnot(None))
            .order_by(BedStats.id)
        )
        if limit:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()

    return [row[0] for row in rows]


def run_create(bbagent, bed_ids):
    """Create a bedset and return timing + stats."""
    print(f"Creating bedset '{BEDSET_ID}' from {len(bed_ids)} BED files...")
    print(f"  IDs: {bed_ids[:5]}{'...' if len(bed_ids) > 5 else ''}")
    print()

    t0 = time.time()
    bbagent.bedset.create(
        identifier=BEDSET_ID,
        name="Smoketest Bedset",
        description=f"End-to-end test with {len(bed_ids)} files",
        bedid_list=bed_ids,
        statistics=True,
        annotation={"author": "test", "source": "test_bedset.py"},
        overwrite=True,
    )
    t_create = time.time() - t0

    return t_create


def run_read(bbagent):
    """Read back the bedset through every accessor and verify."""
    errors = []

    # --- get(full=True) ---
    t0 = time.time()
    meta = bbagent.bedset.get(BEDSET_ID, full=True)
    t_get = time.time() - t0

    if not meta.statistics:
        errors.append("get(full=True).statistics is None")
    else:
        stats = meta.statistics
        checks = {
            "n_files > 0": stats.n_files > 0,
            "scalar_summaries": stats.scalar_summaries is not None,
            "tss_histogram": stats.tss_histogram is not None,
            "widths_histogram": stats.widths_histogram is not None,
            "neighbor_distances": stats.neighbor_distances is not None,
            "gc_content": stats.gc_content is not None,
            "region_distribution": stats.region_distribution is not None,
            "partitions": stats.partitions is not None,
            "chromosome_summaries": stats.chromosome_summaries is not None,
            "composition": stats.composition is not None,
        }
        for name, passed in checks.items():
            if not passed:
                errors.append(f"stats.{name} failed")

        # Verify scalar structure
        if stats.scalar_summaries:
            for key in ["number_of_regions", "mean_region_width", "median_tss_dist"]:
                entry = stats.scalar_summaries.get(key)
                if not entry:
                    errors.append(f"scalar_summaries['{key}'] missing")
                elif "mean" not in entry:
                    errors.append(f"scalar_summaries['{key}'] has no 'mean'")

    # --- get_statistics() ---
    t0 = time.time()
    stats2 = bbagent.bedset.get_statistics(BEDSET_ID)
    t_stats = time.time() - t0

    if stats2.n_files != meta.statistics.n_files:
        errors.append(
            f"get_statistics().n_files ({stats2.n_files}) != "
            f"get(full=True).statistics.n_files ({meta.statistics.n_files})"
        )

    # --- get_bedset_bedfiles() ---
    t0 = time.time()
    bedfiles = bbagent.bedset.get_bedset_bedfiles(BEDSET_ID)
    t_bedfiles = time.time() - t0

    if bedfiles.count != meta.statistics.n_files:
        errors.append(
            f"bedfiles.count ({bedfiles.count}) != n_files ({meta.statistics.n_files})"
        )

    # --- get_ids_list() ---
    t0 = time.time()
    listing = bbagent.bedset.get_ids_list(query="Smoketest", limit=10, offset=0)
    t_list = time.time() - t0

    if listing.count == 0:
        errors.append("get_ids_list(query='Smoketest') returned 0 results")

    # --- exists() ---
    if not bbagent.bedset.exists(BEDSET_ID):
        errors.append("exists() returned False after create")

    return {
        "t_get": t_get,
        "t_stats": t_stats,
        "t_bedfiles": t_bedfiles,
        "t_list": t_list,
        "errors": errors,
        "meta": meta,
    }


def run_cleanup(bbagent):
    """Delete the test bedset."""
    if bbagent.bedset.exists(BEDSET_ID):
        bbagent.bedset.delete(BEDSET_ID)
        print(f"Deleted bedset '{BEDSET_ID}'")
    else:
        print(f"Bedset '{BEDSET_ID}' does not exist, nothing to clean up")


def print_stats_summary(stats):
    """Print a human-readable summary of BedSetStats."""
    print(f"  n_files:             {stats.n_files}")

    if stats.scalar_summaries:
        print(f"  scalar_summaries:    {len(stats.scalar_summaries)} scalars")
        for k, v in stats.scalar_summaries.items():
            mean = v.get("mean", "?")
            sd = v.get("sd", "?")
            n = v.get("n", "?")
            print(f"    {k:<25} mean={mean:<12.4f} sd={sd:<12.4f} n={n}")

    if stats.composition:
        print(f"  composition:")
        for field, counts in stats.composition.items():
            n_vals = len(counts) if isinstance(counts, dict) else "?"
            print(f"    {field:<25} {n_vals} distinct values")

    curve_fields = [
        ("tss_histogram", stats.tss_histogram),
        ("widths_histogram", stats.widths_histogram),
        ("neighbor_distances", stats.neighbor_distances),
        ("gc_content", stats.gc_content),
    ]
    for name, val in curve_fields:
        if val:
            n_pts = len(val.get("mean", []))
            print(f"  {name:<22} {n_pts} points")
        else:
            print(f"  {name:<22} None")

    if stats.region_distribution:
        n_chroms = len(stats.region_distribution)
        print(f"  region_distribution:   {n_chroms} chromosomes")

    if stats.partitions:
        cats = list(stats.partitions.keys())
        print(f"  partitions:            {len(cats)} categories ({', '.join(cats[:4])}...)")

    if stats.chromosome_summaries:
        n_chroms = len(stats.chromosome_summaries)
        print(f"  chromosome_summaries:  {n_chroms} chromosomes")


def print_json_size(stats):
    """Print the JSON size of the stats object."""
    j = json.dumps(stats.model_dump(), default=str)
    print(f"  JSON size:             {len(j) / 1024:.1f} KB")


def main():
    parser = argparse.ArgumentParser(description="Bedset creation smoketest")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="bedbase config path")
    parser.add_argument("--limit", type=int, default=None, help="max BED files to include")
    parser.add_argument("--dump", action="store_true", help="dump BedSetStats as JSON")
    parser.add_argument("--save", action="store_true", help="save BedSetStats JSON to test/sam/")
    parser.add_argument("--cleanup", action="store_true", help="delete test bedset and exit")
    parser.add_argument("--seed", action="store_true", help="seed test data and exit")
    parser.add_argument("--unseed", action="store_true", help="remove seeded test data and exit")
    parser.add_argument(
        "--no-seed", action="store_true",
        help="skip auto-seeding (use pre-existing data only)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Config not found: {args.config}")
        sys.exit(1)

    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(args.config)

    if args.cleanup:
        run_cleanup(bbagent)
        return

    if args.seed:
        run_seed(bbagent)
        return

    if args.unseed:
        run_unseed(bbagent)
        return

    # Find BED IDs with distributions in the database
    bed_ids = get_bed_ids(bbagent, limit=args.limit)

    # Auto-seed if no data found (unless --no-seed)
    seeded = False
    if not bed_ids and not args.no_seed:
        print("No BED files with distributions found — auto-seeding from JSON files...")
        run_seed(bbagent)
        bed_ids = get_bed_ids(bbagent, limit=args.limit)
        seeded = True
        print()

    if not bed_ids:
        print("No BED files with distributions found in database.")
        if args.no_seed:
            print("Run with --seed first, or without --no-seed to auto-seed.")
        sys.exit(1)

    print(f"Config:    {args.config}")
    print(f"BED files: {len(bed_ids)} with distributions")
    print()

    # --- Create ---
    t_create = run_create(bbagent, bed_ids)
    print(f"  create:  {t_create:.3f}s")
    print()

    # --- Read back ---
    print("Reading back...")
    result = run_read(bbagent)
    print(f"  get(full=True):      {result['t_get']:.3f}s")
    print(f"  get_statistics():    {result['t_stats']:.3f}s")
    print(f"  get_bedset_bedfiles: {result['t_bedfiles']:.3f}s")
    print(f"  get_ids_list:        {result['t_list']:.3f}s")
    print()

    # --- Stats summary ---
    stats = result["meta"].statistics
    print("BedSetStats summary:")
    print_stats_summary(stats)
    print_json_size(stats)
    print()

    # --- Errors ---
    if result["errors"]:
        print(f"FAILED — {len(result['errors'])} errors:")
        for e in result["errors"]:
            print(f"  x {e}")
        sys.exit(1)
    else:
        print("PASSED — all checks passed")

    # --- Dump / Save ---
    if args.dump:
        print()
        print(json.dumps(round_floats(stats.model_dump()), indent=2, default=str))

    if args.save:
        out_path = os.path.join(TEST_SAM, "bedsetstats.json")
        with open(out_path, "w") as f:
            json.dump(round_floats(stats.model_dump()), f, separators=(",", ":"), default=str)
        print(f"Saved BedSetStats to {out_path}")

    # --- Cleanup ---
    run_cleanup(bbagent)
    if seeded:
        run_unseed(bbagent)


if __name__ == "__main__":
    main()
