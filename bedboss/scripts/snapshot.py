"""
Bulk metadata snapshot of the bedbase database, plus its ``bedboss snapshot``
CLI sub-app.

Two-phase flow with a clean bedboss/bbconf boundary:

- **Phase 1 (bedboss, always runs).** Connect to the database *through bbconf*
  (``BedBaseAgent(...).config.db_engine.engine``) and do all the querying,
  streaming, and Parquet/manifest processing here. Whole tables of interest are
  selected and joined (``bed`` LEFT JOIN ``bed_metadata``, plus ``bedsets`` and
  ``bedfile_bedset_relation``); the export column set and the Parquet schema are
  derived from the bbconf ORM models' columns, so they cannot drift. Rows are
  streamed to zstd Parquet through server-side cursors and never fully
  materialized in memory. Each file is gated against a pre-scan ``count`` taken
  in the same ``REPEATABLE READ`` snapshot.

- **Phase 2 (bbconf, only with ``publish=True``).** After phase 1 succeeds, the
  built artifacts are handed to ``agent.snapshot.add(...)`` as
  ``BedSnapshotArtifact`` models — a single bbconf call that uploads every file
  to S3 (under the fixed ``snapshot/`` prefix) *and* records it in the
  ``bed_snapshots`` table. bedboss never touches boto3 or the ORM directly.

Heavy imports (pyarrow, sqlalchemy, the bbconf agent) are deferred into the
functions that need them so importing this module for its CLI app stays cheap.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
from pathlib import Path

import typer

from bedboss.const import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)

# Bump when the exported column set or types change in a breaking way.
SCHEMA_VERSION = 1

# Rows fetched per server-side cursor round trip / written per Parquet batch.
DEFAULT_BATCH_SIZE = 20_000


# --------------------------------------------------------------------------- #
# CLI sub-app: ``bedboss snapshot ...`` (registered in bedboss/cli.py).
# --------------------------------------------------------------------------- #

snapshot_app = typer.Typer(
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    help="Create and manage bulk metadata snapshots of the bedbase database.",
)


@snapshot_app.command(
    "new",
    help="Build a new bulk metadata snapshot (Parquet exports + manifest), "
    "optionally publishing it to S3 and the database.",
)
def snapshot_new(
    bedbase_config: str = typer.Option(
        ...,
        help="Path to the bedbase config file",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    outfolder: str = typer.Option(
        "exports", help="Local directory for the built artifacts"
    ),
    batch_size: int = typer.Option(
        DEFAULT_BATCH_SIZE, help="Rows per server-side fetch / Parquet write batch"
    ),
    fail_threshold: float = typer.Option(
        0.01, help="Abort without publishing if this fraction of rows is missing"
    ),
    publish: bool = typer.Option(
        False, help="Upload artifacts to S3 and record rows in bed_snapshots"
    ),
):
    run_snapshot(
        bedbase_config=bedbase_config,
        output_dir=outfolder,
        batch_size=batch_size,
        fail_threshold=fail_threshold,
        publish=publish,
    )


@snapshot_app.command("list", help="List bulk metadata snapshots, newest first.")
def snapshot_list(
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    file_type: str = typer.Option(None, help="Filter by file type"),
    limit: int = typer.Option(100, help="Maximum number of rows to return"),
    offset: int = typer.Option(0, help="Number of rows to skip"),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config, init_ml=False)
    result = bbagent.snapshot.list(file_type=file_type, limit=limit, offset=offset)
    print(f"Total snapshots: {result.count}")
    for row in result.results:
        print(
            f"{row.creation_date}  {row.file_type:18}  "
            f"rows={row.record_count}  cksum={(row.checksum or '')[:12]}  "
            f"{row.file_path}"
        )


@snapshot_app.command("delete", help="Delete a snapshot index row by id.")
def snapshot_delete(
    id: int = typer.Option(..., help="Snapshot row id"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    remove_s3: bool = typer.Option(
        True, help="Also delete the underlying S3 object"
    ),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config, init_ml=False)
    bbagent.snapshot.delete(id, remove_s3=remove_s3)
    print(f"Snapshot {id} deleted from the bedbase database")


@snapshot_app.command(
    "delete-by-checksum", help="Delete snapshot index row(s) by checksum."
)
def snapshot_delete_by_checksum(
    checksum: str = typer.Option(..., help="SHA256 checksum of the snapshot file"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    remove_s3: bool = typer.Option(
        True, help="Also delete the underlying S3 object(s)"
    ),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config, init_ml=False)
    bbagent.snapshot.delete_by_checksum(checksum, remove_s3=remove_s3)
    print(f"Snapshot(s) with checksum {checksum} deleted from the bedbase database")


# --------------------------------------------------------------------------- #
# Column plans derived from the bbconf ORM models (no hand-listed columns).
# --------------------------------------------------------------------------- #


class ColumnPlan:
    """
    An ordered set of SQLAlchemy columns plus the matching Parquet schema.

    Built from whole ORM tables so the SELECT projection and the Parquet schema
    are derived from one source and stay tied to the bbconf models.
    """

    def __init__(self, columns: list, from_clause=None):
        import pyarrow as pa

        self.columns = columns
        self.from_clause = from_clause
        self.names = [c.name for c in columns]
        self._pa_types = [_pa_type(c) for c in columns]
        # Indices of JSON columns, whose values are serialized to a string.
        self.json_idx = {
            i for i, c in enumerate(columns) if _python_type(c) is dict
        }
        self.schema = pa.schema(list(zip(self.names, self._pa_types)))

    def select(self):
        """A ``SELECT`` of exactly these columns (over the joined FROM, if any)."""
        from sqlalchemy import select

        stmt = select(*self.columns)
        if self.from_clause is not None:
            stmt = stmt.select_from(self.from_clause)
        return stmt

    def count_from(self):
        """The table these columns' first entry belongs to, for ``count(*)``."""
        return self.columns[0].table


def _python_type(column):
    try:
        return column.type.python_type
    except (NotImplementedError, AttributeError):
        return str


def _pa_type(column):
    """Map a SQLAlchemy column's Python type onto a pyarrow type."""
    import pyarrow as pa

    pytype = _python_type(column)
    if pytype is bool:
        return pa.bool_()
    if pytype is int:
        return pa.int64()
    if pytype is float:
        return pa.float64()
    if pytype is datetime.datetime:
        return pa.timestamp("us", tz="UTC")
    if pytype is list:
        return pa.list_(pa.string())
    if pytype is dict:
        # JSON/JSONB -> serialized string.
        return pa.string()
    return pa.string()


def metadata_plan() -> ColumnPlan:
    """``bed`` LEFT JOIN ``bed_metadata`` — all columns of both tables.

    The duplicate join key ``bed_metadata.id`` (equal to ``bed.id``) is dropped.
    """
    from bbconf.db_utils import Bed, BedMetadata

    bed_cols = list(Bed.__table__.columns)
    meta_cols = [c for c in BedMetadata.__table__.columns if c.name != "id"]
    from_clause = Bed.__table__.outerjoin(
        BedMetadata.__table__, Bed.__table__.c.id == BedMetadata.__table__.c.id
    )
    return ColumnPlan(bed_cols + meta_cols, from_clause=from_clause)


def bedsets_plan() -> ColumnPlan:
    from bbconf.db_utils import BedSets

    return ColumnPlan(list(BedSets.__table__.columns))


def membership_plan() -> ColumnPlan:
    from bbconf.db_utils import BedFileBedSetRelation

    return ColumnPlan(list(BedFileBedSetRelation.__table__.columns))


# --------------------------------------------------------------------------- #
# Pure helpers (no DB, no network).
# --------------------------------------------------------------------------- #


def _coerce_row(row, json_idx: set) -> tuple:
    """Turn a result row into a value tuple, serializing JSON columns."""
    values = list(row)
    for i in json_idx:
        if values[i] is not None:
            values[i] = json.dumps(values[i])
    return tuple(values)


def rows_to_table(rows, schema):
    """Convert a batch of row tuples (in schema column order) to a pyarrow Table."""
    import pyarrow as pa

    if rows:
        columns = list(zip(*rows))
    else:
        columns = [[] for _ in range(len(schema))]
    arrays = [
        pa.array(list(col), type=schema.field(i).type)
        for i, col in enumerate(columns)
    ]
    return pa.table(arrays, schema=schema)


def check_completeness(rows_written: int, expected_count: int, threshold: float):
    """
    Refuse to publish a partial artifact.

    Raises if the fraction of rows missing versus the pre-scan ``count`` exceeds
    ``threshold``. Extra rows (concurrent ingest) never trip it.
    """
    if expected_count <= 0:
        return
    shortfall = (expected_count - rows_written) / expected_count
    if shortfall > threshold:
        raise RuntimeError(
            f"Wrote {rows_written} rows but expected ~{expected_count} "
            f"(short by {shortfall:.2%} > {threshold:.2%}). "
            f"Refusing to publish a partial artifact."
        )


def sha256_file(path, chunk: int = 1 << 20) -> str:
    """Streaming SHA256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def file_entry(path) -> dict:
    """Manifest entry describing a written file: name, bytes, sha256."""
    path = Path(path)
    return {
        "name": path.name,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def write_parquet_batches(path, schema, batch_iter) -> int:
    """
    Stream batches (lists of row tuples) into a zstd Parquet file.

    Returns the number of rows actually written (the value the manifest and the
    integrity gate must use).
    """
    import pyarrow.parquet as pq

    rows_written = 0
    writer = pq.ParquetWriter(str(path), schema, compression="zstd")
    try:
        for batch in batch_iter:
            if not batch:
                continue
            writer.write_table(rows_to_table(batch, schema))
            rows_written += len(batch)
    finally:
        writer.close()
    return rows_written


# --------------------------------------------------------------------------- #
# Database streaming (server-side cursors; sequential, unordered scans).
# --------------------------------------------------------------------------- #


def stream_plan(conn, plan: ColumnPlan, batch: int):
    """Yield lists of coerced row tuples from a streamed, unordered scan."""
    result = conn.execute(plan.select().execution_options(yield_per=batch))
    for partition in result.partitions():
        yield [_coerce_row(row, plan.json_idx) for row in partition]


def count_rows(conn, plan: ColumnPlan) -> int:
    from sqlalchemy import func, select

    return conn.execute(
        select(func.count()).select_from(plan.count_from())
    ).scalar_one()


def build_exports(
    conn,
    out_dir: Path,
    date_str: str,
    batch_size: int,
    fail_threshold: float,
) -> list[dict]:
    """
    Write the three Parquet files, gating each against its pre-scan count.

    Returns a list of per-file records: path, name, file_type, record_count,
    file_size, checksum. All reads happen in the caller's REPEATABLE READ
    transaction.
    """
    records = []

    exports = [
        ("metadata", metadata_plan()),
        ("bedsets", bedsets_plan()),
        ("bedset_membership", membership_plan()),
    ]

    for file_type, plan in exports:
        expected = count_rows(conn, plan)
        fname = f"bedbase_{file_type}_{date_str}.parquet"
        path = out_dir / fname
        _LOGGER.info(f"Exporting {file_type}: ~{expected} rows -> {fname}")

        rows_written = write_parquet_batches(
            path, plan.schema, stream_plan(conn, plan, batch_size)
        )
        check_completeness(rows_written, expected, fail_threshold)

        entry = file_entry(path)
        record = {
            "path": str(path),
            "name": entry["name"],
            "file_type": file_type,
            "record_count": rows_written,
            "file_size": entry["bytes"],
            "checksum": entry["sha256"],
        }
        records.append(record)
        _LOGGER.info(
            f"  wrote {rows_written} rows, {entry['bytes'] / 1e6:.1f} MB, "
            f"sha256={entry['sha256'][:12]}…"
        )

    return records


def write_manifest(
    out_dir: Path,
    date_str: str,
    records: list[dict],
    started: str,
    ended: str,
    source_db: str,
) -> dict:
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "build_started": started,
        "build_ended": ended,
        "source_database": source_db,
        "files": [
            {
                "name": r["name"],
                "file_type": r["file_type"],
                "rows": r["record_count"],
                "bytes": r["file_size"],
                "sha256": r["checksum"],
            }
            for r in records
        ],
    }
    path = out_dir / f"manifest_{date_str}.json"
    path.write_text(json.dumps(manifest, indent=2))

    entry = file_entry(path)
    record = {
        "path": str(path),
        "name": entry["name"],
        "file_type": "manifest",
        "record_count": None,
        "file_size": entry["bytes"],
        "checksum": entry["sha256"],
    }
    return record


# --------------------------------------------------------------------------- #
# Orchestration.
# --------------------------------------------------------------------------- #


def run_snapshot(
    bedbase_config: str,
    output_dir: str | Path = "exports",
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_threshold: float = 0.01,
    publish: bool = False,
) -> None:
    """
    Build a bulk metadata snapshot, optionally publishing it.

    Args:
        bedbase_config: Path to the bedbase config file.
        output_dir: Local directory for the built artifacts.
        batch_size: Rows per server-side fetch / Parquet write batch.
        fail_threshold: Abort without publishing if this fraction of rows is
            missing versus the pre-scan count.
        publish: If True, upload the artifacts to S3 and record them in the
            ``bed_snapshots`` table (both done inside bbconf).
    """
    from bbconf.bbagent import BedBaseAgent
    from bbconf.models.base_models import BedSnapshotArtifact

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.timezone.utc)
    date_str = now.strftime("%Y_%m_%d")
    started = now.isoformat()

    # Connect through bbconf; querying/processing stays here in bedboss.
    agent = BedBaseAgent(bedbase_config, init_ml=False)
    source_db = agent.config.config.database.database

    # One REPEATABLE READ transaction: counts and the full scans share a
    # consistent snapshot.
    with agent.config.db_engine.engine.connect().execution_options(
        isolation_level="REPEATABLE READ"
    ) as conn:
        records = build_exports(conn, out_dir, date_str, batch_size, fail_threshold)

    ended = datetime.datetime.now(datetime.timezone.utc).isoformat()
    manifest_record = write_manifest(
        out_dir, date_str, records, started, ended, source_db
    )
    _LOGGER.info(f"Manifest: {manifest_record['path']}")

    if not publish:
        _LOGGER.info(
            "publish=False; artifacts left on disk, nothing uploaded or recorded."
        )
        return

    # Phase 2: hand off to bbconf, which does the S3 upload AND the SQL insert.
    artifacts = [
        BedSnapshotArtifact(
            path=r["path"],
            file_type=r["file_type"],
            record_count=r["record_count"],
            file_size=r["file_size"],
            checksum=r["checksum"],
            schema_version=SCHEMA_VERSION,
        )
        for r in records + [manifest_record]
    ]
    result = agent.snapshot.add(artifacts, creation_date=now)

    http_prefix = None
    if agent.config.config.access_methods and agent.config.config.access_methods.http:
        http_prefix = agent.config.config.access_methods.http.prefix
    for row in result.results:
        url = f"{http_prefix}{row.file_path}" if http_prefix else row.file_path
        _LOGGER.info(f"Published {url}")
