"""
HPC orchestration for qdrant reindexing.

Fetches bed metadata from the database, splits into chunks, generates
SLURM sbatch scripts for parallel vectorization, and manages job state.

Modeled after bedboss/bedboss_hpc.py.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"
CHUNKS_DIR = "chunks"
STATE_DIR = "state"

# ---------------------------------------------------------------------------
# sbatch templates
# ---------------------------------------------------------------------------

REGION_TEMPLATE = """\
#!/bin/bash

#SBATCH --account={account}
#SBATCH --ntasks={ntasks}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem={mem}
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --job-name=qdrant-region-{chunk_id}
#SBATCH -o {logs_dir}/{chunk_id}.out
#SBATCH -e {logs_dir}/{chunk_id}.err

echo "Hello $USER, node $(hostname). Running {chunk_id} (region vectorization)."

bedboss qdrant vectorize-region \\
    --chunk-pep {chunk_pep_path} \\
    --output-parquet {output_parquet_path} \\
    --model-path {model_path}
status=$?

if [ $status -eq 0 ]; then
    touch {state_dir}/{chunk_id}.done
else
    touch {state_dir}/{chunk_id}.failed
fi
exit $status
"""

HYBRID_TEMPLATE = """\
#!/bin/bash

#SBATCH --account={account}
#SBATCH --ntasks={ntasks}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --mem={mem}
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --job-name=qdrant-hybrid-{chunk_id}
#SBATCH -o {logs_dir}/{chunk_id}.out
#SBATCH -e {logs_dir}/{chunk_id}.err

echo "Hello $USER, node $(hostname). Running {chunk_id} (hybrid vectorization)."

bedboss qdrant vectorize-hybrid \\
    --chunk-pep {chunk_pep_path} \\
    --output-parquet {output_parquet_path} \\
    --model-path {model_path} \\
    {sparse_flag}
status=$?

if [ $status -eq 0 ]; then
    touch {state_dir}/{chunk_id}.done
else
    touch {state_dir}/{chunk_id}.failed
fi
exit $status
"""


# ---------------------------------------------------------------------------
# models
# ---------------------------------------------------------------------------


class SlurmConfig(BaseModel):
    account: str
    partition: str
    time: str
    mem: str
    cpus_per_task: int
    ntasks: int


class ChunkMeta(BaseModel):
    id: str
    sample_range: tuple[int, int]
    n_samples: int
    pep_path: str
    sbatch_path: str
    logs_dir: str
    output_parquet: str
    job_id: Optional[str] = None
    submitted_at: Optional[str] = None


class QdrantHpcManifest(BaseModel):
    created_at: str
    search_type: str  # "region" or "hybrid"
    bedbase_config: str
    n_chunks: int
    total_samples: int
    model_path: str
    sparse_model_path: Optional[str] = None
    slurm: SlurmConfig
    chunks: list[ChunkMeta]


# ---------------------------------------------------------------------------
# manifest helpers
# ---------------------------------------------------------------------------


def _manifest_path(workdir: Path) -> Path:
    return workdir / MANIFEST_NAME


def _load_manifest(workdir: Path) -> QdrantHpcManifest | None:
    p = _manifest_path(workdir)
    if not p.exists():
        return None
    return QdrantHpcManifest.model_validate_json(p.read_text())


def _save_manifest(workdir: Path, manifest: QdrantHpcManifest) -> None:
    _manifest_path(workdir).write_text(manifest.model_dump_json(indent=2))


# ---------------------------------------------------------------------------
# fetch metadata from database
# ---------------------------------------------------------------------------


def _fetch_region_metadata(
    config: str,
    only_unindexed: bool,
    limit: int | None,
) -> tuple[pd.DataFrame, str, str]:
    """Fetch hg38 bed metadata for region-based vectorization.

    Returns:
        (DataFrame, region2vec_model_path, bedbase_config_path)
    """
    from bbconf.bbagent import BedBaseAgent
    from bbconf.const import DEFAULT_QDRANT_GENOME_DIGESTS
    from bbconf.db_utils import Bed, BedMetadata

    agent = BedBaseAgent(config=config)
    model_path = agent.config.config.path.region2vec

    conditions = [Bed.genome_digest.in_(DEFAULT_QDRANT_GENOME_DIGESTS)]
    if only_unindexed:
        conditions.append(Bed.file_indexed.is_(False))

    statement = (
        select(Bed)
        .join(BedMetadata, Bed.id == BedMetadata.id)
        .where(and_(*conditions))
    )
    if limit:
        statement = statement.limit(limit)

    with Session(agent.config.db_engine.engine) as session:
        results = session.scalars(statement).all()
        rows = _records_to_rows(results)

    df = pd.DataFrame(rows)
    _LOGGER.info(f"Fetched {len(df)} bed records for region reindexing")
    return df, model_path, config


def _fetch_hybrid_metadata(
    config: str,
    only_unindexed: bool,
    limit: int | None,
) -> tuple[pd.DataFrame, str, str | None, str]:
    """Fetch all bed metadata for hybrid vectorization.

    Returns:
        (DataFrame, dense_model_path, sparse_model_path, bedbase_config_path)
    """
    from bbconf.bbagent import BedBaseAgent
    from bbconf.db_utils import Bed, BedMetadata

    agent = BedBaseAgent(config=config)
    model_path = agent.config.config.path.text2vec
    sparse_model_path = agent.config.config.path.sparse_model

    conditions = []
    if only_unindexed:
        conditions.append(Bed.indexed.is_(False))

    statement = select(Bed).join(BedMetadata, Bed.id == BedMetadata.id)
    if conditions:
        statement = statement.where(and_(*conditions))
    if limit:
        statement = statement.limit(limit)

    with Session(agent.config.db_engine.engine) as session:
        results = session.scalars(statement).all()
        rows = _records_to_rows(results)

    df = pd.DataFrame(rows)
    _LOGGER.info(f"Fetched {len(df)} bed records for hybrid reindexing")
    return df, model_path, sparse_model_path, config


def _records_to_rows(results) -> list[dict]:
    """Convert SQLAlchemy Bed records to list of dicts for DataFrame."""
    rows = []
    for r in results:
        rows.append(
            {
                "sample_name": r.id,
                "name": r.name or "",
                "description": r.description or "",
                "genome_alias": r.genome_alias or "",
                "genome_digest": r.genome_digest or "",
                "cell_line": r.annotations.cell_line if r.annotations else "",
                "cell_type": r.annotations.cell_type if r.annotations else "",
                "tissue": r.annotations.tissue if r.annotations else "",
                "target": r.annotations.target if r.annotations else "",
                "treatment": r.annotations.treatment if r.annotations else "",
                "assay": r.annotations.assay if r.annotations else "",
                "species_name": r.annotations.species_name if r.annotations else "",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# splitting
# ---------------------------------------------------------------------------


def _split_into_chunks(
    workdir: Path,
    df: pd.DataFrame,
    n_chunks: int,
) -> list[ChunkMeta]:
    """Split a DataFrame into N chunk CSVs on disk."""
    n = len(df)
    if n == 0:
        raise RuntimeError("No bed records to process")
    n_chunks = min(n_chunks, n)

    base, extra = divmod(n, n_chunks)
    chunks: list[ChunkMeta] = []
    start = 0

    for i in range(n_chunks):
        size = base + (1 if i < extra else 0)
        end = start + size
        chunk_id = f"chunk_{i:04d}"
        chunk_root = workdir / CHUNKS_DIR / chunk_id
        pep_dir = chunk_root / "pep"
        slurm_dir = chunk_root / "slurm"
        logs_dir = chunk_root / "logs"
        output_dir = chunk_root / "output"
        for d in (pep_dir, slurm_dir, logs_dir, output_dir):
            d.mkdir(parents=True, exist_ok=True)

        chunk_csv = pep_dir / "sample_table.csv"
        df.iloc[start:end].to_csv(chunk_csv, index=False)

        chunks.append(
            ChunkMeta(
                id=chunk_id,
                sample_range=(start, end),
                n_samples=size,
                pep_path=str(chunk_csv),
                sbatch_path=str(slurm_dir / f"{chunk_id}.sbatch"),
                logs_dir=str(logs_dir),
                output_parquet=str(output_dir / "vectors.parquet"),
            )
        )
        start = end

    return chunks


# ---------------------------------------------------------------------------
# sbatch rendering + submission
# ---------------------------------------------------------------------------


def _write_region_sbatch_files(
    chunks: list[ChunkMeta],
    slurm_cfg: SlurmConfig,
    model_path: str,
    state_dir: Path,
) -> None:
    for chunk in chunks:
        content = REGION_TEMPLATE.format(
            account=slurm_cfg.account,
            ntasks=slurm_cfg.ntasks,
            cpus_per_task=slurm_cfg.cpus_per_task,
            mem=slurm_cfg.mem,
            partition=slurm_cfg.partition,
            time=slurm_cfg.time,
            chunk_id=chunk.id,
            logs_dir=chunk.logs_dir,
            state_dir=str(state_dir),
            chunk_pep_path=chunk.pep_path,
            output_parquet_path=chunk.output_parquet,
            model_path=model_path,
        )
        Path(chunk.sbatch_path).write_text(content)


def _write_hybrid_sbatch_files(
    chunks: list[ChunkMeta],
    slurm_cfg: SlurmConfig,
    model_path: str,
    sparse_model_path: str | None,
    state_dir: Path,
) -> None:
    sparse_flag = (
        f"--sparse-model-path {sparse_model_path}" if sparse_model_path else ""
    )
    for chunk in chunks:
        content = HYBRID_TEMPLATE.format(
            account=slurm_cfg.account,
            ntasks=slurm_cfg.ntasks,
            cpus_per_task=slurm_cfg.cpus_per_task,
            mem=slurm_cfg.mem,
            partition=slurm_cfg.partition,
            time=slurm_cfg.time,
            chunk_id=chunk.id,
            logs_dir=chunk.logs_dir,
            state_dir=str(state_dir),
            chunk_pep_path=chunk.pep_path,
            output_parquet_path=chunk.output_parquet,
            model_path=model_path,
            sparse_flag=sparse_flag,
        )
        Path(chunk.sbatch_path).write_text(content)


def _sbatch_submit(sbatch_path: str) -> str:
    out = subprocess.run(
        ["sbatch", sbatch_path],
        capture_output=True,
        text=True,
        check=True,
    )
    line = out.stdout.strip().splitlines()[-1]
    return line.split()[-1]


def _get_alive_job_ids(chunks: list[ChunkMeta]) -> set[str]:
    """Batch-check which SLURM jobs are still alive with a single squeue call."""
    job_ids = [c.job_id for c in chunks if c.job_id]
    if not job_ids:
        return set()
    try:
        out = subprocess.run(
            ["squeue", "-j", ",".join(job_ids), "-h", "-o", "%i"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        _LOGGER.warning("squeue not found on PATH")
        return set()
    return {line.strip() for line in out.stdout.strip().splitlines() if line.strip()}


def _chunk_status(
    chunk: ChunkMeta, state_dir: Path, alive_jobs: set[str]
) -> str:
    cid = chunk.id
    if (state_dir / f"{cid}.done").exists():
        return "done"
    if chunk.job_id and chunk.job_id in alive_jobs:
        return "running"
    if (state_dir / f"{cid}.failed").exists():
        return "failed"
    return "pending"


def _submit_pending(manifest: QdrantHpcManifest, workdir: Path) -> None:
    state_dir = workdir / STATE_DIR
    state_dir.mkdir(parents=True, exist_ok=True)
    alive_jobs = _get_alive_job_ids(manifest.chunks)
    submitted = 0
    skipped_done = 0
    skipped_running = 0
    for chunk in manifest.chunks:
        status = _chunk_status(chunk, state_dir, alive_jobs)
        if status == "done":
            skipped_done += 1
            continue
        if status == "running":
            skipped_running += 1
            continue
        failed_sentinel = state_dir / f"{chunk.id}.failed"
        if failed_sentinel.exists():
            failed_sentinel.unlink()
        job_id = _sbatch_submit(chunk.sbatch_path)
        chunk.job_id = job_id
        chunk.submitted_at = datetime.now(timezone.utc).isoformat()
        submitted += 1
        if submitted % 100 == 0:
            print(f"  submitted {submitted} jobs...")
    _save_manifest(workdir, manifest)
    print(
        f"Submission summary: submitted={submitted}, "
        f"already_done={skipped_done}, still_running={skipped_running}"
    )


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------


def reindex_region_hpc(
    bedbase_config: str,
    workdir: str,
    n_chunks: int,
    slurm_cfg: SlurmConfig,
    limit: int | None = None,
    only_unindexed: bool = False,
    dry_run: bool = False,
) -> None:
    """Fetch hg38 bed metadata, split, generate sbatch scripts, and submit."""
    wd = Path(workdir).expanduser().resolve()
    wd.mkdir(parents=True, exist_ok=True)
    (wd / STATE_DIR).mkdir(exist_ok=True)

    manifest = _load_manifest(wd)
    if manifest is None:
        _LOGGER.info("Fetching bed metadata from database...")
        df, model_path, config_path = _fetch_region_metadata(
            bedbase_config, only_unindexed, limit
        )
        if df.empty:
            print("No bed records found matching criteria. Nothing to do.")
            return

        source_dir = wd / "source_pep"
        source_dir.mkdir(exist_ok=True)
        df.to_csv(source_dir / "sample_table.csv", index=False)

        chunks = _split_into_chunks(wd, df, n_chunks)
        state_dir = wd / STATE_DIR

        manifest = QdrantHpcManifest(
            created_at=datetime.now(timezone.utc).isoformat(),
            search_type="region",
            bedbase_config=config_path,
            n_chunks=len(chunks),
            total_samples=len(df),
            model_path=model_path,
            slurm=slurm_cfg,
            chunks=chunks,
        )
        _write_region_sbatch_files(chunks, slurm_cfg, model_path, state_dir)
        _save_manifest(wd, manifest)
        print(
            f"Created {len(chunks)} chunks in {wd} "
            f"({len(df)} total samples, sizes: {[c.n_samples for c in chunks]})"
        )
    else:
        _LOGGER.info(f"Resuming from existing manifest at {wd}")

    if dry_run:
        print("Dry run: skipping sbatch submission")
        return

    _submit_pending(manifest, wd)


def reindex_hybrid_hpc(
    bedbase_config: str,
    workdir: str,
    n_chunks: int,
    slurm_cfg: SlurmConfig,
    limit: int | None = None,
    only_unindexed: bool = False,
    dry_run: bool = False,
) -> None:
    """Fetch all bed metadata, split, generate sbatch scripts, and submit."""
    wd = Path(workdir).expanduser().resolve()
    wd.mkdir(parents=True, exist_ok=True)
    (wd / STATE_DIR).mkdir(exist_ok=True)

    manifest = _load_manifest(wd)
    if manifest is None:
        _LOGGER.info("Fetching bed metadata from database...")
        df, model_path, sparse_model_path, config_path = _fetch_hybrid_metadata(
            bedbase_config, only_unindexed, limit
        )
        if df.empty:
            print("No bed records found matching criteria. Nothing to do.")
            return

        source_dir = wd / "source_pep"
        source_dir.mkdir(exist_ok=True)
        df.to_csv(source_dir / "sample_table.csv", index=False)

        chunks = _split_into_chunks(wd, df, n_chunks)
        state_dir = wd / STATE_DIR

        manifest = QdrantHpcManifest(
            created_at=datetime.now(timezone.utc).isoformat(),
            search_type="hybrid",
            bedbase_config=config_path,
            n_chunks=len(chunks),
            total_samples=len(df),
            model_path=model_path,
            sparse_model_path=sparse_model_path,
            slurm=slurm_cfg,
            chunks=chunks,
        )
        _write_hybrid_sbatch_files(
            chunks, slurm_cfg, model_path, sparse_model_path, state_dir
        )
        _save_manifest(wd, manifest)
        print(
            f"Created {len(chunks)} chunks in {wd} "
            f"({len(df)} total samples, sizes: {[c.n_samples for c in chunks]})"
        )
    else:
        _LOGGER.info(f"Resuming from existing manifest at {wd}")

    if dry_run:
        print("Dry run: skipping sbatch submission")
        return

    _submit_pending(manifest, wd)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _parquet_row_count(path: Path) -> int:
    """Read parquet row count from file metadata without loading data."""
    if not path.exists():
        return 0
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(path).num_rows
    except Exception:
        return 0


def reindex_hpc_status(workdir: str, verbose: bool = False) -> None:
    """Print per-chunk status table for a qdrant reindex workdir.

    Args:
        workdir: Working directory created by reindex-*-hpc.
        verbose: Print per-chunk table even when chunk count is large.
    """
    wd = Path(workdir).expanduser().resolve()
    manifest = _load_manifest(wd)
    if manifest is None:
        raise RuntimeError(f"No manifest found at {wd}")

    state_dir = wd / STATE_DIR
    alive_jobs = _get_alive_job_ids(manifest.chunks)
    counts = {"done": 0, "failed": 0, "running": 0, "pending": 0}
    total_vectors = 0
    failed_chunks: list[str] = []
    rows = []

    for chunk in manifest.chunks:
        status = _chunk_status(chunk, state_dir, alive_jobs)
        counts[status] += 1
        if status == "failed":
            failed_chunks.append(chunk.id)

        n_vectors = _parquet_row_count(Path(chunk.output_parquet))
        total_vectors += n_vectors

        rows.append(
            (chunk.id, chunk.n_samples, status, chunk.job_id or "-", n_vectors)
        )

    print(f"Search type: {manifest.search_type}")
    print(f"Model: {manifest.model_path}")
    print()

    # Print per-chunk table only when manageable or explicitly requested
    show_table = verbose or len(rows) <= 200
    if show_table:
        header = (
            f"{'chunk_id':<14} {'samples':>8} {'status':<10} "
            f"{'job_id':>12} {'vectors':>10}"
        )
        print(header)
        print("-" * len(header))
        for cid, n, st, jid, nv in rows:
            print(f"{cid:<14} {n:>8} {st:<10} {jid:>12} {nv:>10}")
        print("-" * len(header))
    else:
        print(f"({len(rows)} chunks — use --verbose to print full table)")
        print()

    total = sum(counts.values())
    print(
        f"Chunks: done={counts['done']} failed={counts['failed']} "
        f"running={counts['running']} pending={counts['pending']} (of {total})"
    )
    print(
        f"Samples: {manifest.total_samples} total, "
        f"{total_vectors} vectors produced"
    )

    if failed_chunks:
        show_n = min(20, len(failed_chunks))
        print(f"\nFailed chunks ({len(failed_chunks)} total):")
        for cid in failed_chunks[:show_n]:
            print(f"  {cid}")
        if len(failed_chunks) > show_n:
            print(f"  ... and {len(failed_chunks) - show_n} more")
