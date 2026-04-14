"""
CLI subapp for qdrant reindexing on HPC.

Registered as ``bedboss qdrant <command>``.
"""

import typer

qdrant_app = typer.Typer(
    name="qdrant",
    help="Qdrant reindexing commands for HPC.",
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
)

# ---------------------------------------------------------------------------
# region-based commands
# ---------------------------------------------------------------------------


@qdrant_app.command(
    name="reindex-region-hpc",
    help="Fetch hg38 beds from DB, split into chunks, and submit SLURM vectorization jobs. Idempotent: re-run to resume.",
)
def reindex_region_hpc_cmd(
    bedbase_config: str = typer.Option(
        ..., help="Path to the bedbase config file", exists=True
    ),
    workdir: str = typer.Option(
        ..., help="Working directory for chunks, state, and manifest"
    ),
    n_chunks: int = typer.Option(..., help="Number of SLURM jobs to create"),
    limit: int = typer.Option(None, help="Max number of bed files to process"),
    only_unindexed: bool = typer.Option(
        False, help="Only fetch beds where file_indexed=False"
    ),
    # SLURM
    slurm_account: str = typer.Option("shefflab", help="SLURM --account"),
    slurm_partition: str = typer.Option("standard", help="SLURM --partition"),
    slurm_time: str = typer.Option("72:00:00", help="SLURM --time"),
    slurm_mem: str = typer.Option("60000", help="SLURM --mem (MB)"),
    slurm_cpus: int = typer.Option(4, help="SLURM --cpus-per-task"),
    slurm_ntasks: int = typer.Option(2, help="SLURM --ntasks"),
    dry_run: bool = typer.Option(False, help="Write chunks but do not submit"),
):
    from bedboss.qdrant_index.qdrant_hpc import SlurmConfig
    from bedboss.qdrant_index.qdrant_hpc import reindex_region_hpc as _run

    slurm_cfg = SlurmConfig(
        account=slurm_account,
        partition=slurm_partition,
        time=slurm_time,
        mem=slurm_mem,
        cpus_per_task=slurm_cpus,
        ntasks=slurm_ntasks,
    )
    _run(
        bedbase_config=bedbase_config,
        workdir=workdir,
        n_chunks=n_chunks,
        slurm_cfg=slurm_cfg,
        limit=limit,
        only_unindexed=only_unindexed,
        dry_run=dry_run,
    )


@qdrant_app.command(
    name="reindex-region-upload",
    help="Read parquet vectors from workdir and upload to qdrant (region/file-to-file collection).",
)
def reindex_region_upload_cmd(
    bedbase_config: str = typer.Option(
        ..., help="Path to the bedbase config file", exists=True
    ),
    workdir: str = typer.Option(
        ..., help="Working directory created by reindex-region-hpc"
    ),
    batch: int = typer.Option(100, help="Qdrant upsert batch size"),
):
    from bedboss.qdrant_index.upload import upload_region_vectors

    upload_region_vectors(config=bedbase_config, workdir=workdir, batch=batch)


@qdrant_app.command(
    name="reindex-region-status",
    help="Show per-chunk status for a region reindex workdir.",
)
def reindex_region_status_cmd(
    workdir: str = typer.Option(
        ..., help="Working directory created by reindex-region-hpc"
    ),
    verbose: bool = typer.Option(
        False, help="Print full per-chunk table even for large runs"
    ),
):
    from bedboss.qdrant_index.qdrant_hpc import reindex_hpc_status

    reindex_hpc_status(workdir=workdir, verbose=verbose)


# ---------------------------------------------------------------------------
# hybrid commands
# ---------------------------------------------------------------------------


@qdrant_app.command(
    name="reindex-hybrid-hpc",
    help="Fetch all beds from DB, split into chunks, and submit SLURM vectorization jobs. Idempotent: re-run to resume.",
)
def reindex_hybrid_hpc_cmd(
    bedbase_config: str = typer.Option(
        ..., help="Path to the bedbase config file", exists=True
    ),
    workdir: str = typer.Option(
        ..., help="Working directory for chunks, state, and manifest"
    ),
    n_chunks: int = typer.Option(..., help="Number of SLURM jobs to create"),
    limit: int = typer.Option(None, help="Max number of bed files to process"),
    only_unindexed: bool = typer.Option(
        False, help="Only fetch beds where indexed=False"
    ),
    # SLURM
    slurm_account: str = typer.Option("shefflab", help="SLURM --account"),
    slurm_partition: str = typer.Option("standard", help="SLURM --partition"),
    slurm_time: str = typer.Option("72:00:00", help="SLURM --time"),
    slurm_mem: str = typer.Option("60000", help="SLURM --mem (MB)"),
    slurm_cpus: int = typer.Option(4, help="SLURM --cpus-per-task"),
    slurm_ntasks: int = typer.Option(2, help="SLURM --ntasks"),
    dry_run: bool = typer.Option(False, help="Write chunks but do not submit"),
):
    from bedboss.qdrant_index.qdrant_hpc import SlurmConfig
    from bedboss.qdrant_index.qdrant_hpc import reindex_hybrid_hpc as _run

    slurm_cfg = SlurmConfig(
        account=slurm_account,
        partition=slurm_partition,
        time=slurm_time,
        mem=slurm_mem,
        cpus_per_task=slurm_cpus,
        ntasks=slurm_ntasks,
    )
    _run(
        bedbase_config=bedbase_config,
        workdir=workdir,
        n_chunks=n_chunks,
        slurm_cfg=slurm_cfg,
        limit=limit,
        only_unindexed=only_unindexed,
        dry_run=dry_run,
    )


@qdrant_app.command(
    name="reindex-hybrid-upload",
    help="Read parquet vectors from workdir and upload to qdrant (hybrid/semantic collection).",
)
def reindex_hybrid_upload_cmd(
    bedbase_config: str = typer.Option(
        ..., help="Path to the bedbase config file", exists=True
    ),
    workdir: str = typer.Option(
        ..., help="Working directory created by reindex-hybrid-hpc"
    ),
    batch: int = typer.Option(1000, help="Qdrant upsert batch size"),
):
    from bedboss.qdrant_index.upload import upload_hybrid_vectors

    upload_hybrid_vectors(config=bedbase_config, workdir=workdir, batch=batch)


@qdrant_app.command(
    name="reindex-hybrid-status",
    help="Show per-chunk status for a hybrid reindex workdir.",
)
def reindex_hybrid_status_cmd(
    workdir: str = typer.Option(
        ..., help="Working directory created by reindex-hybrid-hpc"
    ),
    verbose: bool = typer.Option(
        False, help="Print full per-chunk table even for large runs"
    ),
):
    from bedboss.qdrant_index.qdrant_hpc import reindex_hpc_status

    reindex_hpc_status(workdir=workdir, verbose=verbose)


# ---------------------------------------------------------------------------
# vectorize commands (called by sbatch scripts, not typically by users)
# ---------------------------------------------------------------------------


@qdrant_app.command(
    name="vectorize-region",
    help="[HPC worker] Vectorize a chunk of bed files using Region2Vec. Writes parquet.",
)
def vectorize_region_cmd(
    chunk_pep: str = typer.Option(..., help="Path to chunk CSV"),
    output_parquet: str = typer.Option(..., help="Path to write output parquet"),
    model_path: str = typer.Option(..., help="Region2Vec model path or HuggingFace ID"),
):
    from bedboss.qdrant_index.vectorize import vectorize_region

    vectorize_region(
        chunk_pep=chunk_pep,
        output_parquet=output_parquet,
        model_path=model_path,
    )


@qdrant_app.command(
    name="vectorize-hybrid",
    help="[HPC worker] Vectorize a chunk of bed metadata using dense+sparse text encoders. Writes parquet.",
)
def vectorize_hybrid_cmd(
    chunk_pep: str = typer.Option(..., help="Path to chunk CSV"),
    output_parquet: str = typer.Option(..., help="Path to write output parquet"),
    model_path: str = typer.Option(..., help="Dense text encoder model path"),
    sparse_model_path: str = typer.Option(
        None, help="Sparse encoder model path (optional)"
    ),
):
    from bedboss.qdrant_index.vectorize import vectorize_hybrid

    vectorize_hybrid(
        chunk_pep=chunk_pep,
        output_parquet=output_parquet,
        model_path=model_path,
        sparse_model_path=sparse_model_path,
    )
