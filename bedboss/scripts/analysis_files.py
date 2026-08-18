"""
Upload and manage standalone analysis files (openSignalMatrix, models, other
analysis inputs), plus the ``bedboss files`` CLI sub-app.

Clean bedboss/bbconf boundary, same as ``bedboss snapshot``:

- bedboss computes the local file's ``sha256`` and size, then hands an
  ``AnalysisFileArtifact`` to ``agent.analysis_files.add(...)`` — a single
  bbconf call that uploads the file to S3 (under the fixed ``analysis_files/``
  prefix) *and* records it in the ``analysis_files`` table. bedboss never
  touches boto3 or the ORM directly.

Heavy imports (the bbconf agent) are deferred into the functions that need them
so importing this module for its CLI app stays cheap.
"""

from __future__ import annotations

import hashlib
import logging
import os

import typer

from bedboss.const import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


files_app = typer.Typer(
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    help="Upload and manage standalone analysis files "
    "(openSignalMatrix, models, other analysis inputs).",
)


def sha256_file(path: str, chunk: int = 1 << 20) -> str:
    """Streaming SHA256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


@files_app.command(
    "upload",
    help="Upload an analysis file to S3 and record it in the analysis_files table.",
)
def files_upload(
    path: str = typer.Option(
        ...,
        help="Local path to the file to upload",
        exists=True,
        file_okay=True,
        readable=True,
    ),
    name: str = typer.Option(..., help="Logical name/key, e.g. openSignalMatrix"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    file_type: str = typer.Option(
        None, help="Category, e.g. openSignalMatrix | reference | model"
    ),
    genome: str = typer.Option(None, help="Genome/assembly, e.g. hg38 (optional)"),
    description: str = typer.Option(None, help="Free-text description"),
    tag: list[str] = typer.Option(
        None, "--tag", help="Tag to attach; repeat for multiple tags"
    ),
):
    from bbconf import BedBaseAgent
    from bbconf.models.base_models import AnalysisFileArtifact

    checksum = sha256_file(path)
    file_size = os.path.getsize(path)

    artifact = AnalysisFileArtifact(
        path=path,
        name=name,
        file_type=file_type,
        genome=genome,
        description=description,
        tags=list(tag) if tag else None,
        file_size=file_size,
        checksum=checksum,
    )

    bbagent = BedBaseAgent(config, init_ml=False)
    result = bbagent.analysis_files.add(artifact)
    row = result.results[0]
    print(
        f"Uploaded analysis file '{row.name}' (id={row.id}) -> {row.file_path} "
        f"[{file_size} bytes, sha256={checksum[:12]}…]"
    )


@files_app.command("list", help="List analysis files, newest first.")
def files_list(
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    file_type: str = typer.Option(None, help="Filter by file type"),
    genome: str = typer.Option(None, help="Filter by genome/assembly"),
    tag: str = typer.Option(None, help="Filter by a single tag"),
    limit: int = typer.Option(100, help="Maximum number of rows to return"),
    offset: int = typer.Option(0, help="Number of rows to skip"),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config, init_ml=False)
    result = bbagent.analysis_files.list(
        file_type=file_type, genome=genome, tag=tag, limit=limit, offset=offset
    )
    print(f"Total analysis files: {result.count}")
    for row in result.results:
        print(
            f"id={row.id}  {row.creation_date}  {(row.name or ''):20}  "
            f"type={row.file_type}  genome={row.genome}  "
            f"cksum={(row.checksum or '')[:12]}  {row.file_path}"
        )


@files_app.command("delete", help="Delete an analysis file by id.")
def files_delete(
    id: int = typer.Option(..., help="Analysis file row id"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    remove_s3: bool = typer.Option(True, help="Also delete the underlying S3 object"),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config, init_ml=False)
    bbagent.analysis_files.delete(id, remove_s3=remove_s3)
    print(f"Analysis file {id} deleted from the bedbase database")


@files_app.command("delete-by-checksum", help="Delete analysis file(s) by checksum.")
def files_delete_by_checksum(
    checksum: str = typer.Option(..., help="SHA256 checksum of the analysis file"),
    config: str = typer.Option(..., help="Path to the bedbase config file"),
    remove_s3: bool = typer.Option(
        True, help="Also delete the underlying S3 object(s)"
    ),
):
    from bbconf import BedBaseAgent

    bbagent = BedBaseAgent(config, init_ml=False)
    bbagent.analysis_files.delete_by_checksum(checksum, remove_s3=remove_s3)
    print(
        f"Analysis file(s) with checksum {checksum} deleted from the bedbase database"
    )
