"""
Upload pre-computed vectors from parquet files to Qdrant and update DB flags.

This phase requires a bbagent connection (database + qdrant).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from bbconf.bbagent import BedBaseAgent
from bbconf.db_utils import Bed
from bbconf.models.bed_models import VectorMetadata
from qdrant_client.http.models import PointStruct
from qdrant_client.models import SparseVector
from sqlalchemy.orm import Session

_LOGGER = logging.getLogger(__name__)


def _load_parquet_files(workdir: Path) -> pd.DataFrame:
    """Glob and concatenate all vectors.parquet files from chunk output dirs."""
    parquet_files = sorted(workdir.glob("chunks/*/output/vectors.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No vectors.parquet files found in {workdir}/chunks/")

    dfs = []
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        if len(df) > 0:
            dfs.append(df)
            _LOGGER.info(f"Loaded {len(df)} vectors from {pf}")

    if not dfs:
        raise RuntimeError("All parquet files are empty — nothing to upload")

    combined = pd.concat(dfs, ignore_index=True)
    _LOGGER.info(f"Total vectors to upload: {len(combined)}")
    return combined


def upload_region_vectors(
    config: str,
    workdir: str,
    batch: int = 100,
) -> None:
    """Upload region-based vectors to qdrant and mark file_indexed=True.

    Args:
        config: Path to the bedbase config file.
        workdir: Working directory containing chunk output parquet files.
        batch: Number of points to upload in one qdrant upsert call.
    """
    wd = Path(workdir).expanduser().resolve()
    df = _load_parquet_files(wd)

    agent = BedBaseAgent(config=config)
    qd_client = agent.config.qdrant_file_backend.qd_client
    collection = agent.config.config.qdrant.file_collection

    uploaded = 0
    points_batch: list[PointStruct] = []
    ids_batch: list[str] = []

    with Session(agent.config.db_engine.engine) as session:
        for _, row in df.iterrows():
            bed_id = row["sample_name"]
            vector = row["vector"]
            if isinstance(vector, str):
                import ast

                vector = ast.literal_eval(vector)

            metadata = VectorMetadata(
                id=bed_id,
                name=row.get("name") or "",
                description=row.get("description") or "",
                genome_alias=row.get("genome_alias") or "",
                genome_digest=row.get("genome_digest"),
                cell_line=row.get("cell_line") or "",
                cell_type=row.get("cell_type") or "",
                tissue=row.get("tissue") or "",
                target=row.get("target") or "",
                treatment=row.get("treatment") or "",
                assay=row.get("assay") or "",
                species_name=row.get("species_name") or "",
            )

            points_batch.append(
                PointStruct(
                    id=bed_id,
                    vector=list(vector),
                    payload=metadata.model_dump(),
                )
            )
            ids_batch.append(bed_id)

            if len(points_batch) >= batch:
                _upsert_and_mark(
                    session, qd_client, collection, points_batch, ids_batch, "file_indexed"
                )
                uploaded += len(points_batch)
                _LOGGER.info(f"Uploaded {uploaded} points")
                points_batch = []
                ids_batch = []

        # Upload remaining
        if points_batch:
            _upsert_and_mark(
                session, qd_client, collection, points_batch, ids_batch, "file_indexed"
            )
            uploaded += len(points_batch)

    _LOGGER.info(f"Region upload complete: {uploaded} points uploaded to '{collection}'")
    print(f"Upload complete: {uploaded} points to collection '{collection}'")


def upload_hybrid_vectors(
    config: str,
    workdir: str,
    batch: int = 1000,
) -> None:
    """Upload hybrid (dense+sparse) vectors to qdrant and mark indexed=True.

    Args:
        config: Path to the bedbase config file.
        workdir: Working directory containing chunk output parquet files.
        batch: Number of points to upload in one qdrant upsert call.
    """
    wd = Path(workdir).expanduser().resolve()
    df = _load_parquet_files(wd)

    agent = BedBaseAgent(config=config)
    qd_client = agent.config.qdrant_client
    collection = agent.config.config.qdrant.hybrid_collection

    uploaded = 0
    points_batch: list[PointStruct] = []
    ids_batch: list[str] = []

    with Session(agent.config.db_engine.engine) as session:
        for _, row in df.iterrows():
            bed_id = row["sample_name"]
            dense_vec = row["dense_vector"]
            if isinstance(dense_vec, str):
                import ast

                dense_vec = ast.literal_eval(dense_vec)

            point_vectors = {"dense": list(dense_vec)}

            sparse_indices = row.get("sparse_indices")
            sparse_values = row.get("sparse_values")
            if sparse_indices is not None and sparse_values is not None:
                if isinstance(sparse_indices, str):
                    import ast

                    sparse_indices = ast.literal_eval(sparse_indices)
                    sparse_values = ast.literal_eval(sparse_values)
                point_vectors["sparse"] = SparseVector(
                    indices=list(sparse_indices),
                    values=list(sparse_values),
                )

            metadata = VectorMetadata(
                id=bed_id,
                name=row.get("name") or "",
                description=row.get("description") or "",
                genome_alias=row.get("genome_alias") or "",
                genome_digest=row.get("genome_digest"),
                cell_line=row.get("cell_line") or "",
                cell_type=row.get("cell_type") or "",
                tissue=row.get("tissue") or "",
                target=row.get("target") or "",
                treatment=row.get("treatment") or "",
                assay=row.get("assay") or "",
                species_name=row.get("species_name") or "",
            )

            points_batch.append(
                PointStruct(
                    id=bed_id,
                    vector=point_vectors,
                    payload=metadata.model_dump(),
                )
            )
            ids_batch.append(bed_id)

            if len(points_batch) >= batch:
                _upsert_and_mark(
                    session, qd_client, collection, points_batch, ids_batch, "indexed"
                )
                uploaded += len(points_batch)
                _LOGGER.info(f"Uploaded {uploaded} points")
                points_batch = []
                ids_batch = []

        if points_batch:
            _upsert_and_mark(
                session, qd_client, collection, points_batch, ids_batch, "indexed"
            )
            uploaded += len(points_batch)

    _LOGGER.info(f"Hybrid upload complete: {uploaded} points uploaded to '{collection}'")
    print(f"Upload complete: {uploaded} points to collection '{collection}'")


def _upsert_and_mark(
    session: Session,
    qd_client,
    collection: str,
    points: list[PointStruct],
    bed_ids: list[str],
    flag_column: str,
) -> None:
    """Upsert a batch of points to qdrant and update the DB indexed flag.

    Args:
        session: Active SQLAlchemy session.
        qd_client: Qdrant client instance.
        collection: Qdrant collection name.
        points: List of PointStruct to upsert.
        bed_ids: Corresponding bed IDs to mark in the DB.
        flag_column: DB column to set True ('file_indexed' or 'indexed').
    """
    operation_info = qd_client.upsert(
        collection_name=collection,
        points=points,
    )
    assert operation_info.status in ("completed", "acknowledged")

    session.query(Bed).filter(Bed.id.in_(bed_ids)).update(
        {getattr(Bed, flag_column): True},
        synchronize_session=False,
    )
    session.commit()