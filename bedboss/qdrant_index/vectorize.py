"""
Connection-free vectorization for HPC jobs.

Each function loads a model by path, processes a chunk PEP CSV,
and writes vectors + metadata to a parquet file. No bbagent or
database connection is needed.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

_LOGGER = logging.getLogger(__name__)

# Columns carried through from PEP CSV to parquet output (metadata payload)
METADATA_COLUMNS = [
    "sample_name",
    "name",
    "description",
    "genome_alias",
    "genome_digest",
    "cell_line",
    "cell_type",
    "tissue",
    "target",
    "treatment",
    "assay",
    "species_name",
]


def vectorize_region(
    chunk_pep: str,
    output_parquet: str,
    model_path: str,
) -> None:
    """Vectorize bed files using Region2Vec. Writes results to parquet.

    Args:
        chunk_pep: Path to chunk CSV with sample_name + metadata columns.
        output_parquet: Path to write output parquet file.
        model_path: Path or HuggingFace ID for the Region2VecExModel.
    """
    from geniml.bbclient import BBClient
    from geniml.region2vec.main import Region2VecExModel
    from gtars.models import RegionSet as GRegionSet

    _LOGGER.info(f"Loading Region2Vec model from {model_path}")
    encoder = Region2VecExModel(model_path)

    df = pd.read_csv(chunk_pep)
    _LOGGER.info(f"Processing {len(df)} bed files")

    bb_client = BBClient()
    rows = []
    failed_ids = []

    for _, row in df.iterrows():
        bed_id = row["sample_name"]
        try:
            try:
                bed_region_set = GRegionSet(bb_client.seek(bed_id))
            except FileNotFoundError:
                bed_region_set = bb_client.load_bed(bed_id)

            embedding = np.mean(encoder.encode(bed_region_set), axis=0)
            vector = embedding.tolist()

            record = {"vector": vector}
            for col in METADATA_COLUMNS:
                record[col] = row.get(col)
            rows.append(record)

        except Exception as e:
            _LOGGER.warning(f"Failed to vectorize {bed_id}: {e}")
            failed_ids.append(bed_id)
            continue

    if not rows:
        _LOGGER.warning("No files were successfully vectorized")
        # Write empty parquet so the upload step doesn't break
        out = pd.DataFrame(columns=["vector"] + METADATA_COLUMNS)
        out.to_parquet(output_parquet, index=False)
    else:
        out = pd.DataFrame(rows)
        out.to_parquet(output_parquet, index=False)
        _LOGGER.info(
            f"Wrote {len(rows)} vectors to {output_parquet} "
            f"({len(failed_ids)} failures)"
        )

    # Write failed IDs for debugging
    if failed_ids:
        failed_path = Path(output_parquet).parent / "failed_ids.txt"
        failed_path.write_text("\n".join(failed_ids) + "\n")


def vectorize_hybrid(
    chunk_pep: str,
    output_parquet: str,
    model_path: str,
    sparse_model_path: str | None = None,
) -> None:
    """Vectorize bed metadata text using dense + sparse encoders. Writes to parquet.

    Args:
        chunk_pep: Path to chunk CSV with sample_name + metadata columns.
        output_parquet: Path to write output parquet file.
        model_path: Path or HuggingFace ID for the dense text encoder.
        sparse_model_path: Path or HuggingFace ID for the sparse encoder. Optional.
    """
    from fastembed import TextEmbedding

    _LOGGER.info(f"Loading dense encoder from {model_path}")
    dense_encoder = TextEmbedding(model_path)

    sparse_encoder = None
    if sparse_model_path:
        from sentence_transformers import SparseEncoder as STSparseEncoder

        _LOGGER.info(f"Loading sparse encoder from {sparse_model_path}")
        sparse_encoder = STSparseEncoder(sparse_model_path)

    df = pd.read_csv(chunk_pep)
    _LOGGER.info(f"Processing {len(df)} bed files for hybrid search")

    rows = []
    failed_ids = []

    for _, row in df.iterrows():
        bed_id = row["sample_name"]
        try:
            text = (
                f"biosample is {row.get('cell_line')} / {row.get('cell_type')} / "
                f"{row.get('tissue')} with target {row.get('target')} "
                f"assay {row.get('assay')}."
                f"File name {row.get('name')} with summary {row.get('description')}"
            )

            dense_vec = list(list(dense_encoder.embed(text))[0])

            record = {"dense_vector": dense_vec}

            if sparse_encoder:
                sparse_result = sparse_encoder.encode(text).coalesce()
                record["sparse_indices"] = sparse_result.indices().tolist()[0]
                record["sparse_values"] = sparse_result.values().tolist()
            else:
                record["sparse_indices"] = None
                record["sparse_values"] = None

            for col in METADATA_COLUMNS:
                record[col] = row.get(col)
            rows.append(record)

        except Exception as e:
            _LOGGER.warning(f"Failed to vectorize {bed_id}: {e}")
            failed_ids.append(bed_id)
            continue

    if not rows:
        _LOGGER.warning("No files were successfully vectorized")
        out = pd.DataFrame(
            columns=["dense_vector", "sparse_indices", "sparse_values"]
            + METADATA_COLUMNS
        )
        out.to_parquet(output_parquet, index=False)
    else:
        out = pd.DataFrame(rows)
        out.to_parquet(output_parquet, index=False)
        _LOGGER.info(
            f"Wrote {len(rows)} vectors to {output_parquet} "
            f"({len(failed_ids)} failures)"
        )

    if failed_ids:
        failed_path = Path(output_parquet).parent / "failed_ids.txt"
        failed_path.write_text("\n".join(failed_ids) + "\n")
