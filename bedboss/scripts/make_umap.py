from qdrant_client import QdrantClient
import os
import sys
import pandas as pd
import logging
import numpy as np
from pydantic import BaseModel, ConfigDict

from umap import UMAP
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE

import matplotlib.pyplot as plt
import seaborn as sns
from typing import Union
import warnings
from functools import lru_cache

import joblib
from bbconf import BedBaseAgent
from bbconf.db_utils import Bed, BedMetadata, BedStats
from sqlalchemy.orm import Session, joinedload
import json

from bedboss.const import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)

python_version = f"{sys.version_info.major}_{sys.version_info.minor}"


class umapReturn(BaseModel):
    model: Union[UMAP, PCA, TSNE]
    dataframe: pd.DataFrame

    model_config = ConfigDict(arbitrary_types_allowed=True)


def save_umap_model(umap_model: Union[UMAP, PCA, TSNE], model_path: str) -> None:
    """
    Save the UMAP, PCA, or t-SNE model to a file.

    :param umap_model: Fitted UMAP, PCA, or t-SNE model
    :param model_path: Path to save the model
    :return: None

    """
    with open(model_path, "wb") as file:
        joblib.dump(umap_model, file)

    _LOGGER.info(f"Model saved to {model_path}")


# @lru_cache()
# TODO: can we make this function cached, without using credentials as part of the cache key?
def fetch_data(agent: BedBaseAgent) -> pd.DataFrame:
    """
    Fetch data from Qdrant collection and return it as a DataFrame.

    :param agent: BedBaseAgent instance containing Qdrant access details

    :return: DataFrame with the following columns:
    """

    _LOGGER.info("Fetching data from Qdrant...")

    client = QdrantClient(
        host=agent.config.config.qdrant.host,
        port=6333,
        api_key=agent.config.config.qdrant.api_key,
    )

    points = []
    next_offset = 0
    batch_size = 10000

    while next_offset is not None:
        response = client.scroll(
            collection_name=agent.config.config.qdrant.file_collection,
            limit=batch_size,
            offset=next_offset,
            with_payload=True,
            with_vectors=True,
        )
        batch_points, next_offset = response
        points.extend(batch_points)
        _LOGGER.info(
            f"Fetched batch of {len(batch_points)} records (total: {len(points)})."
        )

    for m in points:
        m.id = m.id.replace("-", "")
    payload_df = pd.DataFrame([{**m.payload, "vector": m.vector} for m in points])
    merged = payload_df.set_index("id")

    _LOGGER.info(f"Fetched {len(merged)} records from Qdrant.")
    return merged


def fetch_db_metadata(agent: BedBaseAgent, bed_ids: list[str]) -> pd.DataFrame:
    """
    Fetch bed_stats and annotation metadata from PostgreSQL for the given bed IDs.

    Returns a DataFrame indexed by bed ID with stats and annotation columns.
    """
    _LOGGER.info(f"Fetching DB metadata for {len(bed_ids)} beds...")

    rows = []
    with Session(agent.config.db_engine.engine) as session:
        # Batch query to avoid N+1
        for bed_obj in (
            session.query(Bed)
            .options(joinedload(Bed.stats), joinedload(Bed.annotations))
            .filter(Bed.id.in_(bed_ids))
            .all()
        ):
            row = {"id": bed_obj.id}

            # Stats (tier 1 region characteristics + tier 2 TSS distance)
            if bed_obj.stats:
                stats = bed_obj.stats
                row["number_of_regions"] = stats.number_of_regions
                row["mean_region_width"] = stats.mean_region_width
                row["gc_content"] = stats.gc_content
                row["median_tss_dist"] = stats.median_tss_dist
                row["promoterprox_frequency"] = stats.promoterprox_frequency
                row["promoterprox_percentage"] = stats.promoterprox_percentage

            # Annotation (tier 2 fields not in Qdrant payload)
            if bed_obj.annotations:
                anno = bed_obj.annotations
                row["antibody"] = anno.antibody
                row["library_source"] = anno.library_source
                row["original_file_name"] = anno.original_file_name
                row["global_sample_id"] = (
                    ";".join(anno.global_sample_id) if anno.global_sample_id else None
                )
                row["global_experiment_id"] = (
                    ";".join(anno.global_experiment_id)
                    if anno.global_experiment_id
                    else None
                )

            # Classification (from Bed table)
            row["bed_compliance"] = bed_obj.bed_compliance
            row["data_format"] = bed_obj.data_format

            rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.set_index("id")
    _LOGGER.info(f"Fetched DB metadata for {len(df)} beds.")
    return df


def save_df_as_json(df: pd.DataFrame, output_path: str) -> None:
    """
    Save a DataFrame as a JSON file in the specified format.
    It includes the following columns:
    - x, y, z (coordinates)
    - id (string identifier)
    - name (string)
    - description (string)
    - assay (string)
    - cell_line (string)

    :param df: DataFrame to save
    :param output_path: Path to save the JSON file
    :return: None

    """
    # Select the required columns
    columns_to_include = [
        "x",
        "y",
        "id",
        "name",
        "description",
        # "data_format",
        # "bed_compliance",
        "assay",
        "cell_line",
    ]

    if "z" in df.columns:
        columns_to_include.insert(2, "z")

    output_path = os.path.abspath(output_path)
    (f"Saving DataFrame as JSON to {output_path}")

    df.loc[:, "id"] = df.index.astype(str)  # Ensure 'id' is a string
    df = df.fillna("")

    coord_cols = [c for c in ["x", "y", "z"] if c in df.columns]

    # Create the nodes structure
    nodes = df[columns_to_include].to_dict(orient="records")
    for node in nodes:
        for col in coord_cols:
            if col in node:
                node[col] = round(float(node[col]), 2)

    # Create the final JSON structure
    json_data = {"nodes": nodes, "links": []}

    # Save to a JSON file
    output_path = f"{output_path}_{python_version}.json"
    with open(output_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    _LOGGER.info(f"Data saved to {output_path} successfully.")


def save_parquet_tiers(
    df: pd.DataFrame,
    db_meta: pd.DataFrame,
    output_dir: str,
) -> None:
    """
    Save UMAP data as tiered Parquet files.

    Produces:
      - hg38_geometry.parquet  (x, y, id)
      - hg38_meta_t1.parquet   (core biological annotation + region stats)
      - hg38_meta_t2.parquet   (extended annotation)
      - hg38_meta_t3.parquet   (genomic partition frequencies)
    """
    os.makedirs(output_dir, exist_ok=True)

    # Ensure id is a column (not just index)
    if "id" not in df.columns:
        df = df.copy()
        df["id"] = df.index.astype(str)

    # Join DB metadata
    if not db_meta.empty:
        combined = df.join(db_meta, how="left", rsuffix="_db")
    else:
        combined = df

    # --- Geometry ---
    coord_cols = ["id", "x", "y"]
    if "z" in combined.columns:
        coord_cols.append("z")
    geometry = combined[coord_cols].copy()
    for col in ["x", "y", "z"]:
        if col in geometry.columns:
            geometry[col] = geometry[col].round(2).astype("float32")
    geometry.to_parquet(os.path.join(output_dir, "hg38_geometry.parquet"), index=False)
    _LOGGER.info(f"Geometry: {len(geometry)} rows")

    # --- Tier 1: Core metadata ---
    t1_cols = [
        "id",
        "name",
        "description",
        "assay",
        "target",
        "cell_line",
        "cell_type",
        "tissue",
        "number_of_regions",
        "mean_region_width",
        "gc_content",
    ]
    t1 = combined[[c for c in t1_cols if c in combined.columns]].copy()
    t1 = t1.fillna("")
    t1.to_parquet(os.path.join(output_dir, "hg38_meta_t1.parquet"), index=False)
    _LOGGER.info(f"Tier 1: {len(t1)} rows, {len(t1.columns)} columns")

    # --- Tier 2: Extended annotation ---
    t2_cols = [
        "id",
        "treatment",
        "antibody",
        "species_name",
        "genome_alias",
        "bed_compliance",
        "data_format",
        "median_tss_dist",
        "library_source",
        "global_sample_id",
        "global_experiment_id",
        "original_file_name",
    ]
    t2 = combined[[c for c in t2_cols if c in combined.columns]].copy()
    t2 = t2.fillna("")
    t2.to_parquet(os.path.join(output_dir, "hg38_meta_t2.parquet"), index=False)
    _LOGGER.info(f"Tier 2: {len(t2)} rows, {len(t2.columns)} columns")

    # Tier 3 reserved for future analysis results (gtars genomic distributions,
    # enrichment profiles, embedding quality scores). Not generated in this version.


def create_umap(
    df: pd.DataFrame,
    n_components: int = 3,
    plot_name: Union[str, None] = None,
    label_column: str = "cell_line",
    method: str = "umap",
) -> umapReturn:
    """
    Create UMAP, PCA, or t-SNE embeddings from the DataFrame.

    :param df: DataFrame containing the vectors and labels
    :param n_components: Number of dimensions for UMAP/PCA/t-SNE (default is 3)
    :param plot_name: Name for the output plot file. if None, no plot will be saved
    :param label_column: Column name to use for labeling the points in the plot e.g. "cell_line" or "assay". Default is "cell_line"
    :param method: Dimensionality reduction method to use. Options: "umap" (default), "pca", or "tsne"

    :return: Tuple of DataFrame with added coordinates and fitted model
    """

    if n_components not in [2, 3]:
        raise ValueError("n_components must be either 2 or 3.")

    method = method.lower()
    if method not in ["umap", "pca", "tsne"]:
        raise ValueError("method must be either 'umap', 'pca', or 'tsne'.")

    _LOGGER.info(f"Creating {method.upper()} embeddings...")

    if method == "umap":
        model = UMAP(
            n_components=n_components,
            random_state=42,
            verbose=True,
        )
    elif method == "pca":
        model = PCA(
            n_components=n_components,
            random_state=42,
        )
    else:  # method == "tsne"
        model = TSNE(
            n_components=n_components,
            random_state=42,
            verbose=True,
        )

    # Convert list of vectors to numpy array
    vectors = np.array(list(df["vector"]))

    if method == "tsne":
        # t-SNE doesn't support separate fit/transform, use fit_transform
        embeddings = model.fit_transform(vectors)
        fitted_model = model
    else:
        fitted_model = model.fit(vectors)
        embeddings = model.transform(vectors)

    if plot_name:
        if n_components == 2:
            if label_column not in ["cell_line", "assay"]:
                raise ValueError(
                    f"label_column must be either 'cell_line' or 'assay', got {label_column}."
                )

            plot_umap(embeddings, list(df[label_column]), name=plot_name)
        else:
            warnings.warn(
                "Plotting is only supported for 2D embeddings. No plot will be saved."
            )

    if n_components == 2:
        df[["x", "y"]] = pd.DataFrame(embeddings, index=df.index)
    elif n_components == 3:
        df[["x", "y", "z"]] = pd.DataFrame(embeddings, index=df.index)

    _LOGGER.info(f"{method.upper()} shape: {embeddings.shape}")

    return umapReturn(
        model=fitted_model,
        dataframe=df,
    )


def plot_umap(value, label, name="default") -> None:
    """
    Plot UMAP embeddings and save the figure.

    :param value: UMAP embeddings (only 2D)
    :param label: Labels for the points in the UMAP plot
    :param name: Name for the output plot file
    :return: None
    """

    fig, ax = plt.subplots(figsize=(5, 5))
    sns.scatterplot(
        ax=ax,
        x=value[:, 0],
        y=value[:, 1],
        hue=label,
        palette="tab20",
        s=2,  # Increase the size of the points
        edgecolor="none",
    )
    ax.legend(
        title=f"Clustering by {name}",
        loc="upper left",
        bbox_to_anchor=(1, 1),
        markerscale=2,
        fontsize="small",
    )
    ax.margins(0)  # Remove margins from the plot

    save_path = "."
    fig.savefig(
        os.path.join(save_path, f"{name}"),
        dpi=3000,
        bbox_inches="tight",
    )


def get_embeddings(
    bbconf: str,
    output_file: str,
    n_components: int = 2,
    plot_name: str = None,
    plot_label: str = None,
    top_assays: Union[int, None] = 15,
    top_cell_lines: Union[int, None] = 15,
    save_model: bool = True,
    method: str = "umap",
) -> None:
    """
    Get embeddings from Qdrant and create UMAP, PCA, or t-SNE, and save the results to a JSON file, and optionally plot the embeddings.

    :param bbconf: string containing to bedbase configuration file path
    :param output_file: Path to save the output JSON file
    :param n_components: Number of dimensions for UMAP/PCA/t-SNE [default is 2]

    :param plot_name: Name for the output plot file. if None, no plot will be saved
    :param plot_label: Column name to use for labeling the points in the plot e.g. "cell_line" or "assay". [Default is "cell_line"]

    :param top_assays: Number of top assays to consider. If None, all assays are considered. [Default is 15]
    :param top_cell_lines: Number of top cell lines to consider. If None, all. [Default is 15]

    :param save_model: Whether to save the model or not [Default is True]
    :param method: Dimensionality reduction method to use. Options: "umap", "pca", or "tsne" [Default is "umap"]

    :return: None

    """

    if isinstance(bbconf, str):
        agent = BedBaseAgent(config=bbconf)
    elif isinstance(bbconf, BedBaseAgent):
        agent = bbconf
    else:
        raise TypeError(
            "bbconf must be either a string path or a BedBaseAgent instance."
        )

    merged = fetch_data(agent=agent)

    if output_file.endswith(".json"):
        output_file = output_file[:-5]
        # output_file += ".json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    CELL_LINE = "cell_line"
    ASSAY = "assay"

    return_df = merged.copy()

    ##############################################################
    ######## Option 1 ############################################
    ######### Remove empty/None cell lines and assays ############
    ##############################################################

    # # Select top cell lines available in the dataset
    # if top_cell_lines is not None:
    #     top_cell_lines_list = [
    #         x
    #         for x in merged[CELL_LINE].value_counts().nlargest(top_cell_lines).index
    #         if x is not None and x != ""
    #     ]
    #
    #     return_df = return_df[return_df[CELL_LINE].isin(top_cell_lines_list)]
    #
    # # Select top assays available in the dataset
    # if top_assays is not None:
    #     top_assays_list = [
    #         x
    #         for x in merged[ASSAY].value_counts().nlargest(top_assays).index
    #         if x is not None and x != ""
    #     ]
    #
    #     return_df = return_df[return_df[ASSAY].isin(top_assays_list)]

    ################################################################################
    ######################### Option 2 #############################################
    ## Label empty/None cell lines and assays as "na" instead of removing them #####
    ################################################################################
    na_name = "UNKNOWN"

    return_df[CELL_LINE] = return_df[CELL_LINE].fillna(na_name).replace("", na_name)
    return_df[ASSAY] = return_df[ASSAY].fillna(na_name).replace("", na_name)

    # Select top cell lines available in the dataset
    if top_cell_lines is not None:
        top_cell_lines_list = list(
            return_df[CELL_LINE].value_counts().nlargest(top_cell_lines).index
        )

        return_df = return_df[return_df[CELL_LINE].isin(top_cell_lines_list)]

    # Select top assays available in the dataset
    if top_assays is not None:
        top_assays_list = list(
            return_df[ASSAY].value_counts().nlargest(top_assays).index
        )
        return_df = return_df[return_df[ASSAY].isin(top_assays_list)]

    ###############################################################################

    umap_return = create_umap(
        return_df,
        n_components=n_components,
        plot_name=plot_name,
        label_column=plot_label,
        method=method,
    )

    # Legacy JSON output
    save_df_as_json(umap_return.dataframe, output_file)

    # Parquet tiered output
    parquet_dir = os.path.dirname(os.path.abspath(output_file))
    db_meta = fetch_db_metadata(agent, list(umap_return.dataframe.index))
    save_parquet_tiers(umap_return.dataframe, db_meta, parquet_dir)

    if save_model:
        ## controls the random initialization and stochastic optimization during UMAP fitting. But removing, because it causes issues during saving/loading
        ## I am removing it here, but it should be set later to the same value (42)
        if method == "umap":
            umap_return.model.random_state = None
        save_umap_model(
            umap_return.model,
            model_path=f"{output_file}_{method}_model_{python_version}.joblib",
        )

    _LOGGER.info(f"{method.upper()} processing completed!")
