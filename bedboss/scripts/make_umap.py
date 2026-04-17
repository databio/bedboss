import json
import logging
import os
import sys
import warnings

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from bbconf import BedBaseAgent
from pydantic import BaseModel, ConfigDict
from qdrant_client import QdrantClient
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from umap import UMAP

from bedboss.const import PKG_NAME, UMAP_PARQUET_COLUMNS

_LOGGER = logging.getLogger(PKG_NAME)

python_version = f"{sys.version_info.major}_{sys.version_info.minor}"


class umapReturn(BaseModel):
    model: UMAP | PCA | TSNE
    dataframe: pd.DataFrame

    model_config = ConfigDict(arbitrary_types_allowed=True)



def save_umap_model(umap_model: UMAP | PCA | TSNE, model_path: str) -> None:
    """
    Save the UMAP, PCA, or t-SNE model to a file.

    Args:
        umap_model: Fitted UMAP, PCA, or t-SNE model.
        model_path: Path to save the model.
    """
    with open(model_path, "wb") as file:
        joblib.dump(umap_model, file)

    _LOGGER.info(f"Model saved to {model_path}")


# @lru_cache()
# TODO: can we make this function cached, without using credentials as part of the cache key?
def fetch_data(agent: BedBaseAgent) -> pd.DataFrame:
    """
    Fetch data from Qdrant collection and return it as a DataFrame.

    Args:
        agent: BedBaseAgent instance containing Qdrant access details.

    Returns:
        DataFrame indexed by bed ID with vector and payload columns.
    """

    _LOGGER.info("Fetching data from Qdrant...")

    client = QdrantClient(
        url=agent.config.config.qdrant.host,
        port=agent.config.config.qdrant.port,
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



def save_df_as_json(df: pd.DataFrame, output_path: str) -> None:
    """
    Save a DataFrame as a JSON file in the specified format.

    Includes columns: x, y, z (coordinates), id, name, description, assay, cell_line.

    Args:
        df: DataFrame to save.
        output_path: Path to save the JSON file.
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
        "cell_type",
        "tissue",
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
                node[col] = round(float(node[col]), 3)

    # Create the final JSON structure
    json_data = {"nodes": nodes, "links": []}

    # Save to a JSON file
    output_path = f"{output_path}_{python_version}.json"
    with open(output_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    _LOGGER.info(f"Data saved to {output_path} successfully.")


def save_parquet(
    df: pd.DataFrame,
    output_path: str,
) -> None:
    """
    Save UMAP data as a single Parquet file.

    Columns: id, x, y, name, description, assay, cell_line (+ z if 3D).

    :param df: DataFrame with UMAP coordinates and metadata.
    :param output_path: Path to save the parquet file (without extension).
    """
    if "id" not in df.columns:
        df = df.copy()
        df["id"] = df.index.astype(str)

    cols = list(UMAP_PARQUET_COLUMNS)
    if "z" in df.columns:
        cols.insert(3, "z")

    out = df[[c for c in cols if c in df.columns]].copy()

    # float32 for coordinates, dictionary encoding for categorical strings
    for col in ["x", "y", "z"]:
        if col in out.columns:
            out[col] = out[col].round(3).astype("float32")

    str_cols = out.select_dtypes(include="object").columns
    out[str_cols] = out[str_cols].fillna("")

    parquet_path = f"{output_path}_{python_version}.parquet"
    out.to_parquet(
        parquet_path,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )
    _LOGGER.info(f"Parquet saved to {parquet_path}: {len(out)} rows, {len(out.columns)} columns")


def create_umap(
    df: pd.DataFrame,
    n_components: int = 3,
    plot_name: str | None = None,
    label_column: str = "cell_line",
    method: str = "umap",
) -> umapReturn:
    """
    Create UMAP, PCA, or t-SNE embeddings from the DataFrame.

    Args:
        df: DataFrame containing the vectors and labels.
        n_components: Number of dimensions for UMAP/PCA/t-SNE. Default: 3.
        plot_name: Name for the output plot file. If None, no plot will be saved.
        label_column: Column name to use for labeling plot points (e.g. "cell_line" or "assay").
        method: Dimensionality reduction method. Options: "umap", "pca", or "tsne".

    Returns:
        umapReturn with the fitted model and DataFrame with added coordinate columns.
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

    Args:
        value: UMAP embeddings (only 2D).
        label: Labels for the points in the UMAP plot.
        name: Name for the output plot file.
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


def update_umap_metadata(
    bbconf: str,
    output_path: str,
    geometry: str = None,
) -> None:
    """
    Update UMAP parquet without regenerating geometry.

    :param bbconf: Path to bedbase configuration file.
    :param output_path: Path to write parquet file (without extension).
    :param geometry: Path to existing geometry Parquet to read bed IDs from.
        If not provided, fetches IDs from Qdrant.
    """
    if isinstance(bbconf, str):
        agent = BedBaseAgent(config=bbconf)
    elif isinstance(bbconf, BedBaseAgent):
        agent = bbconf
    else:
        raise TypeError(
            "bbconf must be either a string path or a BedBaseAgent instance."
        )

    if geometry:
        geo_df = pd.read_parquet(geometry)
        bed_ids = list(geo_df["id"])
        qdrant_df = fetch_data(agent=agent)
        qdrant_df = qdrant_df.loc[qdrant_df.index.isin(bed_ids)]
    else:
        qdrant_df = fetch_data(agent=agent)

    save_parquet(qdrant_df, output_path)


def get_embeddings(
    bbconf: str,
    output_file: str,
    n_components: int = 2,
    plot_name: str = None,
    plot_label: str = None,
    top_assays: int | None = 15,
    top_cell_lines: int | None = 15,
    save_model: bool = True,
    method: str = "umap",
    output_format: str = "parquet",
) -> None:
    """
    Get embeddings from Qdrant, create UMAP/PCA/t-SNE, and save results.

    :param bbconf: Path to bedbase configuration file.
    :param output_file: Path to save the output file (without extension).
    :param n_components: Number of dimensions for UMAP/PCA/t-SNE.
    :param plot_name: Name for the output plot file. If None, no plot will be saved.
    :param plot_label: Column name to use for labeling plot points.
    :param top_assays: Number of top assays to consider. If None, all assays are used.
    :param top_cell_lines: Number of top cell lines to consider. If None, all.
    :param save_model: Whether to save the fitted model.
    :param method: Dimensionality reduction method: "umap", "pca", or "tsne".
    :param output_format: Output format: "json", "parquet", or "both".
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

    for ext in (".json", ".parquet"):
        if output_file.endswith(ext):
            output_file = output_file[: -len(ext)]
            break
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

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

    if output_format in ("json", "both"):
        save_df_as_json(umap_return.dataframe, output_file)

    if output_format in ("parquet", "both"):
        save_parquet(umap_return.dataframe, output_file)

    if save_model:
        # controls the random initialization and stochastic optimization during UMAP fitting. But removing, because it causes issues during saving/loading
        # I am removing it here, but it should be set later to the same value (42)
        if method == "umap":
            umap_return.model.random_state = None
        save_umap_model(
            umap_return.model,
            model_path=f"{output_file}_{method}_model_{python_version}.joblib",
        )

    _LOGGER.info(f"{method.upper()} processing completed!")
