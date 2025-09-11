from qdrant_client import QdrantClient
import os
import pandas as pd

from umap import UMAP
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Union
import warnings
from functools import lru_cache

from bbconf import BedBaseAgent

import json


@lru_cache()
def fetch_data(agent: BedBaseAgent) -> pd.DataFrame:
    """
    Fetch data from Qdrant collection and return it as a DataFrame.

    :param agent: BedBaseAgent instance containing Qdrant access details

    :return: DataFrame with the following columns:
    """

    client = QdrantClient(
        host=agent.config.config.qdrant.host,
        port=6333,
        api_key=agent.config.config.qdrant.api_key,
    )
    response = client.scroll(
        collection_name=agent.config.config.qdrant.file_collection,
        limit=50000,  # Adjust batch size
        offset=0,
        with_payload=True,
        with_vectors=True,
    )
    points, next_offset = response

    for m in points:
        m.id = m.id.replace("-", "")
    payload_df = pd.DataFrame([{**m.payload, "vector": m.vector} for m in points])
    merged = payload_df.set_index("id")
    return merged


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
    print(f"Saving DataFrame as JSON to {output_path}")

    df.loc[:, "id"] = df.index.astype(str)  # Ensure 'id' is a string
    df = df.fillna("")

    # Create the nodes structure
    nodes = df[columns_to_include].to_dict(orient="records")

    # Create the final JSON structure
    json_data = {"nodes": nodes, "links": []}

    # Save to a JSON file
    with open(output_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    print(f"Data saved to {output_path} successfully.")


def create_umap(
    df: pd.DataFrame,
    n_components: int = 3,
    plot_name: Union[str, None] = None,
    label_column: str = "cell_line",
) -> pd.DataFrame:
    """
    Create UMAP embeddings from the DataFrame.

    :param df: DataFrame containing the vectors and labels
    :param n_components: Number of dimensions for UMAP (default is 3)
    :param plot_name: Name for the output plot file. if None, no plot will be saved
    :param label_column: Column name to use for labeling the points in the UMAP plot e.g. "cell_line" or "assay". Default is "cell_line"

    :return: Tuple of UMAP embeddings and the DataFrame with added coordinates
    """

    if n_components not in [2, 3]:
        raise ValueError("n_components must be either 2 or 3 for UMAP.")
    print("Creating UMAP embeddings...")

    umap = UMAP(
        n_components=n_components,
        random_state=42,
        verbose=True,
    ).fit_transform(list(df["vector"]))

    if plot_name:
        if n_components == 2:
            if label_column not in ["cell_line", "assay"]:
                raise ValueError(
                    f"label_column must be either 'cell_line' or 'assay', got {label_column}."
                )

            plot_umap(umap, list(df[label_column]), name=plot_name)
        else:
            warnings.warn(
                "Plotting is only supported for 2D UMAP. No plot will be saved."
            )

    if n_components == 2:
        df[["x", "y"]] = pd.DataFrame(umap, index=df.index)
    elif n_components == 3:
        df[["x", "y", "z"]] = pd.DataFrame(umap, index=df.index)

    print(f"UMAP shape: {umap.shape}")

    return df


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
    n_components: int = 3,
    plot_name: str = None,
    plot_label: str = None,
    top_assays: Union[int, None] = 15,
    top_cell_lines: Union[int, None] = 15,
) -> None:
    """
    Get embeddings from Qdrant and create UMAP, and save the results to a JSON file, and optionally plot the UMAP.

    :param bbconf: string containing to bedbase configuration file path
    :param output_file: Path to save the output JSON file
    :param n_components: Number of dimensions for UMAP (default is 3)

    :param plot_name: Name for the output plot file. if None, no plot will be saved
    :param plot_label: Column name to use for labeling the points in the UMAP plot e.g. "cell_line" or "assay". Default is "cell_line"

    :param top_assays: Number of top assays to consider. If None, all assays are considered. [Default is 15]
    :param top_cell_lines: Number of top cell lines to consider. If None, all. [Default is 15]

    :return: None

    """
    agent = BedBaseAgent(config=bbconf)

    merged = fetch_data(agent=agent)

    CELL_LINE = "cell_line"
    ASSAY = "assay"

    return_df = merged.copy()

    # Select top cell lines available in the dataset
    if top_cell_lines is not None:
        top_cell_lines_list = (
            merged[CELL_LINE].value_counts().nlargest(top_cell_lines).index
        )
        return_df = return_df[return_df[CELL_LINE].isin(top_cell_lines_list)]

    # Select top assays available in the dataset
    if top_assays is not None:
        top_assays_list = merged[ASSAY].value_counts().nlargest(top_assays).index
        return_df = return_df[return_df[ASSAY].isin(top_assays_list)]

    df = create_umap(
        return_df,
        n_components=n_components,
        plot_name=plot_name,
        label_column=plot_label,
    )
    save_df_as_json(df, output_file)
