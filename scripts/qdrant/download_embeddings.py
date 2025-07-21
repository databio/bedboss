from qdrant_client import QdrantClient

from dotenv import load_dotenv
import os
import pandas as pd

from umap import UMAP
import umap.plot
import matplotlib.pyplot as plt
import seaborn as sns

# file folder:
file_folder = os.path.dirname(os.path.abspath(__file__))

load_dotenv(
    os.path.join(file_folder, ".env")
)  # Looks for .env in the current directory by default

# Access variables
access_key = os.getenv("QDRANT_ACCESS_KEY")
access_host = os.getenv("QDRANT_API_HOST")


COLLECTION_NAME = "bedbase2"

import json


def save_df_as_json(df, output_path):
    # Select the required columns
    columns_to_include = [
        "x",
        "y",
        "z",
        "id",
        "name",
        "description",
        "data_format",
        "bed_compliance",
        "assay",
        "cell_line",
    ]

    df["id"] = df.index.astype(str)  # Ensure 'id' is a string
    df = df.fillna("")

    # Create the nodes structure
    nodes = df[columns_to_include].to_dict(orient="records")

    # Create the final JSON structure
    json_data = {"nodes": nodes, "links": []}

    # Save to a JSON file
    with open(output_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)


def get_embeddings():
    client = QdrantClient(host=access_host, port=6333, api_key=access_key)
    response = client.scroll(
        collection_name=COLLECTION_NAME,
        limit=40000,  # Adjust batch size
        offset=0,
        with_payload=False,
        with_vectors=True,
    )

    points, next_offset = response

    df = open_csv_meta()
    for m in points:
        m.id = m.id.replace("-", "")

    model_df = pd.DataFrame([m.model_dump() for m in points])
    model_df = model_df.set_index("id")
    merged = df.merge(model_df, on="id", how="right")

    # Select top 15 cell linse avaliable in the dataset

    CELL_LINE = "cell_line"
    top_cell_lines = merged[CELL_LINE].value_counts().nlargest(15).index
    top_df = merged[merged[CELL_LINE].isin(top_cell_lines)]
    # umap, label = create_umap(top_df, CELL_LINE, CELL_LINE)

    # Select top 15 assays
    ASSAY = "assay"
    top_assays = merged[ASSAY].value_counts().nlargest(15).index
    top_df_assay = merged[merged[ASSAY].isin(top_assays)]
    # umap, label = create_umap(top_df_assay, ASSAY, ASSAY)

    # COMBINED:
    COMBINED = "combined"
    top_df_assay_cell_line = top_df_assay[top_df_assay[CELL_LINE].isin(top_cell_lines)]
    umap, df = create_umap(top_df_assay_cell_line, CELL_LINE, "all_cell_line")

    df[["x", "y", "z"]] = pd.DataFrame(umap, index=df.index)

    output_path = os.path.join(file_folder, "output.json")
    save_df_as_json(df, output_path)

    # umap, label = create_umap(top_df_assay_cell_line, ASSAY, "all_assay")


def open_csv_meta(path: str = os.path.join(file_folder, "hg38_metadata.csv")):
    df = pd.read_csv(path)
    df = df.set_index("id")
    return df


def create_umap(df: pd.DataFrame, label_column: str, plot_name: str = "umap_plot"):
    """
    Create UMAP embeddings from the DataFrame.
    """
    umap = UMAP(
        n_components=3,
        random_state=42,
        verbose=True,
    ).fit_transform(list(df["vector"]))

    # plot_umap(umap, list(df[label_column]), name=plot_name)

    return umap, df


def plot_umap(value, label, name="default"):
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
        os.path.join(save_path, f"{COLLECTION_NAME}_{name}.svg"),
        dpi=3000,
        bbox_inches="tight",
    )


if __name__ == "__main__":
    get_embeddings()
    print("Embeddings downloaded successfully.")
