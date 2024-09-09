import copy

import os

import numpy as np
import requests

import pephubclient
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy.cluster.hierarchy import linkage

# PEP_URL = "donaldcampbelljr/excluded_ranges_species:default"
# PEP_URL = "donaldcampbelljr/excluded_ranges_species_exp2:default"
# REGION_COUNTS_TSV = "/home/drc/Downloads/igd_database_index.tsv"
try:
    PEP_URL = os.environ["PEP_URL"]
except:
    # pep url
    PEP_URL = "donaldcampbelljr/excluded_ranges_species_exp2:default"

try:
    REGION_COUNTS_TSV = os.environ["REGION_COUNTS_TSV"]
except:
    # region counts as created by igd creation
    REGION_COUNTS_TSV = "/home/drc/Downloads/igd_database_index.tsv"


def main():
    results_path = os.path.abspath("stats_results")

    project = pephubclient.PEPHubClient()

    peppyproject = project.load_project(project_registry_path=PEP_URL)

    df = copy.deepcopy(peppyproject.sample_table)

    columns_translation = {}  # which bed file is from which genome?
    filename = os.path.join(results_path, "columns_translation.txt")

    # GET ORIGINAL COLUMN NAMES FROM BED BASE
    # for col in df.columns:
    #     if ".bed.gz" in col:
    #         name = col
    #         name = name.rsplit(".",2)[0]
    #         #print(name)
    #         bed_meta_data = get_bed_metadata(name)
    #         original_name = bed_meta_data["name"]
    #         columns_translation.update({col:original_name})
    #
    #     else:
    #         pass

    # SAVE LOCALLY FOR CONVENIENCE
    # with open(filename, 'w') as f:
    #     for key, value in columns_translation.items():
    #         f.write(f"{key}\t{value}\n")
    #

    # LOAD LOCAL COLUMN NAME FOR CONVENIENCE
    with open(filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            split_result = line.split("\t")
            key = split_result[0]
            value = split_result[1]
            columns_translation.update({key: value.strip()})

    # Get Number of Regions from igd_tsv files, translate them to original file name
    exclude_regions_counts = {}
    df_regions = pd.read_csv(REGION_COUNTS_TSV, sep="\t")
    df_regions["Number of Regions"] = (
        df_regions["Number of Regions"].astype(str).str.strip()
    )
    temp_exclude_regions_counts = dict(
        zip(df_regions["File"], df_regions["Number of Regions"])
    )
    for k, v in temp_exclude_regions_counts.items():
        if k.strip() in columns_translation:
            exclude_regions_counts.update(
                {columns_translation[k.strip()]: temp_exclude_regions_counts[k]}
            )

    df = df.rename(columns=columns_translation)

    df = calculate_jaccard_index_within_df(df, exclude_regions_counts)

    create_heatmap(df, columns_translation)

    # create_clustermap(df, columns_translation)


def calculate_jaccard_index_within_df(df, exclude_regions_counts):
    # df = df.fillna(0)
    list_of_columns = list(exclude_regions_counts.keys())

    # path = "/home/drc/Downloads/test_1.csv"
    # df.to_csv(path, index=False)
    df = convert_and_fill(df, list_of_columns)
    # path = "/home/drc/Downloads/test_2.csv"
    # df.to_csv(path, index=False)

    df["bed_region_count"] = pd.to_numeric(df["bed_region_count"])
    # print(df.head)
    for column in list_of_columns:
        # Jaccard: number_hits/(regions_exc+regions_bed-number_hits)
        # df[column] = pd.to_numeric(df[column])
        # df[column] = eval(f"df['{column}'] / (df['bed_region_count'] + {exclude_regions_counts[column]} - df['{column}'])")
        df[column] = df[column] / (
            df["bed_region_count"] + int(exclude_regions_counts[column]) - df[column]
        )
    # print(df.head)
    # path = "/home/drc/Downloads/test_3.csv"
    # df.to_csv(path, index=False)
    return df


def convert_and_fill(df, columns):
    """Converts specified columns to numeric and fills nulls with -1.

    Args:
      df: The pandas DataFrame.
      columns: A list of column names to convert.

    Returns:
      A new DataFrame with converted and filled columns.
    """

    df_new = df.copy()
    for col in columns:
        df_new[col] = pd.to_numeric(df[col], errors="coerce")
        df_new[col].fillna(0, inplace=True)
    return df_new


def create_clustermap(df, columns):
    """
    Takes a dataframe, sorts and creates a clustermap
    """
    df = df.sort_values("reported_organism")

    # Truncate names to make graph more readable
    df.index = df.index.str[:20]

    df = df[
        (df["reported_organism"] == "Homo sapiens")
        | (df["reported_organism"] == "Mus musculus")
    ]

    # Select numeric columns
    desired_columns = list(columns.values())

    processed_df = convert_and_fill(df, desired_columns)

    # Create a MultiIndex
    processed_df = processed_df.rename(columns={"sample_name": "sample_name_col"})
    processed_df.set_index(["reported_organism", processed_df.index], inplace=True)

    # Create a grouping column
    df["group"] = df["reported_organism"].apply(
        lambda x: "mouse" if "Mus musculus" in x else "human"
    )

    # Pivot the data
    heatmap_data = df.pivot_table(
        index=["reported_organism", "sample_name"],
        columns="group",
        values=desired_columns,
    )

    # Hierarchical clustering for rows and columns
    row_linkage = linkage(heatmap_data.values, method="average")
    col_linkage = linkage(heatmap_data.T.values, method="average")

    num_bins = 255

    min_val = processed_df[desired_columns].min().min()
    max_val = processed_df[desired_columns].max().max()
    bounds = np.linspace(min_val, max_val, num_bins + 1)

    colors = ["lightgray", *sns.color_palette("magma", num_bins)]  # Lightgray for -1
    cmap = mcolors.LinearSegmentedColormap.from_list("mycmap", colors)

    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    # Create the clustermap
    plt.figure(figsize=(20, 12))
    g = sns.clustermap(
        heatmap_data,
        cmap=cmap,
        norm=norm,
        method="complete",  # Complete linkage for the overall cluster
        row_linkage=row_linkage,
        col_linkage=col_linkage,
        annot=False,
    )

    # Adjust tick labels (optional)
    g.ax_heatmap.tick_params(axis="x", labelsize=7)
    g.ax_heatmap.tick_params(axis="y", labelsize=7, rotation=45)

    plt.title("Clustermap of Numeric Columns by Reported Organism")
    plt.show()


def create_heatmap(df, columns):
    """
    Takes a dataframe, sorts and creates a heatmap
    """
    # Sort by 'reported organism'
    df = df.sort_values("reported_organism")

    # Truncate names to make graph more readable
    df.index = df.index.str[:20]
    # processed_df.columns = df.columns.str[:5]

    # Sort on specific species
    # df = df[
    #     (df["reported_organism"] == "Homo sapiens")
    #     | (df["reported_organism"] == "Mus musculus")
    #     | (df["reported_organism"] == "Rattus norvegicus")
    # ]

    df = df[
        (df["reported_organism"] == "Homo sapiens")
        | (df["reported_organism"] == "Mus musculus")
    ]

    df["group"] = df["reported_organism"].apply(
        lambda x: "mouse" if "Mus musculus" in x else "Homo sapiens"
    )

    # df = df.fillna(-1)

    # Select numeric columns
    # numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    desired_columns = list(columns.values())

    # processed_df = convert_and_fill(df, desired_columns)
    processed_df = df

    # processed_df = processed_df.pivot_table(index='reported_organism', values=desired_columns)
    processed_df.set_index(["reported_organism", processed_df.index], inplace=True)

    # print(processed_df.head(n=1000))
    #
    # processed_df = processed_df.rename(columns=columns)

    num_bins = 255

    # min_val = processed_df[desired_columns].min().min()
    # max_val = processed_df[desired_columns].max().max()
    min_val = 0
    max_val = 1
    bounds = np.linspace(min_val, max_val, num_bins + 1)

    # cmap = sns.color_palette("rocket_r", as_cmap=True)
    colors = ["lightgray", *sns.color_palette("magma", num_bins)]  # Lightgray for -1
    cmap = mcolors.LinearSegmentedColormap.from_list("mycmap", colors)

    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    # Create the heatmap
    plt.figure(figsize=(40, 20))
    plt.yticks(rotation=30, ha="right")

    # ax = sns.heatmap(processed_df[desired_columns], cmap=cmap, norm=norm, annot=False)
    ax = sns.heatmap(
        processed_df[processed_df[desired_columns].mean().sort_values().index],
        cmap=cmap,
        norm=norm,
        annot=False,
    )
    ax.tick_params(axis="x", labelsize=7)
    ax.tick_params(axis="y", labelsize=7)
    plt.title("Heatmap of Numeric Columns by Reported Organism")
    plt.show()


def get_bed_metadata(bed_id):
    """
    Gets metadata from bedbase for a bedbase id (digest)
    """

    url = f"https://api.bedbase.org/v1/bed/{bed_id}/metadata?full=true"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for non-200 status codes

        # Parse JSON response
        metadata = response.json()
        return metadata
    except requests.exceptions.RequestException as e:
        print(f"Error fetching metadata: {e}")
        return None


if __name__ == "__main__":
    main()
