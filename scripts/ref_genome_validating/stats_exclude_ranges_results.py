import copy

import os
import requests

import pephubclient
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors


PEP_URL = "donaldcampbelljr/excluded_ranges_species:default"


def main():
    results_path = os.path.abspath("stats_results")

    project = pephubclient.PEPHubClient()

    peppyproject = project.load_project(project_registry_path=PEP_URL)

    # print(peppyproject)

    df = copy.deepcopy(peppyproject.sample_table)

    # print(df.columns)

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

    create_heatmap(df, columns_translation)


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
        df_new[col].fillna(-1, inplace=True)
    return df_new


def create_heatmap(df, columns):
    """
    Takes a dataframe, sorts and creates a heatmap
    """
    # Sort by 'reported organism'
    df = df.sort_values("reported_organism")
    df = df.fillna(-1)

    # Select numeric columns
    # numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    desired_columns = list(columns.keys())

    processed_df = convert_and_fill(df, desired_columns)

    cmap = mcolors.ListedColormap(
        ["lightblue", "red", "green"]
    )  # Adjust colors as needed
    bounds = [-1, 0, 500]
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    # Create the heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(processed_df[desired_columns], cmap=cmap, norm=norm, annot=False)
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
